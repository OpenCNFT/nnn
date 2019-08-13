package praefect

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	gitalyauth "gitlab.com/gitlab-org/gitaly/auth"
	gitalyconfig "gitlab.com/gitlab-org/gitaly/internal/config"
	"gitlab.com/gitlab-org/gitaly/internal/helper"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/models"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/protoregistry"

	"github.com/golang/protobuf/protoc-gen-go/descriptor"
	"github.com/sirupsen/logrus"
	"gitlab.com/gitlab-org/gitaly/client"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/grpc-proxy/proxy"
	"google.golang.org/grpc"
)

// Coordinator takes care of directing client requests to the appropriate
// downstream server. The coordinator is thread safe; concurrent calls to
// register nodes are safe.
type Coordinator struct {
	log           *logrus.Logger
	failoverMutex sync.RWMutex
	connMutex     sync.RWMutex

	datastore ReplicasDatastore

	nodes    map[string]*grpc.ClientConn
	registry *protoregistry.Registry
}

// NewCoordinator returns a new Coordinator that utilizes the provided logger
func NewCoordinator(l *logrus.Logger, datastore ReplicasDatastore, fileDescriptors ...*descriptor.FileDescriptorProto) *Coordinator {
	registry := protoregistry.New()
	registry.RegisterFiles(fileDescriptors...)

	return &Coordinator{
		log:       l,
		datastore: datastore,
		nodes:     make(map[string]*grpc.ClientConn),
		registry:  registry,
	}
}

// RegisterProtos allows coordinator to register new protos on the fly
func (c *Coordinator) RegisterProtos(protos ...*descriptor.FileDescriptorProto) error {
	return c.registry.RegisterFiles(protos...)
}

// streamDirector determines which downstream servers receive requests
func (c *Coordinator) streamDirector(ctx context.Context, fullMethodName string, peeker proxy.StreamModifier) (context.Context, *grpc.ClientConn, error) {
	// For phase 1, we need to route messages based on the storage location
	// to the appropriate Gitaly node.
	c.log.Debugf("Stream director received method %s", fullMethodName)

	c.failoverMutex.RLock()
	defer c.failoverMutex.RUnlock()

	frame, err := peeker.Peek()
	if err != nil {
		return nil, nil, err
	}

	mi, err := c.registry.LookupMethod(fullMethodName)
	if err != nil {
		return nil, nil, err
	}

	var primary *models.Node

	if mi.Scope == protoregistry.ScopeRepository {
		m, err := mi.UnmarshalRequestProto(frame)
		if err != nil {
			return nil, nil, err
		}

		targetRepo, err := mi.TargetRepo(m)
		if err != nil {
			return nil, nil, err
		}

		primary, err = c.datastore.GetPrimary(targetRepo.GetRelativePath())

		if err != nil {
			if err != ErrPrimaryNotSet {
				return nil, nil, err
			}
			// if there are no primaries for this repository, pick one
			nodes, err := c.datastore.GetStorageNodes()
			if err != nil {
				return nil, nil, err
			}

			if len(nodes) == 0 {
				return nil, nil, fmt.Errorf("no nodes serve storage %s", targetRepo.GetStorageName())

			}
			newPrimary := nodes[rand.New(rand.NewSource(time.Now().Unix())).Intn(len(nodes))]

			// set the primary
			if err = c.datastore.SetPrimary(targetRepo.GetRelativePath(), newPrimary.ID); err != nil {
				return nil, nil, err
			}

			primary = &newPrimary
		}

		targetRepo.StorageName = primary.Storage

		b, err := proxy.Codec().Marshal(m)
		if err != nil {
			return nil, nil, err
		}
		if err = peeker.Modify(b); err != nil {
			return nil, nil, err
		}

	} else {
		//TODO: For now we just pick a random storage node for a non repository scoped RPC, but we will need to figure out exactly how to
		// proxy requests that are not repository scoped
		node, err := c.datastore.GetStorageNodes()
		if err != nil {
			return nil, nil, err
		}
		if len(node) == 0 {
			return nil, nil, errors.New("no node storages found")
		}
		primary = &node[0]
	}

	// We only need the primary node, as there's only one primary storage
	// location per praefect at this time
	cc, err := c.GetConnection(primary.Storage)
	if err != nil {
		return nil, nil, fmt.Errorf("unable to find existing client connection for %s", primary.Storage)
	}

	return helper.IncomingToOutgoing(ctx), cc, nil
}

// RegisterNode will direct traffic to the supplied downstream connection when the storage location
// is encountered.
func (c *Coordinator) RegisterNode(storageName, listenAddr string) error {
	conn, err := client.Dial(listenAddr,
		[]grpc.DialOption{
			grpc.WithDefaultCallOptions(grpc.CallCustomCodec(proxy.Codec())),
			grpc.WithPerRPCCredentials(gitalyauth.RPCCredentials(gitalyconfig.Config.Auth.Token)),
		},
	)
	if err != nil {
		return err
	}

	c.setConn(storageName, conn)

	return nil
}

func (c *Coordinator) setConn(storageName string, conn *grpc.ClientConn) {
	c.connMutex.Lock()
	c.nodes[storageName] = conn
	c.connMutex.Unlock()
}

// GetConnection gets the grpc client connection based on an address
func (c *Coordinator) GetConnection(storageName string) (*grpc.ClientConn, error) {
	c.connMutex.RLock()
	cc, ok := c.nodes[storageName]
	c.connMutex.RUnlock()
	if !ok {
		return nil, errors.New("client connection not found")
	}

	return cc, nil

}

// FailoverRotation waits for the SIGUSR1 signal, then promotes the next secondary to be primary
func (c *Coordinator) FailoverRotation() {
	c.handleSignalAndRotate()
}

func (c *Coordinator) handleSignalAndRotate() {
	failoverChan := make(chan os.Signal, 1)
	signal.Notify(failoverChan, syscall.SIGUSR1)

	for {
		<-failoverChan

		c.failoverMutex.Lock()
		// TODO: update failover logic
		c.log.Info("failover happens")
		c.failoverMutex.Unlock()
	}
}
