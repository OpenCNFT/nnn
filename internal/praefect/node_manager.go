package praefect

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
	gitalyauth "gitlab.com/gitlab-org/gitaly/auth"
	"gitlab.com/gitlab-org/gitaly/client"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/config"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/grpc-proxy/proxy"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/models"
	"google.golang.org/grpc"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

// Shard is a primary with a set of secondaries
type Shard interface {
	GetPrimary() (Node, error)
	GetSecondaries() ([]Node, error)
}

// NodeManager is responsible for returning shards for virtual storages
type NodeManager interface {
	GetShard(virtualStorageName string) (Shard, error)
}

// Node represents some metadata of a node as well as a connection
type Node interface {
	GetStorage() string
	GetAddress() string
	GetToken() string
	GetConnection() *grpc.ClientConn
}

type shard struct {
	m           sync.RWMutex
	primary     *nodeStatus
	secondaries []*nodeStatus
}

// GetPrimary gets the primary of a shard
func (s *shard) GetPrimary() (Node, error) {
	s.m.RLock()
	defer s.m.RUnlock()

	return s.primary, nil
}

// GetSecondaries gets the secondaries of a shard
func (s *shard) GetSecondaries() ([]Node, error) {
	s.m.RLock()
	defer s.m.RUnlock()

	var secondaries []Node
	for _, secondary := range s.secondaries {
		secondaries = append(secondaries, secondary)
	}

	return secondaries, nil
}

// NodeMgr is a concrete type that adheres to the NodeManager interface
type NodeMgr struct {
	shards map[string]*shard
	log    *logrus.Entry
}

// ErrPrimaryNotHealthy indicates the primary of a shard is not in a healthy state and hence
// should not be used for a new request
var ErrPrimaryNotHealthy = errors.New("primary is not healthy")

// NewNodeManager creates a new NodeMgr based on virtual storage configs
func NewNodeManager(log *logrus.Entry, virtualStorages []config.VirtualStorage) (*NodeMgr, error) {
	shards := make(map[string]*shard)

	for _, virtualStorage := range virtualStorages {
		var secondaries []*nodeStatus
		var primary *nodeStatus
		for _, node := range virtualStorage.Nodes {
			conn, err := client.Dial(node.Address,
				[]grpc.DialOption{
					grpc.WithDefaultCallOptions(grpc.CallCustomCodec(proxy.Codec())),
					grpc.WithPerRPCCredentials(gitalyauth.RPCCredentials(node.Token)),
				},
			)
			if err != nil {
				return nil, err
			}
			ns := newConnectionStatus(*node, conn)

			if node.DefaultPrimary {
				primary = ns
				continue
			}

			secondaries = append(secondaries, ns)
		}
		shards[virtualStorage.Name] = &shard{
			primary:     primary,
			secondaries: secondaries,
		}
	}

	return &NodeMgr{
		shards: shards,
		log:    log,
	}, nil
}

// healthcheckThreshold is the number of consecutive healthpb.HealthCheckResponse_SERVING necessary
// for deeming a node "healthy"
const healthcheckThreshold = 3

func (n *NodeMgr) bootstrap(d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()

	for i := 0; i < healthcheckThreshold; i++ {
		<-timer.C
		if err := n.checkShards(); err != nil {
			return err
		}
		timer.Reset(d)
	}

	return nil
}

func (n *NodeMgr) monitor(d time.Duration) {
	ticker := time.NewTicker(d)
	defer ticker.Stop()

	for {
		<-ticker.C
		if err := n.checkShards(); err != nil {
			n.log.WithError(err).Error("error when checking shards")
		}
	}
}

// Start will bootstrap the node manager by calling healthcheck on the nodes as well as kicking off
// the monitoring process. Start must be called before NodeMgr can be used.
func (n *NodeMgr) Start(bootstrapInterval, monitorInterval time.Duration) {
	n.bootstrap(bootstrapInterval)
	go n.monitor(monitorInterval)
}

// GetShard retrieves a shard for a virtual storage name
func (n *NodeMgr) GetShard(virtualStorageName string) (Shard, error) {
	shard, ok := n.shards[virtualStorageName]
	if !ok {
		return nil, errors.New("virtual storage does not exist")
	}

	if !shard.primary.isHealthy() {
		return nil, ErrPrimaryNotHealthy
	}

	return shard, nil
}

type errCollection []error

func (e errCollection) Error() string {
	sb := bytes.NewBufferString("")
	for _, err := range e {
		sb.WriteString(err.Error())
		sb.WriteString("\n")
	}

	return sb.String()
}

func (n *NodeMgr) checkShards() error {
	var errs errCollection
	for _, shard := range n.shards {
		if err := shard.primary.check(); err != nil {
			errs = append(errs, err)
		}
		for _, secondary := range shard.secondaries {
			if err := secondary.check(); err != nil {
				errs = append(errs, err)
			}
		}

		if shard.primary.isHealthy() {
			continue
		}

		newPrimaryIndex := -1
		for i, secondary := range shard.secondaries {
			if secondary.isHealthy() {
				newPrimaryIndex = i
				break
			}
		}

		if newPrimaryIndex < 0 {
			// no healthy secondaries exist
			continue
		}
		shard.m.Lock()
		newPrimary := shard.secondaries[newPrimaryIndex]
		shard.secondaries = append(shard.secondaries[:newPrimaryIndex], shard.secondaries[newPrimaryIndex+1:]...)
		shard.secondaries = append(shard.secondaries, shard.primary)
		shard.primary = newPrimary
		shard.m.Unlock()
	}

	if len(errs) > 0 {
		return errs
	}

	return nil
}

func newConnectionStatus(node models.Node, cc *grpc.ClientConn) *nodeStatus {
	return &nodeStatus{
		Node:       node,
		ClientConn: cc,
		statuses:   make([]healthpb.HealthCheckResponse_ServingStatus, 0),
	}
}

type nodeStatus struct {
	models.Node
	*grpc.ClientConn
	statuses []healthpb.HealthCheckResponse_ServingStatus
}

// GetStorage gets the storage name of a node
func (n *nodeStatus) GetStorage() string {
	return n.Storage
}

// GetAddress gets the address of a node
func (n *nodeStatus) GetAddress() string {
	return n.Address
}

// GetToken gets the token of a node
func (n *nodeStatus) GetToken() string {
	return n.Token
}

// GetConnection gets the client connection of a node
func (n *nodeStatus) GetConnection() *grpc.ClientConn {
	return n.ClientConn
}

func (n *nodeStatus) isHealthy() bool {
	if len(n.statuses) < healthcheckThreshold {
		return false
	}

	for _, status := range n.statuses[len(n.statuses)-healthcheckThreshold:] {
		if status != healthpb.HealthCheckResponse_SERVING {
			return false
		}
	}

	return true
}

func (n *nodeStatus) check() error {
	client := healthpb.NewHealthClient(n.ClientConn)
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	resp, err := client.Check(ctx, &healthpb.HealthCheckRequest{Service: "TestService"})
	if err != nil {
		resp = &healthpb.HealthCheckResponse{
			Status: healthpb.HealthCheckResponse_UNKNOWN,
		}
	}

	n.statuses = append(n.statuses, resp.Status)
	if len(n.statuses) > healthcheckThreshold {
		n.statuses = n.statuses[1:]
	}

	return err
}
