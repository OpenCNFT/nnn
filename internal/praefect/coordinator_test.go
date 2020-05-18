package praefect

import (
	"context"
	"errors"
	"fmt"
	"io/ioutil"
	"sync"
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/middleware/metadatahandler"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/config"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/datastore"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/models"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/nodes"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/protoregistry"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/transactions"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper/promtest"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"gitlab.com/gitlab-org/labkit/correlation"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
)

var testLogger = logrus.New()

func init() {
	testLogger.SetOutput(ioutil.Discard)
}

func TestSecondaryRotation(t *testing.T) {
	t.Skip("secondary rotation will change with the new data model")
}

type mockNodeManager struct {
	GetShardFunc func(string) (nodes.Shard, error)
}

func (m *mockNodeManager) GetShard(storage string) (nodes.Shard, error) {
	return m.GetShardFunc(storage)
}

func (m *mockNodeManager) EnableWrites(context.Context, string) error { panic("unimplemented") }

type mockNode struct {
	nodes.Node
	storageName string
}

func (m *mockNode) GetStorage() string { return m.storageName }

func (m *mockNode) GetConnection() *grpc.ClientConn { return nil }

func TestStreamDirectorReadOnlyEnforcement(t *testing.T) {
	for _, tc := range []struct {
		readOnly        bool
		readOnlyEnabled bool
		shouldError     bool
	}{
		{
			readOnly:        false,
			readOnlyEnabled: true,
			shouldError:     false,
		},
		{
			readOnly:        true,
			readOnlyEnabled: true,
			shouldError:     true,
		},
		{
			readOnly:        false,
			readOnlyEnabled: false,
			shouldError:     false,
		},
		{
			readOnly:        true,
			readOnlyEnabled: false,
			shouldError:     false,
		},
	} {
		t.Run(fmt.Sprintf("read-only: %v, enabled: %v", tc.readOnly, tc.readOnlyEnabled), func(t *testing.T) {
			conf := config.Config{
				Failover: config.Failover{ReadOnlyAfterFailover: tc.readOnlyEnabled},
				VirtualStorages: []*config.VirtualStorage{
					&config.VirtualStorage{
						Name: "praefect",
						Nodes: []*models.Node{
							&models.Node{
								Address:        "tcp://gitaly-primary.example.com",
								Storage:        "praefect-internal-1",
								DefaultPrimary: true,
							},
						},
					},
				},
			}
			pbRegistry := protoregistry.New()
			require.NoError(t, pbRegistry.RegisterFiles(protoregistry.GitalyProtoFileDescriptors...))

			const storageName = "test-storage"
			coordinator := NewCoordinator(
				testhelper.DiscardTestEntry(t),
				datastore.Datastore{datastore.NewInMemory(conf), datastore.NewMemoryReplicationEventQueue()},
				&mockNodeManager{GetShardFunc: func(storage string) (nodes.Shard, error) {
					return nodes.Shard{
						IsReadOnly: tc.readOnly,
						Primary:    &mockNode{storageName: storageName},
					}, nil
				}},
				transactions.NewManager(),
				conf,
				pbRegistry,
			)

			ctx, cancel := testhelper.Context()
			defer cancel()

			frame, err := proto.Marshal(&gitalypb.CleanupRequest{Repository: &gitalypb.Repository{
				StorageName:  storageName,
				RelativePath: "only-for-validation",
			}})
			require.NoError(t, err)

			_, err = coordinator.StreamDirector(ctx, "/gitaly.RepositoryService/Cleanup", &mockPeeker{frame: frame})
			if tc.shouldError {
				require.True(t, errors.Is(err, ReadOnlyStorageError(storageName)))
				testhelper.RequireGrpcError(t, err, codes.FailedPrecondition)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestStreamDirectorMutator(t *testing.T) {
	gitalySocket0, gitalySocket1 := testhelper.GetTemporaryGitalySocketFileName(), testhelper.GetTemporaryGitalySocketFileName()
	_, healthSrv0 := testhelper.NewServerWithHealth(t, gitalySocket0)
	_, healthSrv1 := testhelper.NewServerWithHealth(t, gitalySocket1)
	healthSrv0.SetServingStatus("", grpc_health_v1.HealthCheckResponse_SERVING)
	healthSrv1.SetServingStatus("", grpc_health_v1.HealthCheckResponse_SERVING)

	primaryAddress, secondaryAddress := "unix://"+gitalySocket0, "unix://"+gitalySocket1
	conf := config.Config{
		VirtualStorages: []*config.VirtualStorage{
			&config.VirtualStorage{
				Name: "praefect",
				Nodes: []*models.Node{
					&models.Node{
						Address:        primaryAddress,
						Storage:        "praefect-internal-1",
						DefaultPrimary: true,
					},
					&models.Node{
						Address: secondaryAddress,
						Storage: "praefect-internal-2",
					}},
			},
		},
	}

	var replEventWait sync.WaitGroup

	queueInterceptor := datastore.NewReplicationEventQueueInterceptor(datastore.NewMemoryReplicationEventQueue())
	queueInterceptor.OnEnqueue(func(ctx context.Context, event datastore.ReplicationEvent, queue datastore.ReplicationEventQueue) (datastore.ReplicationEvent, error) {
		defer replEventWait.Done()
		return queue.Enqueue(ctx, event)
	})

	ds := datastore.Datastore{
		ReplicasDatastore:     datastore.NewInMemory(conf),
		ReplicationEventQueue: queueInterceptor,
	}

	targetRepo := gitalypb.Repository{
		StorageName:  "praefect",
		RelativePath: "/path/to/hashed/storage",
	}

	ctx, cancel := testhelper.Context()
	defer cancel()

	entry := testhelper.DiscardTestEntry(t)

	nodeMgr, err := nodes.NewManager(entry, conf, nil, promtest.NewMockHistogramVec())
	require.NoError(t, err)
	r := protoregistry.New()
	require.NoError(t, r.RegisterFiles(protoregistry.GitalyProtoFileDescriptors...))
	txMgr := transactions.NewManager()

	coordinator := NewCoordinator(entry, ds, nodeMgr, txMgr, conf, r)

	frame, err := proto.Marshal(&gitalypb.FetchIntoObjectPoolRequest{
		Origin:     &targetRepo,
		ObjectPool: &gitalypb.ObjectPool{Repository: &targetRepo},
		Repack:     false,
	})
	require.NoError(t, err)

	fullMethod := "/gitaly.ObjectPoolService/FetchIntoObjectPool"

	peeker := &mockPeeker{frame}
	streamParams, err := coordinator.StreamDirector(correlation.ContextWithCorrelation(ctx, "my-correlation-id"), fullMethod, peeker)
	require.NoError(t, err)
	require.Equal(t, primaryAddress, streamParams.Conn().Target())

	md, ok := metadata.FromOutgoingContext(streamParams.Context())
	require.True(t, ok)
	require.Contains(t, md, "praefect-server")

	mi, err := coordinator.registry.LookupMethod(fullMethod)
	require.NoError(t, err)

	m, err := protoMessageFromPeeker(mi, peeker)
	require.NoError(t, err)

	rewrittenTargetRepo, err := mi.TargetRepo(m)
	require.NoError(t, err)
	require.Equal(t, "praefect-internal-1", rewrittenTargetRepo.GetStorageName(), "stream director should have rewritten the storage name")

	replEventWait.Add(1) // expected only one event to be created
	// this call creates new events in the queue and simulates usual flow of the update operation
	streamParams.RequestFinalizer()

	targetNode, err := ds.GetStorageNode("praefect-internal-2")
	require.NoError(t, err)
	sourceNode, err := ds.GetStorageNode("praefect-internal-1")
	require.NoError(t, err)

	replEventWait.Wait() // wait until event persisted (async operation)
	events, err := ds.ReplicationEventQueue.Dequeue(ctx, "praefect", "praefect-internal-2", 10)
	require.NoError(t, err)
	require.Len(t, events, 1)

	expectedEvent := datastore.ReplicationEvent{
		ID:        1,
		State:     datastore.JobStateInProgress,
		Attempt:   2,
		LockID:    "praefect|praefect-internal-2|/path/to/hashed/storage",
		CreatedAt: events[0].CreatedAt,
		UpdatedAt: events[0].UpdatedAt,
		Job: datastore.ReplicationJob{
			Change:            datastore.UpdateRepo,
			VirtualStorage:    conf.VirtualStorages[0].Name,
			RelativePath:      targetRepo.RelativePath,
			TargetNodeStorage: targetNode.Storage,
			SourceNodeStorage: sourceNode.Storage,
		},
		Meta: datastore.Params{metadatahandler.CorrelationIDKey: "my-correlation-id"},
	}
	require.Equal(t, expectedEvent, events[0], "ensure replication job created by stream director is correct")
}

func TestStreamDirectorAccessor(t *testing.T) {
	gitalySocket0, gitalySocket1 := testhelper.GetTemporaryGitalySocketFileName(), testhelper.GetTemporaryGitalySocketFileName()
	_, healthSrv0 := testhelper.NewServerWithHealth(t, gitalySocket0)
	_, healthSrv1 := testhelper.NewServerWithHealth(t, gitalySocket1)
	healthSrv0.SetServingStatus("", grpc_health_v1.HealthCheckResponse_SERVING)
	healthSrv1.SetServingStatus("", grpc_health_v1.HealthCheckResponse_SERVING)

	primaryAddress, secondaryAddress := "unix://"+gitalySocket0, "unix://"+gitalySocket1
	conf := config.Config{
		VirtualStorages: []*config.VirtualStorage{
			{
				Name: "praefect",
				Nodes: []*models.Node{
					{
						Address:        primaryAddress,
						Storage:        "praefect-internal-1",
						DefaultPrimary: true,
					},
					{
						Address: secondaryAddress,
						Storage: "praefect-internal-2",
					}},
			},
		},
	}

	ds := datastore.Datastore{
		ReplicasDatastore:     datastore.NewInMemory(conf),
		ReplicationEventQueue: datastore.NewMemoryReplicationEventQueue(),
	}

	targetRepo := gitalypb.Repository{
		StorageName:  "praefect",
		RelativePath: "/path/to/hashed/storage",
	}

	ctx, cancel := testhelper.Context()
	defer cancel()

	entry := testhelper.DiscardTestEntry(t)

	nodeMgr, err := nodes.NewManager(entry, conf, nil, promtest.NewMockHistogramVec())
	require.NoError(t, err)
	r := protoregistry.New()
	require.NoError(t, r.RegisterFiles(protoregistry.GitalyProtoFileDescriptors...))

	txMgr := transactions.NewManager()

	coordinator := NewCoordinator(entry, ds, nodeMgr, txMgr, conf, r)

	frame, err := proto.Marshal(&gitalypb.FindAllBranchesRequest{Repository: &targetRepo})
	require.NoError(t, err)

	fullMethod := "/gitaly.RefService/FindAllBranches"

	peeker := &mockPeeker{frame: frame}
	streamParams, err := coordinator.StreamDirector(correlation.ContextWithCorrelation(ctx, "my-correlation-id"), fullMethod, peeker)
	require.NoError(t, err)
	require.Equal(t, primaryAddress, streamParams.Conn().Target())

	md, ok := metadata.FromOutgoingContext(streamParams.Context())
	require.True(t, ok)
	require.Contains(t, md, "praefect-server")

	mi, err := coordinator.registry.LookupMethod(fullMethod)
	require.NoError(t, err)

	m, err := protoMessageFromPeeker(mi, peeker)
	require.NoError(t, err)

	rewrittenTargetRepo, err := mi.TargetRepo(m)
	require.NoError(t, err)
	require.Equal(t, "praefect-internal-1", rewrittenTargetRepo.GetStorageName(), "stream director should have rewritten the storage name")

	// must be invoked without issues
	streamParams.RequestFinalizer()
}

type mockPeeker struct {
	frame []byte
}

func (m *mockPeeker) Peek() ([]byte, error) {
	return m.frame, nil
}

func (m *mockPeeker) Modify(payload []byte) error {
	m.frame = payload

	return nil
}

func TestAbsentCorrelationID(t *testing.T) {
	gitalySocket0, gitalySocket1 := testhelper.GetTemporaryGitalySocketFileName(), testhelper.GetTemporaryGitalySocketFileName()
	_, healthSrv0 := testhelper.NewServerWithHealth(t, gitalySocket0)
	_, healthSrv1 := testhelper.NewServerWithHealth(t, gitalySocket1)
	healthSrv0.SetServingStatus("", grpc_health_v1.HealthCheckResponse_SERVING)
	healthSrv1.SetServingStatus("", grpc_health_v1.HealthCheckResponse_SERVING)

	primaryAddress, secondaryAddress := "unix://"+gitalySocket0, "unix://"+gitalySocket1
	conf := config.Config{
		VirtualStorages: []*config.VirtualStorage{
			&config.VirtualStorage{
				Name: "praefect",
				Nodes: []*models.Node{
					&models.Node{
						Address:        primaryAddress,
						Storage:        "praefect-internal-1",
						DefaultPrimary: true,
					},
					&models.Node{
						Address: secondaryAddress,
						Storage: "praefect-internal-2",
					}},
			},
		},
	}

	var replEventWait sync.WaitGroup

	queueInterceptor := datastore.NewReplicationEventQueueInterceptor(datastore.NewMemoryReplicationEventQueue())
	queueInterceptor.OnEnqueue(func(ctx context.Context, event datastore.ReplicationEvent, queue datastore.ReplicationEventQueue) (datastore.ReplicationEvent, error) {
		defer replEventWait.Done()
		return queue.Enqueue(ctx, event)
	})

	ds := datastore.Datastore{
		ReplicasDatastore:     datastore.NewInMemory(conf),
		ReplicationEventQueue: queueInterceptor,
	}

	targetRepo := gitalypb.Repository{
		StorageName:  "praefect",
		RelativePath: "/path/to/hashed/storage",
	}

	ctx, cancel := testhelper.Context()
	defer cancel()

	entry := testhelper.DiscardTestEntry(t)

	nodeMgr, err := nodes.NewManager(entry, conf, nil, promtest.NewMockHistogramVec())
	require.NoError(t, err)
	txMgr := transactions.NewManager()

	coordinator := NewCoordinator(entry, ds, nodeMgr, txMgr, conf, protoregistry.GitalyProtoPreregistered)

	frame, err := proto.Marshal(&gitalypb.FetchIntoObjectPoolRequest{
		Origin:     &targetRepo,
		ObjectPool: &gitalypb.ObjectPool{Repository: &targetRepo},
		Repack:     false,
	})
	require.NoError(t, err)

	fullMethod := "/gitaly.ObjectPoolService/FetchIntoObjectPool"
	peeker := &mockPeeker{frame}
	streamParams, err := coordinator.StreamDirector(ctx, fullMethod, peeker)
	require.NoError(t, err)
	require.Equal(t, primaryAddress, streamParams.Conn().Target())

	replEventWait.Add(1) // expected only one event to be created
	// must be run as it adds replication events to the queue
	streamParams.RequestFinalizer()

	replEventWait.Wait() // wait until event persisted (async operation)
	jobs, err := coordinator.datastore.Dequeue(ctx, conf.VirtualStorages[0].Name, conf.VirtualStorages[0].Nodes[1].Storage, 1)
	require.NoError(t, err)
	require.Len(t, jobs, 1)

	require.NotZero(t, jobs[0].Meta[metadatahandler.CorrelationIDKey],
		"the coordinator should have generated a random ID")
}
