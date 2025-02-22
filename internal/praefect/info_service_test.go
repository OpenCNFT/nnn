package praefect

import (
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/gittest"
	gconfig "gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/service"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/service/repository"
	"gitlab.com/gitlab-org/gitaly/v16/internal/grpc/backchannel"
	"gitlab.com/gitlab-org/gitaly/v16/internal/grpc/protoregistry"
	"gitlab.com/gitlab-org/gitaly/v16/internal/grpc/sidechannel"
	"gitlab.com/gitlab-org/gitaly/v16/internal/praefect/config"
	"gitlab.com/gitlab-org/gitaly/v16/internal/praefect/datastore"
	"gitlab.com/gitlab-org/gitaly/v16/internal/praefect/nodes"
	"gitlab.com/gitlab-org/gitaly/v16/internal/praefect/service/transaction"
	"gitlab.com/gitlab-org/gitaly/v16/internal/praefect/transactions"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper/testdb"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper/testserver"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
	"google.golang.org/grpc"
)

func TestInfoService_RepositoryReplicas(t *testing.T) {
	t.Parallel()

	var cfgs []gconfig.Cfg
	var cfgNodes []*config.Node
	storages := []string{"g-1", "g-2", "g-3"}

	for i, storage := range storages {
		cfg := testcfg.Build(t, testcfg.WithStorages(storage))
		cfgs = append(cfgs, cfg)
		cfgs[i].SocketPath = testserver.RunGitalyServer(t, cfgs[i], func(srv *grpc.Server, deps *service.Dependencies) {
			gitalypb.RegisterRepositoryServiceServer(srv, repository.NewServer(deps))
		}, testserver.WithDisablePraefect())
		cfgNodes = append(cfgNodes, &config.Node{
			Storage: cfgs[i].Storages[0].Name,
			Address: cfgs[i].SocketPath,
			Token:   cfgs[i].Auth.Token,
		})
	}

	const virtualStorage = "default"
	conf := config.Config{
		VirtualStorages: []*config.VirtualStorage{{Name: virtualStorage, Nodes: cfgNodes}},
		Failover:        config.Failover{ElectionStrategy: config.ElectionStrategyPerRepository},
	}

	ctx := testhelper.Context(t)

	db := testdb.New(t)
	logger := testhelper.SharedLogger(t)
	// the only thing used from the config is the grpc_latency_buckets which is not relevant for the test
	txManager := transactions.NewManager(config.Config{}, logger)
	sidechannelRegistry := sidechannel.NewRegistry()
	nodeSet, err := DialNodes(
		ctx,
		conf.VirtualStorages,
		protoregistry.GitalyProtoPreregistered,
		nil,
		backchannel.NewClientHandshaker(
			logger,
			NewBackchannelServerFactory(
				logger,
				transaction.NewServer(txManager),
				sidechannelRegistry,
			),
			backchannel.DefaultConfiguration(),
		),
		sidechannelRegistry,
		testhelper.SharedLogger(t),
	)
	require.NoError(t, err)
	t.Cleanup(nodeSet.Close)

	// Use a transaction in the elector itself to avoid flakiness due to the health timeout. We mark
	// only the first repo has healthy so the elector picks it always as the primary.
	tx := db.Begin(t)
	t.Cleanup(func() { tx.Rollback(t) })
	testdb.SetHealthyNodes(t, ctx, tx, map[string]map[string][]string{
		"praefect-0": {virtualStorage: storages[0:1]},
	})
	elector := nodes.NewPerRepositoryElector(logger, tx)

	conns := nodeSet.Connections()
	rs := datastore.NewPostgresRepositoryStore(db, conf.StorageNames())

	cc, _, cleanup := RunPraefectServer(t, ctx, conf, BuildOptions{
		WithConnections: conns,
		WithRepoStore:   rs,
		WithRouter: NewPerRepositoryRouter(
			conns,
			elector,
			StaticHealthChecker{virtualStorage: storages},
			NewLockedRandom(rand.New(rand.NewSource(0))),
			rs,
			datastore.NewAssignmentStore(db, conf.StorageNames()),
			rs,
			conf.DefaultReplicationFactors(),
		),
		WithPrimaryGetter: elector,
		WithTxMgr:         txManager,
	})
	// use cleanup to close the connections as gittest.CreateRepository will still use the connection
	// for clean up after the test.
	t.Cleanup(cleanup)

	client := gitalypb.NewPraefectInfoServiceClient(cc)

	testRepository, testRepoPath := gittest.CreateRepository(t, ctx,
		// The helper was implemented with the test server in mind. Here we need use the virtual storage's name
		// as the storage and the path of the storage we want to modify the replica in.
		gconfig.Cfg{Storages: []gconfig.Storage{{Name: virtualStorage, Path: cfgs[1].Storages[0].Path}}},
		gittest.CreateRepositoryConfig{ClientConn: cc},
	)

	// create a commit in the second replica so we can check that its checksum is different than the primary
	gittest.WriteCommit(t, cfgs[1], testRepoPath, gittest.WithBranch("master"))

	// Increment the generation of the unmodified repositories so the below CalculateChecksum calls goes to one of them
	// as the test expects the primary to have that checksum.
	require.NoError(t, rs.IncrementGeneration(ctx, 1, cfgs[0].Storages[0].Name, []string{cfgs[2].Storages[0].Name}))

	// CalculateChecksum through praefect will get the checksum of the primary
	checksum, err := gitalypb.NewRepositoryServiceClient(cc).CalculateChecksum(ctx, &gitalypb.CalculateChecksumRequest{Repository: testRepository})
	require.NoError(t, err)

	resp, err := client.RepositoryReplicas(ctx, &gitalypb.RepositoryReplicasRequest{Repository: testRepository})
	require.NoError(t, err)

	require.Equal(t, checksum.GetChecksum(), resp.GetPrimary().GetChecksum())
	var checked []string
	for _, secondary := range resp.GetReplicas() {
		switch storage := secondary.GetRepository().GetStorageName(); storage {
		case conf.VirtualStorages[0].Nodes[1].Storage:
			require.NotEqual(t, checksum.GetChecksum(), secondary.GetChecksum(), "should not be equal since we added a commit")
			checked = append(checked, storage)
		case conf.VirtualStorages[0].Nodes[2].Storage:
			require.Equal(t, checksum.GetChecksum(), secondary.GetChecksum())
			checked = append(checked, storage)
		default:
			require.FailNow(t, "unexpected storage: %q", storage)
		}
	}
	require.ElementsMatch(t, []string{conf.VirtualStorages[0].Nodes[1].Storage, conf.VirtualStorages[0].Nodes[2].Storage}, checked)
}
