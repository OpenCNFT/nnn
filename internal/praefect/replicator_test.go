package praefect

import (
	"context"
	"io/ioutil"
	"log"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	gitalyauth "gitlab.com/gitlab-org/gitaly/auth"
	gitaly_config "gitlab.com/gitlab-org/gitaly/internal/config"
	"gitlab.com/gitlab-org/gitaly/internal/git/objectpool"
	"gitlab.com/gitlab-org/gitaly/internal/helper"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/config"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/datastore"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/models"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/nodes"
	"gitlab.com/gitlab-org/gitaly/internal/rubyserver"
	serverPkg "gitlab.com/gitlab-org/gitaly/internal/server"
	objectpoolservice "gitlab.com/gitlab-org/gitaly/internal/service/objectpool"
	"gitlab.com/gitlab-org/gitaly/internal/service/remote"
	"gitlab.com/gitlab-org/gitaly/internal/service/repository"
	"gitlab.com/gitlab-org/gitaly/internal/service/ssh"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper/promtest"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/reflection"
)

func TestProcessReplicationJob(t *testing.T) {
	srv, srvSocketPath := runFullGitalyServer(t)
	defer srv.Stop()

	testRepo, testRepoPath, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	backupStorageName := "backup"

	backupDir, err := ioutil.TempDir(testhelper.GitlabTestStoragePath(), backupStorageName)
	require.NoError(t, err)

	defer func() {
		os.RemoveAll(backupDir)
	}()

	oldStorages := gitaly_config.Config.Storages
	defer func() {
		gitaly_config.Config.Storages = oldStorages
	}()

	gitaly_config.Config.Storages = append(gitaly_config.Config.Storages, gitaly_config.Storage{
		Name: backupStorageName,
		Path: backupDir,
	},
		gitaly_config.Storage{
			Name: "default",
			Path: testhelper.GitlabTestStoragePath(),
		},
	)

	conf := config.Config{
		VirtualStorages: []*config.VirtualStorage{
			&config.VirtualStorage{
				Name: "default",
				Nodes: []*models.Node{
					&models.Node{
						Storage:        "default",
						Address:        srvSocketPath,
						Token:          gitaly_config.Config.Auth.Token,
						DefaultPrimary: true,
					},
					&models.Node{
						Storage: backupStorageName,
						Address: srvSocketPath,
						Token:   gitaly_config.Config.Auth.Token,
					},
				},
			},
		},
	}

	ds := datastore.MemoryQueue{
		MemoryDatastore:       datastore.NewInMemory(conf),
		ReplicationEventQueue: datastore.NewMemoryReplicationEventQueue(),
	}

	// create object pool on the source
	objectPoolPath := testhelper.NewTestObjectPoolName(t)
	pool, err := objectpool.NewObjectPool(testRepo.GetStorageName(), objectPoolPath)
	require.NoError(t, err)

	poolCtx, cancel := testhelper.Context()
	defer cancel()

	require.NoError(t, pool.Create(poolCtx, testRepo))
	require.NoError(t, pool.Link(poolCtx, testRepo))

	// replicate object pool repository to target node
	targetObjectPoolRepo := *pool.ToProto().GetRepository()
	targetObjectPoolRepo.StorageName = "backup"

	ctx, cancel := testhelper.Context()
	defer cancel()

	injectedCtx := metadata.NewOutgoingContext(ctx, testhelper.GitalyServersMetadata(t, srvSocketPath))

	repoClient, con := newRepositoryClient(t, srvSocketPath)
	defer con.Close()

	_, err = repoClient.ReplicateRepository(injectedCtx, &gitalypb.ReplicateRepositoryRequest{
		Repository: &targetObjectPoolRepo,
		Source:     pool.ToProto().GetRepository(),
	})
	require.NoError(t, err)

	primary, err := ds.GetPrimary(conf.VirtualStorages[0].Name)
	require.NoError(t, err)
	secondaries, err := ds.GetSecondaries(conf.VirtualStorages[0].Name)
	require.NoError(t, err)

	var jobs []datastore.ReplJob
	for _, secondary := range secondaries {
		jobs = append(jobs, datastore.ReplJob{
			Change:        datastore.UpdateRepo,
			TargetNode:    secondary,
			SourceNode:    primary,
			RelativePath:  testRepo.GetRelativePath(),
			State:         datastore.JobStateReady,
			Attempts:      3,
			CorrelationID: "correlation-id",
		})
	}
	require.Len(t, jobs, 1)

	commitID := testhelper.CreateCommit(t, testRepoPath, "master", &testhelper.CreateCommitOpts{
		Message: "a commit",
	})

	var replicator defaultReplicator
	entry := testhelper.DiscardTestEntry(t)
	replicator.log = entry

	nodeMgr, err := nodes.NewManager(entry, conf, promtest.NewMockHistogramVec())
	require.NoError(t, err)
	nodeMgr.Start(1*time.Millisecond, 5*time.Millisecond)

	var mockReplicationGauge promtest.MockGauge
	var mockReplicationHistogram promtest.MockHistogram

	replMgr := NewReplMgr("", testhelper.DiscardTestEntry(t), ds, nodeMgr, WithLatencyMetric(&mockReplicationHistogram), WithQueueMetric(&mockReplicationGauge))
	replMgr.replicator = replicator

	shard, err := nodeMgr.GetShard(conf.VirtualStorages[0].Name)
	require.NoError(t, err)
	primaryNode, err := shard.GetPrimary()
	require.NoError(t, err)
	secondaryNodes, err := shard.GetSecondaries()
	require.NoError(t, err)
	require.Len(t, secondaryNodes, 1)

	replMgr.processReplJob(ctx, jobs[0], primaryNode.GetConnection(), secondaryNodes[0].GetConnection())

	relativeRepoPath, err := filepath.Rel(testhelper.GitlabTestStoragePath(), testRepoPath)
	require.NoError(t, err)
	replicatedPath := filepath.Join(backupDir, relativeRepoPath)

	testhelper.MustRunCommand(t, nil, "git", "-C", replicatedPath, "cat-file", "-e", commitID)
	testhelper.MustRunCommand(t, nil, "git", "-C", replicatedPath, "gc")
	require.Less(t, testhelper.GetGitPackfileDirSize(t, replicatedPath), int64(100), "expect a small pack directory")

	require.Equal(t, 1, mockReplicationGauge.IncsCalled())
	require.Equal(t, 1, mockReplicationGauge.DecsCalled())
	require.Len(t, mockReplicationHistogram.Values, 1)
}

func TestConfirmReplication(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	srv, srvSocketPath := runFullGitalyServer(t)
	defer srv.Stop()

	testRepoA, testRepoAPath, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	testRepoB, _, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	connOpts := []grpc.DialOption{
		grpc.WithInsecure(),
		grpc.WithPerRPCCredentials(gitalyauth.RPCCredentials(testhelper.RepositoryAuthToken)),
	}
	conn, err := grpc.Dial(srvSocketPath, connOpts...)
	require.NoError(t, err)

	var replicator defaultReplicator
	entry := testhelper.DiscardTestEntry(t)
	replicator.log = entry

	equal, err := replicator.confirmChecksums(ctx, gitalypb.NewRepositoryServiceClient(conn), gitalypb.NewRepositoryServiceClient(conn), testRepoA, testRepoB)
	require.NoError(t, err)
	require.True(t, equal)

	testhelper.CreateCommit(t, testRepoAPath, "master", &testhelper.CreateCommitOpts{
		Message: "a commit",
	})

	equal, err = replicator.confirmChecksums(ctx, gitalypb.NewRepositoryServiceClient(conn), gitalypb.NewRepositoryServiceClient(conn), testRepoA, testRepoB)
	require.NoError(t, err)
	require.False(t, equal)
}

func TestProcessBacklog_FailedJobs(t *testing.T) {
	primarySvr, primarySocket := newReplicationService(t)
	defer primarySvr.Stop()

	backupSvr, backupSocket := newReplicationService(t)
	defer backupSvr.Stop()

	internalListener, err := net.Listen("unix", gitaly_config.GitalyInternalSocketPath())
	require.NoError(t, err)
	go backupSvr.Serve(internalListener)

	testRepo, _, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	backupStorageName := "backup"

	backupDir, err := ioutil.TempDir(testhelper.GitlabTestStoragePath(), backupStorageName)
	require.NoError(t, err)

	defer os.RemoveAll(backupDir)

	primary := models.Node{
		Storage:        "default",
		Address:        "unix://" + primarySocket,
		DefaultPrimary: true,
	}

	secondary := models.Node{
		Storage: backupStorageName,
		Address: "unix://" + backupSocket,
	}

	conf := config.Config{
		VirtualStorages: []*config.VirtualStorage{
			{
				Name: "default",
				Nodes: []*models.Node{
					&primary,
					&secondary,
				},
			},
		},
	}

	ctx, cancel := testhelper.Context()
	defer cancel()

	defer func(oldStorages []gitaly_config.Storage) { gitaly_config.Config.Storages = oldStorages }(gitaly_config.Config.Storages)

	gitaly_config.Config.Storages = append(gitaly_config.Config.Storages, gitaly_config.Storage{
		Name: backupStorageName,
		Path: backupDir,
	})

	require.Len(t, gitaly_config.Config.Storages, 2, "expected 'default' storage and a new one")

	queueInterceptor := datastore.NewReplicationEventQueueInterceptor(datastore.NewMemoryReplicationEventQueue())
	processed := make(chan struct{})

	dequeues := 0
	queueInterceptor.OnDequeue(func(ctx context.Context, target string, count int, queue datastore.ReplicationEventQueue) ([]datastore.ReplicationEvent, error) {
		events, err := queue.Dequeue(ctx, target, count)
		if len(events) > 0 {
			dequeues++
		}
		return events, err
	})

	completedAcks := 0
	failedAcks := 0
	cancelledAcks := 0

	queueInterceptor.OnAcknowledge(func(ctx context.Context, state datastore.JobState, ids []uint64, queue datastore.ReplicationEventQueue) ([]uint64, error) {
		switch state {
		case datastore.JobStateCompleted:
			require.Equal(t, []uint64{1}, ids)
			completedAcks++
		case datastore.JobStateFailed:
			require.Equal(t, []uint64{2}, ids)
			failedAcks++
		case datastore.JobStateCancelled:
			require.Equal(t, []uint64{2}, ids)
			cancelledAcks++
		default:
			require.FailNow(t, "acknowledge is not expected", state)
		}
		ackIDs, err := queue.Acknowledge(ctx, state, ids)
		if completedAcks+failedAcks+cancelledAcks == 4 {
			close(processed)
		}
		return ackIDs, err
	})

	ds := datastore.MemoryQueue{
		ReplicationEventQueue: queueInterceptor,
		MemoryDatastore:       datastore.NewInMemory(conf),
	}

	// this job exists to verify that replication works
	okJob := datastore.ReplicationJob{
		Change:            datastore.UpdateRepo,
		RelativePath:      testRepo.RelativePath,
		TargetNodeStorage: secondary.Storage,
		SourceNodeStorage: primary.Storage,
	}
	event1, err := ds.ReplicationEventQueue.Enqueue(ctx, datastore.ReplicationEvent{Job: okJob})
	require.NoError(t, err)
	require.Equal(t, uint64(1), event1.ID)

	// this job checks flow for replication event that fails
	failJob := okJob
	failJob.RelativePath = "invalid path to fail the job"
	event2, err := ds.ReplicationEventQueue.Enqueue(ctx, datastore.ReplicationEvent{Job: failJob})
	require.NoError(t, err)
	require.Equal(t, uint64(2), event2.ID)

	logEntry := testhelper.DiscardTestEntry(t)

	nodeMgr, err := nodes.NewManager(logEntry, conf, promtest.NewMockHistogramVec())
	require.NoError(t, err)

	replMgr := NewReplMgr("default", logEntry, ds, nodeMgr)

	go func() {
		require.Equal(t, context.Canceled, replMgr.ProcessBacklog(ctx, noopBackoffFunc), "backlog processing failed")
	}()

	select {
	case <-processed:
	case <-time.After(60 * time.Second):
		// strongly depends on the processing capacity
		t.Fatal("time limit expired for job to complete")
	}

	require.Equal(t, 3, dequeues, "expected 1 deque to get [okJob, failJob] and 2 more for [failJob] only")
	require.Equal(t, 2, failedAcks)
	require.Equal(t, 1, cancelledAcks)
	require.Equal(t, 1, completedAcks)
}

func TestProcessBacklog_Success(t *testing.T) {
	primarySvr, primarySocket := newReplicationService(t)
	defer primarySvr.Stop()

	backupSvr, backupSocket := newReplicationService(t)
	defer backupSvr.Stop()

	internalListener, err := net.Listen("unix", gitaly_config.GitalyInternalSocketPath())
	require.NoError(t, err)
	go backupSvr.Serve(internalListener)

	testRepo, _, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	backupStorageName := "backup"

	backupDir, err := ioutil.TempDir(testhelper.GitlabTestStoragePath(), backupStorageName)
	require.NoError(t, err)

	defer os.RemoveAll(backupDir)

	primary := models.Node{
		Storage:        "default",
		Address:        "unix://" + primarySocket,
		DefaultPrimary: true,
	}

	secondary := models.Node{
		Storage: backupStorageName,
		Address: "unix://" + backupSocket,
	}

	conf := config.Config{
		VirtualStorages: []*config.VirtualStorage{
			{
				Name: "default",
				Nodes: []*models.Node{
					&primary,
					&secondary,
				},
			},
		},
	}

	ctx, cancel := testhelper.Context()
	defer cancel()

	defer func(oldStorages []gitaly_config.Storage) { gitaly_config.Config.Storages = oldStorages }(gitaly_config.Config.Storages)

	gitaly_config.Config.Storages = append(gitaly_config.Config.Storages, gitaly_config.Storage{
		Name: backupStorageName,
		Path: backupDir,
	})
	require.Len(t, gitaly_config.Config.Storages, 2, "expected 'default' storage and a new one")

	queueInterceptor := datastore.NewReplicationEventQueueInterceptor(datastore.NewMemoryReplicationEventQueue())

	processed := make(chan struct{})
	queueInterceptor.OnAcknowledge(func(ctx context.Context, state datastore.JobState, ids []uint64, queue datastore.ReplicationEventQueue) ([]uint64, error) {
		ackIDs, err := queue.Acknowledge(ctx, state, ids)
		if len(ids) > 0 {
			require.Equal(t, datastore.JobStateCompleted, state, "no fails expected")
			require.Equal(t, []uint64{1, 2, 3, 4}, ids, "all jobs must be processed at once")
			close(processed)
		}
		return ackIDs, err
	})

	ds := datastore.MemoryQueue{
		MemoryDatastore:       datastore.NewInMemory(conf),
		ReplicationEventQueue: queueInterceptor,
	}

	// Update replication job
	eventType1 := datastore.ReplicationEvent{
		Job: datastore.ReplicationJob{
			Change:            datastore.UpdateRepo,
			RelativePath:      testRepo.GetRelativePath(),
			TargetNodeStorage: secondary.Storage,
			SourceNodeStorage: primary.Storage,
		},
	}

	_, err = ds.ReplicationEventQueue.Enqueue(ctx, eventType1)
	require.NoError(t, err)

	_, err = ds.ReplicationEventQueue.Enqueue(ctx, eventType1)
	require.NoError(t, err)

	renameTo1 := filepath.Join(testRepo.GetRelativePath(), "..", filepath.Base(testRepo.GetRelativePath())+"-mv1")
	fullNewPath1 := filepath.Join(backupDir, renameTo1)

	renameTo2 := filepath.Join(renameTo1, "..", filepath.Base(testRepo.GetRelativePath())+"-mv2")
	fullNewPath2 := filepath.Join(backupDir, renameTo2)

	// Rename replication job
	eventType2 := datastore.ReplicationEvent{
		Job: datastore.ReplicationJob{
			Change:            datastore.RenameRepo,
			RelativePath:      testRepo.GetRelativePath(),
			TargetNodeStorage: secondary.Storage,
			SourceNodeStorage: primary.Storage,
			Params:            datastore.Params{"RelativePath": renameTo1},
		},
	}

	_, err = ds.ReplicationEventQueue.Enqueue(ctx, eventType2)
	require.NoError(t, err)

	// Rename replication job
	eventType3 := datastore.ReplicationEvent{
		Job: datastore.ReplicationJob{
			Change:            datastore.RenameRepo,
			RelativePath:      renameTo1,
			TargetNodeStorage: secondary.Storage,
			SourceNodeStorage: primary.Storage,
			Params:            datastore.Params{"RelativePath": renameTo2},
		},
	}
	require.NoError(t, err)

	_, err = ds.ReplicationEventQueue.Enqueue(ctx, eventType3)
	require.NoError(t, err)

	logEntry := testhelper.DiscardTestEntry(t)

	nodeMgr, err := nodes.NewManager(logEntry, conf, promtest.NewMockHistogramVec())
	require.NoError(t, err)

	replMgr := NewReplMgr("default", logEntry, ds, nodeMgr)

	go func() {
		require.Equal(t, context.Canceled, replMgr.ProcessBacklog(ctx, noopBackoffFunc), "backlog processing failed")
	}()

	select {
	case <-processed:
	case <-time.After(60 * time.Second):
		// strongly depends on the processing capacity
		t.Fatal("time limit expired for job to complete")
	}

	_, serr := os.Stat(fullNewPath1)
	require.True(t, os.IsNotExist(serr), "repository must be moved from %q to the new location", fullNewPath1)
	require.True(t, helper.IsGitDirectory(fullNewPath2), "repository must exist at new last RenameRepository location")
}

func TestBackoff(t *testing.T) {
	start := 1 * time.Microsecond
	max := 6 * time.Microsecond
	expectedBackoffs := []time.Duration{
		1 * time.Microsecond,
		2 * time.Microsecond,
		4 * time.Microsecond,
		6 * time.Microsecond,
		6 * time.Microsecond,
		6 * time.Microsecond,
	}
	b, reset := ExpBackoffFunc(start, max)()
	for _, expectedBackoff := range expectedBackoffs {
		require.Equal(t, expectedBackoff, b())
	}

	reset()
	require.Equal(t, start, b())
}

func runFullGitalyServer(t *testing.T) (*grpc.Server, string) {
	server := serverPkg.NewInsecure(RubyServer)

	serverSocketPath := testhelper.GetTemporaryGitalySocketFileName()

	listener, err := net.Listen("unix", serverSocketPath)
	if err != nil {
		t.Fatal(err)
	}
	//listen on internal socket
	internalListener, err := net.Listen("unix", gitaly_config.GitalyInternalSocketPath())
	require.NoError(t, err)

	go server.Serve(listener)
	go server.Serve(internalListener)

	return server, "unix://" + serverSocketPath
}

// newReplicationService is a grpc service that has the SSH, Repository, Remote and ObjectPool services, which
// are the only ones needed for replication
func newReplicationService(tb testing.TB) (*grpc.Server, string) {
	socketName := testhelper.GetTemporaryGitalySocketFileName()

	svr := testhelper.NewTestGrpcServer(tb, nil, nil)

	gitalypb.RegisterRepositoryServiceServer(svr, repository.NewServer(&rubyserver.Server{}, gitaly_config.GitalyInternalSocketPath()))
	gitalypb.RegisterObjectPoolServiceServer(svr, objectpoolservice.NewServer())
	gitalypb.RegisterRemoteServiceServer(svr, remote.NewServer(&rubyserver.Server{}))
	gitalypb.RegisterSSHServiceServer(svr, ssh.NewServer())
	reflection.Register(svr)

	listener, err := net.Listen("unix", socketName)
	require.NoError(tb, err)

	go svr.Serve(listener)

	return svr, socketName
}

func newRepositoryClient(t *testing.T, serverSocketPath string) (gitalypb.RepositoryServiceClient, *grpc.ClientConn) {
	connOpts := []grpc.DialOption{
		grpc.WithInsecure(),
		grpc.WithPerRPCCredentials(gitalyauth.RPCCredentials(testhelper.RepositoryAuthToken)),
	}
	conn, err := grpc.Dial(serverSocketPath, connOpts...)
	if err != nil {
		t.Fatal(err)
	}

	return gitalypb.NewRepositoryServiceClient(conn), conn
}

var RubyServer = &rubyserver.Server{}

func TestMain(m *testing.M) {
	os.Exit(testMain(m))
}

func testMain(m *testing.M) int {
	defer testhelper.MustHaveNoChildProcess()

	gitaly_config.Config.Auth.Token = testhelper.RepositoryAuthToken

	var err error
	gitaly_config.Config.GitlabShell.Dir, err = filepath.Abs("testdata/gitlab-shell")
	if err != nil {
		log.Fatal(err)
	}

	testhelper.ConfigureGitalySSH()

	if err := RubyServer.Start(); err != nil {
		log.Fatal(err)
	}
	defer RubyServer.Stop()

	return m.Run()
}

func TestSubtractUint64(t *testing.T) {
	testCases := []struct {
		desc  string
		left  []uint64
		right []uint64
		exp   []uint64
	}{
		{desc: "empty left", left: nil, right: []uint64{1, 2}, exp: nil},
		{desc: "empty right", left: []uint64{1, 2}, right: []uint64{}, exp: []uint64{1, 2}},
		{desc: "some exists", left: []uint64{1, 2, 3, 4, 5}, right: []uint64{2, 4, 5}, exp: []uint64{1, 3}},
		{desc: "nothing exists", left: []uint64{10, 20}, right: []uint64{100, 200}, exp: []uint64{10, 20}},
		{desc: "duplicates exists", left: []uint64{1, 1, 2, 3, 3, 4, 4, 5}, right: []uint64{3, 4, 4, 5}, exp: []uint64{1, 1, 2}},
	}

	for _, testCase := range testCases {
		t.Run(testCase.desc, func(t *testing.T) {
			require.Equal(t, testCase.exp, subtractUint64(testCase.left, testCase.right))
		})
	}
}
