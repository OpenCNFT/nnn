package testserver

import (
	"context"
	"net"
	"os"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	gitalyauth "gitlab.com/gitlab-org/gitaly/v16/auth"
	"gitlab.com/gitlab-org/gitaly/v16/client"
	"gitlab.com/gitlab-org/gitaly/v16/internal/backup"
	"gitlab.com/gitlab-org/gitaly/v16/internal/cache"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/catfile"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/housekeeping"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git2go"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/config/auth"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/hook"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/hook/updateref"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/server"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/service"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/counter"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/storagemgr"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/transaction"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitlab"
	"gitlab.com/gitlab-org/gitaly/v16/internal/grpc/backchannel"
	internalclient "gitlab.com/gitlab-org/gitaly/v16/internal/grpc/client"
	"gitlab.com/gitlab-org/gitaly/v16/internal/grpc/middleware/limithandler"
	"gitlab.com/gitlab-org/gitaly/v16/internal/helper/perm"
	"gitlab.com/gitlab-org/gitaly/v16/internal/limiter"
	gitalylog "gitlab.com/gitlab-org/gitaly/v16/internal/log"
	praefectconfig "gitlab.com/gitlab-org/gitaly/v16/internal/praefect/config"
	"gitlab.com/gitlab-org/gitaly/v16/internal/streamcache"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper/testdb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

// RunGitalyServer starts gitaly server based on the provided cfg and returns a connection address.
// It accepts addition Registrar to register all required service instead of
// calling service.RegisterAll explicitly because it creates a circular dependency
// when the function is used in on of internal/gitaly/service/... packages.
func RunGitalyServer(tb testing.TB, cfg config.Cfg, registrar func(srv *grpc.Server, deps *service.Dependencies), opts ...GitalyServerOpt) string {
	return StartGitalyServer(tb, cfg, registrar, opts...).Address()
}

// StartGitalyServer creates and runs gitaly (and praefect as a proxy) server.
func StartGitalyServer(tb testing.TB, cfg config.Cfg, registrar func(srv *grpc.Server, deps *service.Dependencies), opts ...GitalyServerOpt) GitalyServer {
	gitalySrv, gitalyAddr, disablePraefect := runGitaly(tb, cfg, registrar, opts...)

	if !testhelper.IsPraefectEnabled() || disablePraefect {
		return GitalyServer{
			Server:   gitalySrv,
			shutdown: gitalySrv.Stop,
			address:  gitalyAddr,
		}
	}

	praefectServer := runPraefectProxy(tb, cfg, gitalyAddr)
	return GitalyServer{
		Server: gitalySrv,
		shutdown: func() {
			praefectServer.Shutdown()
			gitalySrv.Stop()
		},
		address: praefectServer.Address(),
	}
}

func runPraefectProxy(tb testing.TB, gitalyCfg config.Cfg, gitalyAddr string) PraefectServer {
	var virtualStorages []*praefectconfig.VirtualStorage
	for _, storage := range gitalyCfg.Storages {
		virtualStorages = append(virtualStorages, &praefectconfig.VirtualStorage{
			Name: storage.Name,
			Nodes: []*praefectconfig.Node{
				{
					Storage: storage.Name,
					Address: gitalyAddr,
					Token:   gitalyCfg.Auth.Token,
				},
			},
		})
	}

	return StartPraefect(tb, praefectconfig.Config{
		SocketPath: testhelper.GetTemporaryGitalySocketFileName(tb),
		Auth: auth.Config{
			Token: gitalyCfg.Auth.Token,
		},
		DB: testdb.GetConfig(tb, testdb.New(tb).Name),
		Failover: praefectconfig.Failover{
			Enabled:          true,
			ElectionStrategy: praefectconfig.ElectionStrategyLocal,
		},
		BackgroundVerification: praefectconfig.BackgroundVerification{
			// Some tests cases purposefully create bad metadata by deleting a repository off
			// the disk. If the background verifier is running, it could find these repositories
			// and remove the invalid metadata related to them. This can cause the test assertions
			// to fail. As the background verifier runs asynchronously and possibly changes state
			// during a test and issues unexpected RPCs, it's disabled generally for all tests.
			VerificationInterval: -1,
		},
		Replication: praefectconfig.DefaultReplicationConfig(),
		Logging: gitalylog.Config{
			Format: "json",
			Level:  "info",
		},
		VirtualStorages: virtualStorages,
	})
}

// GitalyServer is a helper that carries additional info and
// functionality about gitaly (+praefect) server.
type GitalyServer struct {
	Server   *grpc.Server
	shutdown func()
	address  string
}

// Shutdown terminates running gitaly (+praefect) server.
func (gs GitalyServer) Shutdown() {
	gs.shutdown()
}

// Address returns address of the running gitaly (or praefect) service.
func (gs GitalyServer) Address() string {
	return gs.address
}

// waitHealthy waits until the server hosted at address becomes healthy.
func waitHealthy(tb testing.TB, ctx context.Context, addr string, authToken string) {
	grpcOpts := []grpc.DialOption{
		grpc.WithBlock(),
		internalclient.UnaryInterceptor(),
		internalclient.StreamInterceptor(),
	}
	if authToken != "" {
		grpcOpts = append(grpcOpts, grpc.WithPerRPCCredentials(gitalyauth.RPCCredentialsV2(authToken)))
	}

	conn, err := client.DialContext(ctx, addr, grpcOpts)
	require.NoError(tb, err)
	defer testhelper.MustClose(tb, conn)

	healthClient := healthpb.NewHealthClient(conn)

	resp, err := healthClient.Check(ctx, &healthpb.HealthCheckRequest{}, grpc.WaitForReady(true))
	require.NoError(tb, err)
	require.Equal(tb, healthpb.HealthCheckResponse_SERVING, resp.Status, "server not yet ready to serve")
}

func runGitaly(tb testing.TB, cfg config.Cfg, registrar func(srv *grpc.Server, deps *service.Dependencies), opts ...GitalyServerOpt) (*grpc.Server, string, bool) {
	tb.Helper()

	ctx := testhelper.Context(tb)

	var gsd gitalyServerDeps
	for _, opt := range opts {
		gsd = opt(gsd)
	}

	// We set up the structerr interceptors so that any error metadata that gets set via
	// `structerr.WithMetadata()` is not only logged, but also present in the error details.
	serverOpts := []server.Option{
		server.WithUnaryInterceptor(StructErrUnaryInterceptor),
		server.WithStreamInterceptor(StructErrStreamInterceptor),
	}

	deps := gsd.createDependencies(tb, ctx, cfg)
	tb.Cleanup(func() { gsd.conns.Close() })

	serverFactory := server.NewGitalyServerFactory(
		cfg,
		gsd.logger.WithField("test", tb.Name()),
		deps.GetBackchannelRegistry(),
		deps.GetDiskCache(),
		[]*limithandler.LimiterMiddleware{deps.GetLimitHandler()},
	)

	if cfg.RuntimeDir != "" {
		internalServer, err := serverFactory.CreateInternal(serverOpts...)
		require.NoError(tb, err)
		tb.Cleanup(internalServer.Stop)

		registrar(internalServer, deps)
		registerHealthServerIfNotRegistered(internalServer)

		require.NoError(tb, os.MkdirAll(cfg.InternalSocketDir(), perm.PrivateDir))
		tb.Cleanup(func() { require.NoError(tb, os.RemoveAll(cfg.InternalSocketDir())) })

		internalListener, err := net.Listen("unix", cfg.InternalSocketPath())
		require.NoError(tb, err)
		go func() {
			assert.NoError(tb, internalServer.Serve(internalListener), "failure to serve internal gRPC")
		}()

		waitHealthy(tb, ctx, "unix://"+internalListener.Addr().String(), cfg.Auth.Token)
	}

	secure := cfg.TLS.CertPath != "" && cfg.TLS.KeyPath != ""

	externalServer, err := serverFactory.CreateExternal(secure, serverOpts...)
	require.NoError(tb, err)
	tb.Cleanup(externalServer.Stop)

	registrar(externalServer, deps)
	registerHealthServerIfNotRegistered(externalServer)

	var listener net.Listener
	var addr string
	switch {
	case cfg.TLSListenAddr != "":
		listener, err = net.Listen("tcp", cfg.TLSListenAddr)
		require.NoError(tb, err)
		_, port, err := net.SplitHostPort(listener.Addr().String())
		require.NoError(tb, err)
		addr = "tls://localhost:" + port
	case cfg.ListenAddr != "":
		listener, err = net.Listen("tcp", cfg.ListenAddr)
		require.NoError(tb, err)
		addr = "tcp://" + listener.Addr().String()
	default:
		serverSocketPath := testhelper.GetTemporaryGitalySocketFileName(tb)
		listener, err = net.Listen("unix", serverSocketPath)
		require.NoError(tb, err)
		addr = "unix://" + serverSocketPath
	}

	go func() {
		assert.NoError(tb, externalServer.Serve(listener), "failure to serve external gRPC")
	}()

	waitHealthy(tb, ctx, addr, cfg.Auth.Token)

	return externalServer, addr, gsd.disablePraefect
}

func registerHealthServerIfNotRegistered(srv *grpc.Server) {
	if _, found := srv.GetServiceInfo()["grpc.health.v1.Health"]; !found {
		// we should register health service as it is used for the health checks
		// praefect service executes periodically (and on the bootstrap step)
		healthpb.RegisterHealthServer(srv, health.NewServer())
	}
}

type gitalyServerDeps struct {
	disablePraefect     bool
	logger              *logrus.Logger
	conns               *client.Pool
	locator             storage.Locator
	txMgr               transaction.Manager
	hookMgr             hook.Manager
	gitlabClient        gitlab.Client
	gitCmdFactory       git.CommandFactory
	backchannelReg      *backchannel.Registry
	catfileCache        catfile.Cache
	diskCache           cache.Cache
	packObjectsCache    streamcache.Cache
	packObjectsLimiter  limiter.Limiter
	limitHandler        *limithandler.LimiterMiddleware
	repositoryCounter   *counter.RepositoryCounter
	git2goExecutor      *git2go.Executor
	updaterWithHooks    *updateref.UpdaterWithHooks
	housekeepingManager housekeeping.Manager
	backupSink          backup.Sink
	backupLocator       backup.Locator
	signingKey          string
}

func (gsd *gitalyServerDeps) createDependencies(tb testing.TB, ctx context.Context, cfg config.Cfg) *service.Dependencies {
	if gsd.logger == nil {
		gsd.logger = testhelper.NewGitalyServerLogger(tb)
	}

	if gsd.conns == nil {
		gsd.conns = client.NewPool(internalclient.UnaryInterceptor(), internalclient.StreamInterceptor())
	}

	if gsd.locator == nil {
		gsd.locator = config.NewLocator(cfg)
	}

	if gsd.gitlabClient == nil {
		gsd.gitlabClient = gitlab.NewMockClient(
			tb, gitlab.MockAllowed, gitlab.MockPreReceive, gitlab.MockPostReceive,
		)
	}

	if gsd.backchannelReg == nil {
		gsd.backchannelReg = backchannel.NewRegistry()
	}

	if gsd.txMgr == nil {
		gsd.txMgr = transaction.NewManager(cfg, gsd.backchannelReg)
	}

	if gsd.gitCmdFactory == nil {
		gsd.gitCmdFactory = gittest.NewCommandFactory(tb, cfg)
	}

	if gsd.hookMgr == nil {
		gsd.hookMgr = hook.NewManager(cfg, gsd.locator, gsd.gitCmdFactory, gsd.txMgr, gsd.gitlabClient)
	}

	if gsd.catfileCache == nil {
		cache := catfile.NewCache(cfg)
		gsd.catfileCache = cache
		tb.Cleanup(cache.Stop)
	}

	if gsd.diskCache == nil {
		gsd.diskCache = cache.New(cfg, gsd.locator)
	}

	if gsd.packObjectsCache == nil {
		gsd.packObjectsCache = streamcache.New(cfg.PackObjectsCache, gsd.logger)
		tb.Cleanup(gsd.packObjectsCache.Stop)
	}

	if gsd.packObjectsLimiter == nil {
		gsd.packObjectsLimiter = limiter.NewConcurrencyLimiter(
			ctx,
			0,
			0,
			nil,
			limiter.NewNoopConcurrencyMonitor(),
		)
	}

	if gsd.limitHandler == nil {
		gsd.limitHandler = limithandler.New(cfg, limithandler.LimitConcurrencyByRepo, limithandler.WithConcurrencyLimiters(ctx))
	}

	if gsd.repositoryCounter == nil {
		gsd.repositoryCounter = counter.NewRepositoryCounter(cfg.Storages)
	}

	if gsd.git2goExecutor == nil {
		gsd.git2goExecutor = git2go.NewExecutor(cfg, gsd.gitCmdFactory, gsd.locator)
	}

	if gsd.updaterWithHooks == nil {
		gsd.updaterWithHooks = updateref.NewUpdaterWithHooks(cfg, gsd.locator, gsd.hookMgr, gsd.gitCmdFactory, gsd.catfileCache)
	}

	if gsd.housekeepingManager == nil {
		gsd.housekeepingManager = housekeeping.NewManager(cfg.Prometheus, gsd.txMgr)
	}

	var partitionManager *storagemgr.PartitionManager
	if testhelper.IsWALEnabled() {
		var err error
		partitionManager, err = storagemgr.NewPartitionManager(
			cfg.Storages,
			gsd.gitCmdFactory,
			gsd.housekeepingManager,
			localrepo.NewFactory(gsd.locator, gsd.gitCmdFactory, gsd.catfileCache),
			gsd.logger,
		)
		require.NoError(tb, err)
		tb.Cleanup(partitionManager.Close)
	}

	if gsd.signingKey != "" {
		cfg.Git.SigningKey = gsd.signingKey
	}

	return &service.Dependencies{
		Cfg:                 cfg,
		ClientPool:          gsd.conns,
		StorageLocator:      gsd.locator,
		TransactionManager:  gsd.txMgr,
		GitalyHookManager:   gsd.hookMgr,
		GitCmdFactory:       gsd.gitCmdFactory,
		BackchannelRegistry: gsd.backchannelReg,
		GitlabClient:        gsd.gitlabClient,
		CatfileCache:        gsd.catfileCache,
		DiskCache:           gsd.diskCache,
		PackObjectsCache:    gsd.packObjectsCache,
		PackObjectsLimiter:  gsd.packObjectsLimiter,
		LimitHandler:        gsd.limitHandler,
		RepositoryCounter:   gsd.repositoryCounter,
		Git2goExecutor:      gsd.git2goExecutor,
		UpdaterWithHooks:    gsd.updaterWithHooks,
		HousekeepingManager: gsd.housekeepingManager,
		PartitionManager:    partitionManager,
		BackupSink:          gsd.backupSink,
		BackupLocator:       gsd.backupLocator,
	}
}

// GitalyServerOpt is a helper type to shorten declarations.
type GitalyServerOpt func(gitalyServerDeps) gitalyServerDeps

// WithLogger sets a logrus.Logger instance that will be used for gitaly services initialisation.
func WithLogger(logger *logrus.Logger) GitalyServerOpt {
	return func(deps gitalyServerDeps) gitalyServerDeps {
		deps.logger = logger
		return deps
	}
}

// WithLocator sets a storage.Locator instance that will be used for gitaly services initialisation.
func WithLocator(locator storage.Locator) GitalyServerOpt {
	return func(deps gitalyServerDeps) gitalyServerDeps {
		deps.locator = locator
		return deps
	}
}

// WithGitCommandFactory sets a git.CommandFactory instance that will be used for gitaly services
// initialisation.
func WithGitCommandFactory(gitCmdFactory git.CommandFactory) GitalyServerOpt {
	return func(deps gitalyServerDeps) gitalyServerDeps {
		deps.gitCmdFactory = gitCmdFactory
		return deps
	}
}

// WithGitLabClient sets gitlab.Client instance that will be used for gitaly services initialisation.
func WithGitLabClient(gitlabClient gitlab.Client) GitalyServerOpt {
	return func(deps gitalyServerDeps) gitalyServerDeps {
		deps.gitlabClient = gitlabClient
		return deps
	}
}

// WithHookManager sets hook.Manager instance that will be used for gitaly services initialisation.
func WithHookManager(hookMgr hook.Manager) GitalyServerOpt {
	return func(deps gitalyServerDeps) gitalyServerDeps {
		deps.hookMgr = hookMgr
		return deps
	}
}

// WithTransactionManager sets transaction.Manager instance that will be used for gitaly services initialisation.
func WithTransactionManager(txMgr transaction.Manager) GitalyServerOpt {
	return func(deps gitalyServerDeps) gitalyServerDeps {
		deps.txMgr = txMgr
		return deps
	}
}

// WithDisablePraefect disables setup and usage of the praefect as a proxy before the gitaly service.
func WithDisablePraefect() GitalyServerOpt {
	return func(deps gitalyServerDeps) gitalyServerDeps {
		deps.disablePraefect = true
		return deps
	}
}

// WithBackchannelRegistry sets backchannel.Registry instance that will be used for gitaly services initialisation.
func WithBackchannelRegistry(backchannelReg *backchannel.Registry) GitalyServerOpt {
	return func(deps gitalyServerDeps) gitalyServerDeps {
		deps.backchannelReg = backchannelReg
		return deps
	}
}

// WithDiskCache sets the cache.Cache instance that will be used for gitaly services initialisation.
func WithDiskCache(diskCache cache.Cache) GitalyServerOpt {
	return func(deps gitalyServerDeps) gitalyServerDeps {
		deps.diskCache = diskCache
		return deps
	}
}

// WithRepositoryCounter sets the counter.RepositoryCounter instance that will be used for gitaly services initialisation.
func WithRepositoryCounter(repositoryCounter *counter.RepositoryCounter) GitalyServerOpt {
	return func(deps gitalyServerDeps) gitalyServerDeps {
		deps.repositoryCounter = repositoryCounter
		return deps
	}
}

// WithPackObjectsLimiter sets the PackObjectsLimiter that will be
// used for gitaly services initialization.
func WithPackObjectsLimiter(limiter *limiter.ConcurrencyLimiter) GitalyServerOpt {
	return func(deps gitalyServerDeps) gitalyServerDeps {
		deps.packObjectsLimiter = limiter
		return deps
	}
}

// WithHousekeepingManager sets the housekeeping.Manager that will be used for Gitaly services
// initialization.
func WithHousekeepingManager(manager housekeeping.Manager) GitalyServerOpt {
	return func(deps gitalyServerDeps) gitalyServerDeps {
		deps.housekeepingManager = manager
		return deps
	}
}

// WithBackupSink sets the backup.Sink that will be used for Gitaly services
func WithBackupSink(backupSink backup.Sink) GitalyServerOpt {
	return func(deps gitalyServerDeps) gitalyServerDeps {
		deps.backupSink = backupSink
		return deps
	}
}

// WithBackupLocator sets the backup.Locator that will be used for Gitaly services
func WithBackupLocator(backupLocator backup.Locator) GitalyServerOpt {
	return func(deps gitalyServerDeps) gitalyServerDeps {
		deps.backupLocator = backupLocator
		return deps
	}
}

// WithSigningKey sets the signing key path that will be used for Gitaly
// services.
func WithSigningKey(signingKey string) GitalyServerOpt {
	return func(deps gitalyServerDeps) gitalyServerDeps {
		deps.signingKey = signingKey
		return deps
	}
}
