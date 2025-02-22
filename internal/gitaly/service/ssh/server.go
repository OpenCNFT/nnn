package ssh

import (
	"github.com/prometheus/client_golang/prometheus"
	"gitlab.com/gitlab-org/gitaly/v16/internal/backup"
	"gitlab.com/gitlab-org/gitaly/v16/internal/bundleuri"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/catfile"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/gitcmd"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/hook"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/hook/updateref"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/service"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/storagemgr"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/transaction"
	"gitlab.com/gitlab-org/gitaly/v16/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v16/internal/log"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
)

type server struct {
	gitalypb.UnimplementedSSHServiceServer
	logger                                   log.Logger
	cfg                                      config.Cfg
	locator                                  storage.Locator
	gitCmdFactory                            gitcmd.CommandFactory
	catfileCache                             catfile.Cache
	txManager                                transaction.Manager
	txRegistry                               *storagemgr.TransactionRegistry
	hookManager                              hook.Manager
	updater                                  *updateref.UpdaterWithHooks
	uploadPackRequestTimeoutTickerFactory    func() helper.Ticker
	uploadArchiveRequestTimeoutTickerFactory func() helper.Ticker
	packfileNegotiationMetrics               *prometheus.CounterVec
	backupLocator                            backup.Locator
	backupSink                               *backup.Sink
	bundleURISink                            *bundleuri.Sink
}

// NewServer creates a new instance of a grpc SSHServer
func NewServer(deps *service.Dependencies, serverOpts ...ServerOpt) gitalypb.SSHServiceServer {
	s := &server{
		logger:        deps.GetLogger(),
		cfg:           deps.GetCfg(),
		locator:       deps.GetLocator(),
		gitCmdFactory: deps.GetGitCmdFactory(),
		catfileCache:  deps.GetCatfileCache(),
		txManager:     deps.GetTxManager(),
		txRegistry:    deps.GetTransactionRegistry(),
		hookManager:   deps.GetHookManager(),
		updater:       deps.GetUpdaterWithHooks(),
		uploadPackRequestTimeoutTickerFactory: func() helper.Ticker {
			return helper.NewTimerTicker(deps.Cfg.Timeout.UploadPackNegotiation.Duration())
		},
		uploadArchiveRequestTimeoutTickerFactory: func() helper.Ticker {
			return helper.NewTimerTicker(deps.Cfg.Timeout.UploadArchiveNegotiation.Duration())
		},
		packfileNegotiationMetrics: prometheus.NewCounterVec(
			prometheus.CounterOpts{},
			[]string{"git_negotiation_feature"},
		),
		backupLocator: deps.GetBackupLocator(),
		backupSink:    deps.GetBackupSink(),
		bundleURISink: deps.GetBundleURISink(),
	}

	for _, serverOpt := range serverOpts {
		serverOpt(s)
	}

	return s
}

// ServerOpt is a self referential option for server
type ServerOpt func(s *server)

// WithUploadPackRequestTimeoutTickerFactory sets the upload pack request timeout ticker factory.
func WithUploadPackRequestTimeoutTickerFactory(factory func() helper.Ticker) ServerOpt {
	return func(s *server) {
		s.uploadPackRequestTimeoutTickerFactory = factory
	}
}

// WithArchiveRequestTimeoutTickerFactory sets the upload pack request timeout ticker factory.
func WithArchiveRequestTimeoutTickerFactory(factory func() helper.Ticker) ServerOpt {
	return func(s *server) {
		s.uploadArchiveRequestTimeoutTickerFactory = factory
	}
}

//nolint:revive // This is unintentionally missing documentation.
func WithPackfileNegotiationMetrics(c *prometheus.CounterVec) ServerOpt {
	return func(s *server) {
		s.packfileNegotiationMetrics = c
	}
}

func (s *server) localrepo(repo storage.Repository) *localrepo.Repo {
	return localrepo.New(s.logger, s.locator, s.gitCmdFactory, s.catfileCache, repo)
}
