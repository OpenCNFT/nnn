package operations

import (
	"context"

	"gitlab.com/gitlab-org/gitaly/v16/internal/git/catfile"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/gitcmd"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/quarantine"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/hook"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/hook/updateref"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/service"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/transaction"
	"gitlab.com/gitlab-org/gitaly/v16/internal/grpc/client"
	"gitlab.com/gitlab-org/gitaly/v16/internal/log"
	"gitlab.com/gitlab-org/gitaly/v16/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
)

//nolint:revive // This is unintentionally missing documentation.
type Server struct {
	gitalypb.UnimplementedOperationServiceServer
	logger        log.Logger
	hookManager   hook.Manager
	txManager     transaction.Manager
	locator       storage.Locator
	conns         *client.Pool
	gitCmdFactory gitcmd.CommandFactory
	catfileCache  catfile.Cache
	updater       *updateref.UpdaterWithHooks
	gitConfig     config.Git
}

// NewServer creates a new instance of a grpc OperationServiceServer
func NewServer(deps *service.Dependencies) *Server {
	return &Server{
		logger:        deps.GetLogger(),
		hookManager:   deps.GetHookManager(),
		txManager:     deps.GetTxManager(),
		locator:       deps.GetLocator(),
		conns:         deps.GetConnsPool(),
		gitCmdFactory: deps.GetGitCmdFactory(),
		catfileCache:  deps.GetCatfileCache(),
		updater:       deps.GetUpdaterWithHooks(),
		gitConfig:     deps.GetCfg().Git,
	}
}

func (s *Server) localrepo(repo storage.Repository) *localrepo.Repo {
	return localrepo.New(s.logger, s.locator, s.gitCmdFactory, s.catfileCache, repo)
}

func (s *Server) quarantinedRepo(
	ctx context.Context, repo *gitalypb.Repository,
) (*quarantine.Dir, *localrepo.Repo, error) {
	quarantineDir, err := quarantine.New(ctx, repo, s.logger, s.locator)
	if err != nil {
		return nil, nil, structerr.NewInternal("creating object quarantine: %w", err)
	}

	quarantineRepo := s.localrepo(quarantineDir.QuarantinedRepo())

	return quarantineDir, quarantineRepo, nil
}
