package repository

import (
	"context"

	"gitlab.com/gitlab-org/gitaly/v16/client"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/catfile"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/housekeeping"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/quarantine"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/repository"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git2go"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/transaction"
	"gitlab.com/gitlab-org/gitaly/v16/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v16/internal/unarycache"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
)

type server struct {
	gitalypb.UnimplementedRepositoryServiceServer
	conns               *client.Pool
	locator             storage.Locator
	txManager           transaction.Manager
	gitCmdFactory       git.CommandFactory
	cfg                 config.Cfg
	loggingCfg          config.Logging
	catfileCache        catfile.Cache
	git2goExecutor      *git2go.Executor
	housekeepingManager housekeeping.Manager

	licenseCache *unarycache.Cache[git.ObjectID, *gitalypb.FindLicenseResponse]
}

// NewServer creates a new instance of a gRPC repo server
func NewServer(
	cfg config.Cfg,
	locator storage.Locator,
	txManager transaction.Manager,
	gitCmdFactory git.CommandFactory,
	catfileCache catfile.Cache,
	connsPool *client.Pool,
	git2goExecutor *git2go.Executor,
	housekeepingManager housekeeping.Manager,
) gitalypb.RepositoryServiceServer {
	return &server{
		locator:             locator,
		txManager:           txManager,
		gitCmdFactory:       gitCmdFactory,
		conns:               connsPool,
		cfg:                 cfg,
		loggingCfg:          cfg.Logging,
		catfileCache:        catfileCache,
		git2goExecutor:      git2goExecutor,
		housekeepingManager: housekeepingManager,

		licenseCache: newLicenseCache(),
	}
}

func (s *server) localrepo(repo repository.GitRepo) *localrepo.Repo {
	return localrepo.New(s.locator, s.gitCmdFactory, s.catfileCache, repo)
}

func (s *server) quarantinedRepo(
	ctx context.Context, repo *gitalypb.Repository,
) (*quarantine.Dir, *localrepo.Repo, error) {
	quarantineDir, err := quarantine.New(ctx, repo, s.locator)
	if err != nil {
		return nil, nil, structerr.NewInternal("creating object quarantine: %w", err)
	}

	quarantineRepo := s.localrepo(quarantineDir.QuarantinedRepo())

	return quarantineDir, quarantineRepo, nil
}
