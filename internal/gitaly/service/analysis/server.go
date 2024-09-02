package analysis

import (
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/catfile"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/gitcmd"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/service"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v16/internal/log"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
)

type server struct {
	gitalypb.UnimplementedAnalysisServiceServer
	logger        log.Logger
	locator       storage.Locator
	gitCmdFactory gitcmd.CommandFactory
	catfileCache  catfile.Cache
}

// NewServer creates a new instance of the gRPC AnalysisService.
func NewServer(deps *service.Dependencies) gitalypb.AnalysisServiceServer {
	return &server{
		logger:        deps.GetLogger(),
		locator:       deps.GetLocator(),
		gitCmdFactory: deps.GetGitCmdFactory(),
		catfileCache:  deps.GetCatfileCache(),
	}
}

func (s *server) localrepo(repo storage.Repository) *localrepo.Repo {
	return localrepo.New(s.logger, s.locator, s.gitCmdFactory, s.catfileCache, repo)
}
