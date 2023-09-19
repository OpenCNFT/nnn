package diff

import (
	"gitlab.com/gitlab-org/gitaly/v16/internal/git"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/catfile"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/service"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
)

const msgSizeThreshold = 5 * 1024

type server struct {
	gitalypb.UnimplementedDiffServiceServer
	MsgSizeThreshold int
	locator          storage.Locator
	gitCmdFactory    git.CommandFactory
	catfileCache     catfile.Cache
}

// NewServer creates a new instance of a gRPC DiffServer
func NewServer(deps *service.Dependencies) gitalypb.DiffServiceServer {
	return &server{
		MsgSizeThreshold: msgSizeThreshold,
		locator:          deps.GetLocator(),
		gitCmdFactory:    deps.GetGitCmdFactory(),
		catfileCache:     deps.GetCatfileCache(),
	}
}

func (s *server) localrepo(repo storage.Repository) *localrepo.Repo {
	return localrepo.New(s.locator, s.gitCmdFactory, s.catfileCache, repo)
}
