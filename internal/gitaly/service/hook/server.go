package hook

import (
	"context"
	"io"

	"gitlab.com/gitlab-org/gitaly/v16/internal/git"
	gitalyhook "gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/hook"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/service"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v16/internal/limiter"
	"gitlab.com/gitlab-org/gitaly/v16/internal/log"
	"gitlab.com/gitlab-org/gitaly/v16/internal/streamcache"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
)

type server struct {
	gitalypb.UnimplementedHookServiceServer
	logger             log.Logger
	manager            gitalyhook.Manager
	locator            storage.Locator
	gitCmdFactory      git.CommandFactory
	packObjectsCache   streamcache.Cache
	packObjectsLimiter limiter.Limiter
	runPackObjectsFn   func(
		context.Context,
		git.CommandFactory,
		io.Writer,
		*gitalypb.PackObjectsHookWithSidechannelRequest,
		*packObjectsArgs,
		io.Reader,
		string,
	) error
}

// NewServer creates a new instance of a gRPC namespace server
func NewServer(deps *service.Dependencies) gitalypb.HookServiceServer {
	srv := &server{
		logger:             deps.GetLogger(),
		manager:            deps.GetHookManager(),
		locator:            deps.GetLocator(),
		gitCmdFactory:      deps.GetGitCmdFactory(),
		packObjectsCache:   deps.GetPackObjectsCache(),
		packObjectsLimiter: deps.GetPackObjectsLimiter(),
		runPackObjectsFn:   runPackObjects,
	}

	return srv
}
