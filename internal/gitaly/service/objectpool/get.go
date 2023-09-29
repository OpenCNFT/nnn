package objectpool

import (
	"context"

	"gitlab.com/gitlab-org/gitaly/v16/internal/git/objectpool"
	"gitlab.com/gitlab-org/gitaly/v16/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
)

func (s *server) GetObjectPool(ctx context.Context, in *gitalypb.GetObjectPoolRequest) (*gitalypb.GetObjectPoolResponse, error) {
	repository := in.GetRepository()
	if err := s.locator.ValidateRepository(repository); err != nil {
		return nil, structerr.NewInvalidArgument("%w", err)
	}

	repo := s.localrepo(repository)

	objectPool, err := objectpool.FromRepo(s.locator, s.gitCmdFactory, s.catfileCache, s.txManager, s.housekeepingManager, repo)
	if err != nil {
		s.logger.
			WithError(err).
			WithField("storage", repository.GetStorageName()).
			WithField("relative_path", repository.GetRelativePath()).
			WarnContext(ctx, "alternates file does not point to valid git repository")
	}

	if objectPool == nil {
		return &gitalypb.GetObjectPoolResponse{}, nil
	}

	return &gitalypb.GetObjectPoolResponse{
		ObjectPool: objectPool.ToProto(),
	}, nil
}
