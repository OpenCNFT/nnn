package repository

import (
	"context"
	"fmt"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/housekeeping/config"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v16/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
	"os"
)

// OffloadRepository if in transaction, don't upload, just remove the needed packfile or objects
// if no in transaction, upload and remove

func (s *server) OffloadRepository(ctx context.Context, in *gitalypb.OffloadRequest) (*gitalypb.OffloadResponse, error) {

	repository := in.GetRepository()
	if err := s.locator.ValidateRepository(ctx, repository, storage.WithSkipRepositoryExistenceCheck()); err != nil {
		return nil, structerr.NewInvalidArgument("%w", err)
	}

	repo := s.localrepo(repository)
	repoPath, err := s.locator.GetRepoPath(ctx, repo)
	if err != nil {
		return nil, structerr.NewInternal("get repo path: %w", err)
	}

	s.logger.Info(fmt.Sprintf("Start offloading %s", repoPath))
	filter := "blob:none"
	if len(in.Filter) != 0 {
		filter = in.Filter
	}

	bucket := "blob_offloads"
	if len(in.Bucket) != 0 {
		bucket = in.Bucket
	}

	bucketPrefix := repo.GetRelativePath()
	s.logger.Info(fmt.Sprintf("Lock %s", repoPath))

	filterTo, err := os.MkdirTemp(repoPath, "gitaly-offloading-*")

	offloadingCfg := config.OffloadingConfig{
		Filter:      filter,
		FilterToDir: filterTo,
		Bucket:      bucket,
		Prefix:      bucketPrefix,
	}

	s.logger.Info(fmt.Sprintf("Start offloading %v", offloadingCfg))

	if err := s.housekeepingManager.OffloadRepository(ctx, repo, offloadingCfg); err != nil {
		return nil, structerr.NewInternal("%w", err)
	}

	return &gitalypb.OffloadResponse{}, nil

}
