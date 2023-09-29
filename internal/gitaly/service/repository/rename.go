package repository

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v16/internal/helper/perm"
	"gitlab.com/gitlab-org/gitaly/v16/internal/safe"
	"gitlab.com/gitlab-org/gitaly/v16/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
)

func (s *server) RenameRepository(ctx context.Context, in *gitalypb.RenameRepositoryRequest) (*gitalypb.RenameRepositoryResponse, error) {
	if err := validateRenameRepositoryRequest(s.locator, in); err != nil {
		return nil, structerr.NewInvalidArgument("%w", err)
	}

	targetRepo := &gitalypb.Repository{
		StorageName:  in.GetRepository().GetStorageName(),
		RelativePath: in.GetRelativePath(),
	}

	if err := s.renameRepository(ctx, in.GetRepository(), targetRepo); err != nil {
		return nil, structerr.NewInternal("%w", err)
	}

	return &gitalypb.RenameRepositoryResponse{}, nil
}

func (s *server) renameRepository(ctx context.Context, sourceRepo, targetRepo *gitalypb.Repository) error {
	sourcePath, err := s.locator.GetRepoPath(sourceRepo)
	if err != nil {
		return structerr.NewInvalidArgument("%w", err)
	}

	targetPath, err := s.locator.GetRepoPath(targetRepo, storage.WithRepositoryVerificationSkipped())
	if err != nil {
		return structerr.NewInvalidArgument("%w", err)
	}

	// Check up front whether the target path exists already. If it does, we can avoid going
	// into the critical section altogether.
	if _, err := os.Stat(targetPath); !os.IsNotExist(err) {
		return structerr.NewAlreadyExists("target repo exists already")
	}

	if err := os.MkdirAll(filepath.Dir(targetPath), perm.GroupPrivateDir); err != nil {
		return fmt.Errorf("create target parent dir: %w", err)
	}

	// We're locking both the source repository path and the target repository path for
	// concurrent modification. This is so that the source repo doesn't get moved somewhere else
	// meanwhile, and so that the target repo doesn't get created concurrently either.
	sourceLocker, err := safe.NewLockingFileWriter(sourcePath)
	if err != nil {
		return fmt.Errorf("creating source repo locker: %w", err)
	}
	defer func() {
		if err := sourceLocker.Close(); err != nil {
			s.logger.WithError(err).ErrorContext(ctx, "closing source repo locker failed")
		}
	}()

	targetLocker, err := safe.NewLockingFileWriter(targetPath)
	if err != nil {
		return fmt.Errorf("creating target repo locker: %w", err)
	}
	defer func() {
		if err := targetLocker.Close(); err != nil {
			s.logger.WithError(err).ErrorContext(ctx, "closing target repo locker failed")
		}
	}()

	// We're now entering the critical section where both the source and target path are locked.
	if err := sourceLocker.Lock(); err != nil {
		return fmt.Errorf("locking source repo: %w", err)
	}
	if err := targetLocker.Lock(); err != nil {
		return fmt.Errorf("locking target repo: %w", err)
	}

	// We need to re-check whether the target path exists in case somebody has removed it before
	// we have taken the lock.
	if _, err := os.Stat(targetPath); !os.IsNotExist(err) {
		return structerr.NewAlreadyExists("target repo exists already")
	}

	if err := os.Rename(sourcePath, targetPath); err != nil {
		return fmt.Errorf("moving repository into place: %w", err)
	}

	storagePath, err := s.locator.GetStorageByName(targetRepo.GetStorageName())
	if err != nil {
		return fmt.Errorf("get storage by name: %w", err)
	}

	syncer := safe.NewSyncer()
	if err := syncer.SyncHierarchy(storagePath, targetRepo.GetRelativePath()); err != nil {
		return fmt.Errorf("sync hierarchy: %w", err)
	}

	if err := syncer.SyncParent(sourcePath); err != nil {
		return fmt.Errorf("sync parent: %w", err)
	}

	return nil
}

func validateRenameRepositoryRequest(locator storage.Locator, in *gitalypb.RenameRepositoryRequest) error {
	if err := locator.ValidateRepository(in.GetRepository()); err != nil {
		return structerr.NewInvalidArgument("%w", err)
	}

	if in.GetRelativePath() == "" {
		return errors.New("destination relative path is empty")
	}

	return nil
}
