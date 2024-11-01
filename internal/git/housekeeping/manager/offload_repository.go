package manager

import (
	"context"
	"fmt"

	"gitlab.com/gitlab-org/gitaly/v16/internal/git/housekeeping"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/housekeeping/config"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v16/internal/structerr"
)

// OffloadRepository is housekeeping manager's entry point of offloading,
// where we determine if it is in the context of transaction.
func (m *RepositoryManager) OffloadRepository(ctx context.Context, repo *localrepo.Repo, cfg config.OffloadHouseKeepingConfig) error {
	m.logger.Info("Offloading in RepositoryManager")

	if err := m.maybeStartTransaction(ctx, repo, func(ctx context.Context, tx storage.Transaction, repo *localrepo.Repo) error {
		if tx != nil {
			return m.offloadWithTransaction(ctx, repo, cfg)
		}
		return m.offload(ctx, repo, cfg)
	}); err != nil {
		return err
	}

	return nil
}

// offload is the offloading logic without transaction
func (m *RepositoryManager) offload(ctx context.Context, repo *localrepo.Repo, cfg config.OffloadHouseKeepingConfig) error {
	repackCfg, repackOptions := housekeeping.GetOffloadingRepackOptions("", "")

	if err := housekeeping.PerformRepack(ctx, repo, repackCfg, repackOptions...); err != nil {
		return structerr.NewInternal("repacking: %w", err)
	}
	if err := housekeeping.UploadToOffloadingStorage(ctx, "", "", ""); err != nil {
		return structerr.NewInternal("uploading: %w", err)
	}

	if err := housekeeping.SetOffloadingGitConfig(ctx, repo); err != nil {
		return structerr.NewInternal("set offloading git config: %w", err)
	}

	if err := housekeeping.AddOffloadTransientAlternate(ctx, repo); err != nil {
		return structerr.NewInternal("set offloading transient alternate: %w", err)
	}
	return nil
}

// offloadWithTransaction is the offloading logic with transaction
func (m *RepositoryManager) offloadWithTransaction(ctx context.Context, repo *localrepo.Repo, cfg config.OffloadHouseKeepingConfig) error {
	if err := m.runInTransaction(ctx, false, repo, func(ctx context.Context, tx storage.Transaction, repo *localrepo.Repo) error {
		return nil
	}); err != nil {
		return fmt.Errorf("run offloading in transaction: %w", err)
	}
	return nil
}

// RehydrateRepository is housekeeping manager's entry point of rehydrating (i.e. revert offloading),
// where we determine if it is in the context of transaction.
func (m *RepositoryManager) RehydrateRepository(ctx context.Context, repo *localrepo.Repo, cfg config.OffloadHouseKeepingConfig) error {
	m.logger.Info("Offloading in RepositoryManager")

	if err := m.maybeStartTransaction(ctx, repo, func(ctx context.Context, tx storage.Transaction, repo *localrepo.Repo) error {
		if tx != nil {
			return m.rehydrateWithTransaction(ctx, repo, cfg)
		}
		return m.rehydrate(ctx, repo, cfg)
	}); err != nil {
		return err
	}

	return nil
}

// rehydrate is the rehydrating logic without transaction
func (m *RepositoryManager) rehydrate(ctx context.Context, repo *localrepo.Repo, cfg config.OffloadHouseKeepingConfig) error {
	if err := housekeeping.RemoveOffloadTransientAlternate(ctx, repo); err != nil {
		return structerr.NewInternal("remove offload alternate: %w", err)
	}
	if err := housekeeping.ResetOffloadingGitConfig(ctx, repo); err != nil {
		return structerr.NewInternal("remove offload from git config: %w", err)
	}

	if err := housekeeping.DownloadOffloadingStorage(ctx, ""); err != nil {
		return structerr.NewInternal("downloading objects: %w", err)
	}

	if err := housekeeping.RemoveFromOffloadingStorage(); err != nil {
		return structerr.NewInternal("remove object in object storage: %w", err)
	}
	return nil
}

// rehydrateWithTransaction is the rehydrating logic with transaction
func (m *RepositoryManager) rehydrateWithTransaction(ctx context.Context, repo *localrepo.Repo, cfg config.OffloadHouseKeepingConfig) error {
	if err := m.runInTransaction(ctx, false, repo, func(ctx context.Context, tx storage.Transaction, repo *localrepo.Repo) error {
		return nil
	}); err != nil {
		return fmt.Errorf("run offloading in transaction: %w", err)
	}
	return nil
}
