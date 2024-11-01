package manager

import (
	"context"
	"fmt"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/housekeeping"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/housekeeping/config"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v16/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
)

// OffloadRepository is housekeeping manager's entry point of offloading,
// where we judge if it is in the context of transaction
func (m *RepositoryManager) OffloadRepository(ctx context.Context, repo *localrepo.Repo, cfg config.OffloadingConfig) error {
	m.logger.Info("Offloading in RepositoryManager")

	if err := m.maybeStartTransaction(ctx, repo, func(ctx context.Context, tx storage.Transaction, repo *localrepo.Repo) error {

		originalRepo := &gitalypb.Repository{
			StorageName:  repo.GetStorageName(),
			RelativePath: repo.GetRelativePath(),
		}

		if tx != nil {
			originalRepo = tx.OriginalRepository(originalRepo)

			// Use original repo's relative path as prefix when in
			// transaction, otherwise snapshot will be used
			cfg.Prefix = originalRepo.GetRelativePath()

			return m.offloadWithTransaction(ctx, repo, cfg)
		}

		return m.offload(ctx, repo, cfg)
	}); err != nil {
		return err
	}

	return nil
}

// offload is a normal offloading logic without transaction
func (m *RepositoryManager) offload(ctx context.Context, repo *localrepo.Repo, cfg config.OffloadingConfig) error {

	repackCfg, repackOptions := housekeeping.GetOffloadingRepackOptions(cfg.Filter, cfg.FilterToDir)

	if err := housekeeping.PerformRepack(ctx, repo, repackCfg, repackOptions...); err != nil {
		return structerr.NewInternal("repacking: %w", err)
	}

	if err := housekeeping.UploadToBucket(ctx, cfg.Bucket, cfg.Prefix, cfg.FilterToDir); err != nil {
		return structerr.NewInternal("uploading: %w", err)
	}

	if err := housekeeping.Cleanup(cfg.FilterToDir); err != nil {
		return structerr.NewInternal("remove: %w", err)
	}

	// add config to git repo
	promisorRemoteUrl := fmt.Sprintf("gs://%s/%s", cfg.Bucket, cfg.Prefix)

	if err := housekeeping.SetOffloadingGitConfig(ctx, repo, m.txManager, promisorRemoteUrl, cfg.Filter); err != nil {
		return err
	}
	return nil
}

// offloadWithTransaction is the offloading logic with transaction
func (m *RepositoryManager) offloadWithTransaction(ctx context.Context, repo *localrepo.Repo, cfg config.OffloadingConfig) error {
	if err := m.runInTransaction(ctx, false, repo, func(ctx context.Context, tx storage.Transaction, repo *localrepo.Repo) error {

		// Set offloading config, it will run when the transaction commit
		tx.OffloadRepository(cfg)

		return nil
	}); err != nil {
		return fmt.Errorf("run object repacking: %w", err)
	}
	return nil
}
