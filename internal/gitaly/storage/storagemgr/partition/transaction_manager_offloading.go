package partition

import (
	"context"
	"fmt"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/housekeeping"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/mode"
	"gitlab.com/gitlab-org/gitaly/v16/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v16/internal/tracing"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
	"os"
	"path/filepath"
	"runtime/trace"
)

const (
	packFileDir    = "pack"
	objectsDir     = "objects"
	infoDir        = "info"
	alternatesFile = "alternates"
	configFile     = "config"
)

// prepareOffloading runs offloading when transaction commits
func (mgr *TransactionManager) prepareOffloading(ctx context.Context, transaction *Transaction) error {
	defer trace.StartRegion(ctx, "prepareOffloading").End()

	span, ctx := tracing.StartSpanIfHasParent(ctx, "transaction.prepareOffloading", nil)
	defer span.Finish()

	finishTimer := mgr.metrics.housekeeping.ReportTaskLatency("total", "prepare")
	defer finishTimer()

	if transaction.offloadRepository == nil {
		return nil
	}

	workingRepository := mgr.repositoryFactory.Build(transaction.snapshot.RelativePath(transaction.relativePath))
	repoPath := mgr.getAbsolutePath(workingRepository.GetRelativePath())
	mgr.logger.Info(repoPath)
	mgr.logger.Info(mgr.storagePath)

	mgr.logger.Info("transaction.prepareOffloading repacking")
	cfg := transaction.offloadRepository.config
	filterToBase, _ := os.MkdirTemp(repoPath, "gitaly-offloading-*")
	filterTo := filepath.Join(filterToBase, objectsDir, packFileDir)
	os.MkdirAll(filterTo, mode.Directory)
	cfg.FilterToDir = filterTo

	repackCfg, repackOptions := housekeeping.GetOffloadingRepackOptions(cfg.Filter, cfg.FilterToDir)
	if err := housekeeping.PerformRepack(ctx, workingRepository, repackCfg, repackOptions...); err != nil {
		return structerr.NewInternal("repacking: %w", err)
	}

	packFileToUpload, err := mgr.collectPackfiles(ctx, filterToBase)
	if err != nil {
		return fmt.Errorf("collecting newPackFileToStay: %w", err)
	}
	for file := range packFileToUpload {
		transaction.offloadRepository.uploadPackFiles = append(transaction.offloadRepository.uploadPackFiles, file)
	}

	if err := housekeeping.UploadToBucket(ctx, cfg.Bucket, cfg.Prefix, cfg.FilterToDir); err != nil {
		return structerr.NewInternal("uploading: %w", err)
	}

	newPackFileToStay, err := mgr.collectPackfiles(ctx, repoPath)
	if err != nil {
		return fmt.Errorf("collecting newPackFileToStay: %w", err)
	}

	// Put new packfiles in WAL file dir
	for file := range newPackFileToStay {
		transaction.offloadRepository.newPackFiles = append(transaction.offloadRepository.newPackFiles, file)
		if err := os.Link(
			filepath.Join(filepath.Join(repoPath, objectsDir, packFileDir), file),
			filepath.Join(transaction.walFilesPath(), file),
		); err != nil {
			return fmt.Errorf("copying packfiles to WAL directory: %w", err)
		}
	}

	promisorRemoteUrl := fmt.Sprintf("gs://%s/%s", cfg.Bucket, cfg.Prefix)

	if err := housekeeping.SetOffloadingGitConfig(ctx, workingRepository, nil, promisorRemoteUrl, cfg.Filter); err != nil {
		return err
	}

	originalRepoPath := cfg.Prefix
	if err := housekeeping.AddOffloadTransientAlternate(ctx, workingRepository, mgr.storagePath, originalRepoPath); err != nil {
		return err
	}

	// // Put new git config file in WAL file dir
	if err := os.Link(
		filepath.Join(filepath.Join(repoPath, configFile)),
		filepath.Join(transaction.walFilesPath(), configFile),
	); err != nil {
		return fmt.Errorf("copying config to WAL directory: %w", err)
	}

	if err := os.Link(
		filepath.Join(filepath.Join(repoPath, objectsDir, infoDir, alternatesFile)),
		filepath.Join(transaction.walFilesPath(), alternatesFile),
	); err != nil {
		return fmt.Errorf("copying config to WAL directory: %w", err)
	}

	return nil
}

func (mgr *TransactionManager) verifyOffloading(ctx context.Context, transaction *Transaction) (*gitalypb.LogEntry_RepositoryOffloading, error) {

	if transaction.offloadRepository == nil {
		return nil, nil
	}

	span, ctx := tracing.StartSpanIfHasParent(ctx, "transaction.verifyOffloading", nil)
	defer span.Finish()

	finishTimer := mgr.metrics.housekeeping.ReportTaskLatency("offload", "verify")
	defer finishTimer()

	// abort when any previous commit has written action
	hasCommittedWriteTransaction := false // TODO this is need investigation

	entryRes := &gitalypb.LogEntry_RepositoryOffloading{
		NewPackFiles: transaction.offloadRepository.newPackFiles,
	}

	if err := mgr.walkCommittedEntries(transaction, func(entry *gitalypb.LogEntry, txnObjectDependencies map[git.ObjectID]struct{}) error {

		// In fact, not all repacking jobs conflicts with offloading.
		// Offloading only conflicts with RepackObjectsStrategyFullWithCruft
		// when cruft blobs contains what we need to offload.
		// However, we will need to calculate and record cruft blobs in the log entry's object dependency.
		// We don't have that logic yet. So as POC, we will just let offloading conflicts with all repacking
		// jobs.
		if entry.GetHousekeeping() != nil && entry.GetHousekeeping().GetRepack() != nil {
			return errOffloadingConflictHousekeeping
		}

		if entry.GetRepositoryDeletion() != nil {
			return errConflictRepositoryDeletion
		}

		if len(txnObjectDependencies) > 0 {
			hasCommittedWriteTransaction = true
		}

		return nil
	}); err != nil {
		return entryRes, fmt.Errorf("walking committed entries: %w", err)
	}
	if hasCommittedWriteTransaction {
		return entryRes, fmt.Errorf("confilict on previous write")
	}

	// TODO In case of rollback
	// we just need to  delete uploaded objects, new packs and config are in snapshot
	return &gitalypb.LogEntry_RepositoryOffloading{
		NewPackFiles: transaction.offloadRepository.newPackFiles,
	}, nil

}

func (mgr *TransactionManager) applyOffloading(ctx context.Context, lsn storage.LSN, logEntry *gitalypb.LogEntry) error {

	if logEntry.GetRepositoryOffloading() == nil {
		return nil
	}

	span, _ := tracing.StartSpanIfHasParent(ctx, "transaction.applyRepacking", nil)
	defer span.Finish()

	finishTimer := mgr.metrics.housekeeping.ReportTaskLatency("repack", "apply")
	defer finishTimer()

	offloading := logEntry.GetRepositoryOffloading()
	repoPath := mgr.getAbsolutePath(logEntry.GetRelativePath())

	// the old pack folder should be already cleanup during applyOperations
	if _, err := os.Stat(filepath.Join(repoPath, objectsDir, packFileDir)); os.IsNotExist(err) {
		if err := os.Mkdir(filepath.Join(repoPath, objectsDir, packFileDir), os.ModePerm); err != nil {
			return fmt.Errorf("create pack dir: %w", err)
		}
	} else {
		return fmt.Errorf("pack dir not removed: %w", err)
	}
	if err := mgr.replacePackfiles(ctx, repoPath, walFilesPathForLSN(mgr.stateDirectory, lsn), offloading.GetNewPackFiles(), []string{}); err != nil {
		return fmt.Errorf("applying packfiles into destination repository: %w", err)
	}

	// overwrite config file
	if err := os.Rename(filepath.Join(walFilesPathForLSN(mgr.stateDirectory, lsn), configFile),
		filepath.Join(repoPath, configFile)); err != nil {
		return fmt.Errorf("replacing config file")
	}

	// copy alternates file
	if err := os.Rename(filepath.Join(walFilesPathForLSN(mgr.stateDirectory, lsn), alternatesFile),
		filepath.Join(repoPath, objectsDir, infoDir, alternatesFile)); err != nil {
		return fmt.Errorf("replacing config file")
	}

	return nil
}
