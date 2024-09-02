package partition

import (
	"context"
	"fmt"
	"path/filepath"

	"gitlab.com/gitlab-org/gitaly/v16/internal/archive"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/storagectx"
	"gitlab.com/gitlab-org/gitaly/v16/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
)

// BackupPartition creates a backup of entire partition streamed directly to object-storage.
func (s *server) BackupPartition(ctx context.Context, in *gitalypb.BackupPartitionRequest) (_ *gitalypb.BackupPartitionResponse, returnErr error) {
	if s.backupSink == nil {
		return nil, structerr.NewFailedPrecondition("backup partition: server-side backups are not configured")
	}

	var root string
	var lsn string
	if tx := storagectx.ExtractTransaction(ctx); tx != nil {
		root = tx.Root()
		lsn = tx.SnapshotLSN().String()
	}
	if root == "" {
		return nil, structerr.NewInternal("backup partition: transaction not initialized")
	}

	backupRelativePath := filepath.Join(in.StorageName, storage.PartitionID(in.PartitionId).String(), lsn+".tar")

	exists, err := s.backupSink.Exists(ctx, backupRelativePath)
	if err != nil {
		return nil, fmt.Errorf("backup exists: %w", err)
	}
	if exists {
		return nil, structerr.NewAlreadyExists("there is an up-to-date backup for the given partition")
	}

	// Create a new context to abort the write on failure.
	writeCtx, cancelWrite := context.WithCancel(ctx)
	defer cancelWrite()

	w, err := s.backupSink.GetWriter(writeCtx, backupRelativePath)
	if err != nil {
		return nil, fmt.Errorf("get backup writer: %w", err)
	}
	defer func() {
		if returnErr != nil {
			// End the context before calling Close to ensure we don't persist the failed
			// write to object storage.
			cancelWrite()
		}
		if err := w.Close(); err != nil && returnErr == nil {
			returnErr = fmt.Errorf("close backup writer: %w", err)
		}
	}()

	if err := archive.WriteTarball(writeCtx, s.logger, w, root, "."); err != nil {
		return nil, fmt.Errorf("write tarball: %w", err)
	}

	return &gitalypb.BackupPartitionResponse{}, nil
}
