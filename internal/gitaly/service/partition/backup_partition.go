package partition

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"gitlab.com/gitlab-org/gitaly/v16/internal/archive"
	"gitlab.com/gitlab-org/gitaly/v16/internal/backup"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
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
	if tx := storage.ExtractTransaction(ctx); tx != nil {
		root = tx.Root()
		lsn = tx.SnapshotLSN().String()
	}
	if root == "" {
		return nil, structerr.NewInternal("backup partition: transaction not initialized")
	}

	backupRelativePath := filepath.Join("partition-backups", in.GetStorageName(), in.GetPartitionId(), lsn+".tar")

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

	manifestRelativePath := filepath.Join("partition-manifests", in.GetStorageName(), in.GetPartitionId()+".json")
	if err := s.updateManifest(ctx, manifestRelativePath, backupRelativePath); err != nil {
		return nil, fmt.Errorf("update manifest: %w", err)
	}

	return &gitalypb.BackupPartitionResponse{}, nil
}

// BackupEntry represents a single backup in the manifest
type BackupEntry struct {
	// Timestamp is the time when the backup was created.
	Timestamp time.Time `json:"timestamp"`
	// Path is the relative path to the backup in the backup bucket.
	Path string `json:"path"`
}

// updateManifest updates the backup manifest file for specific partition.
// Since goblob doesn't support in place updates, we need to stream the existing
// content into an temp file and then upload the temp file to the manifest path.
func (s *server) updateManifest(ctx context.Context, manifestRelativePath, backupRelativePath string) (returnErr error) {
	// Create a temporary file locally.
	tempFile, err := os.CreateTemp("", "manifest-*.json")
	if err != nil {
		return fmt.Errorf("create temp file: %w", err)
	}
	defer func() {
		if err = tempFile.Close(); err != nil {
			returnErr = errors.Join(returnErr, fmt.Errorf("close temp file: %w", err))
		}
		if err = os.Remove(tempFile.Name()); err != nil {
			returnErr = errors.Join(returnErr, fmt.Errorf("remove temp file: %w", err))
		}
	}()

	// Add the new entry first to create reverse chronological order
	// so that when we restore, the newest backups are read first.
	if err := json.NewEncoder(tempFile).Encode(BackupEntry{
		Timestamp: time.Now(),
		Path:      backupRelativePath,
	}); err != nil {
		return fmt.Errorf("encode new entry: %w", err)
	}

	// Copy existing entries after the new entry.
	r, err := s.backupSink.GetReader(ctx, manifestRelativePath)
	if err != nil && !errors.Is(err, backup.ErrDoesntExist) {
		return fmt.Errorf("get reader: %w", err)
	}
	if r != nil {
		defer func() {
			if err := r.Close(); err != nil {
				returnErr = errors.Join(returnErr, fmt.Errorf("close reader %w", err))
			}
		}()

		if _, err := tempFile.ReadFrom(r); err != nil {
			return fmt.Errorf("read existing manifest: %w", err)
		}
	}

	// Rewind the temp file to the beginning before reading from it.
	if _, err := tempFile.Seek(0, 0); err != nil {
		return fmt.Errorf("seek temp file: %w", err)
	}

	// Upload the temp file to the sink.
	w, err := s.backupSink.GetWriter(ctx, manifestRelativePath)
	if err != nil {
		return fmt.Errorf("get writer: %w", err)
	}
	defer func() {
		if err := w.Close(); err != nil {
			returnErr = errors.Join(returnErr, fmt.Errorf("close writer %w", err))
		}
	}()

	if _, err := tempFile.WriteTo(w); err != nil {
		return fmt.Errorf("write manifest: %w", err)
	}

	return nil
}
