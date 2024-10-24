package partition

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"time"

	"gitlab.com/gitlab-org/gitaly/v16/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
)

// CreatePartitionBackupManifest creates the manifest for locating backups.
func (s *server) CreatePartitionBackupManifest(ctx context.Context, in *gitalypb.CreatePartitionBackupManifestRequest) (_ *gitalypb.CreatePartitionBackupManifestResponse, returnErr error) {
	if s.backupSink == nil {
		return nil, structerr.NewFailedPrecondition("create partition backup manifest: server-side backups are not configured")
	}

	manifestRelativePath := filepath.Join("partition-manifests", in.GetStorageName(), in.GetTimestamp().AsTime().String()+".json")

	exists, err := s.backupSink.Exists(ctx, manifestRelativePath)
	if err != nil || exists {
		return nil, fmt.Errorf("manifest exists: %w", err)
	}

	// Create a new context to abort the write on failure.
	writeCtx, cancelWrite := context.WithCancel(ctx)
	defer cancelWrite()

	w, err := s.backupSink.GetWriter(writeCtx, manifestRelativePath)
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
			returnErr = fmt.Errorf("close manifest writer: %w", err)
		}
	}()

	encoder := json.NewEncoder(w)

	// Write the global timestamp entry
	if err := encoder.Encode(struct {
		Timestamp time.Time `json:"timestamp"`
	}{
		Timestamp: in.GetTimestamp().AsTime(),
	}); err != nil {
		return nil, fmt.Errorf("encode manifest timestamp: %w", err)
	}

	// Process each partition backup
	for _, partition := range in.GetPartitions() {
		entry := ManifestEntry{
			PartitionID:        partition.GetPartitionId(),
			BackupRelativePath: partition.GetBackupRelativePath(),
			Timestamp:          partition.GetTimestamp().AsTime(),
		}

		// Encode and write the entry
		if err := encoder.Encode(entry); err != nil {
			return nil, fmt.Errorf("encode manifest entry: %w", err)
		}
	}

	return &gitalypb.CreatePartitionBackupManifestResponse{}, nil
}

// ManifestEntry is the representation of the backup manifest entry.
type ManifestEntry struct {
	PartitionID        string    `json:"partition_id"`
	BackupRelativePath string    `json:"backup_relative_path"`
	Timestamp          time.Time `json:"timestamp"`
}
