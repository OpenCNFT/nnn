package backup

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v16/internal/grpc/client"
	"gitlab.com/gitlab-org/gitaly/v16/internal/log"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
	"golang.org/x/sync/errgroup"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	defaultConcurrencyLimit = 1
	defaultPaginationLimit  = 100
	defaultBackupTimeout    = 5 * time.Minute
)

// PartitionBackupManager manages process of the creating/restoring partition backups.
type PartitionBackupManager struct {
	pool             *client.Pool
	paginationLimit  int32            // pagination page size
	backupTimeout    time.Duration    // timeout for individual partition backup calls
	concurrencyLimit int              // maximum number of concurrent backup calls
	createClient     CreateClientFunc // function for creating partition client
}

// CreateClientFunc is a function for creating partition client, makes it easier to mock client functions in test.
type CreateClientFunc func(context.Context, storage.ServerInfo) (gitalypb.PartitionServiceClient, error)

// PartitionBackupOption is a functional option for the *PartitionBackupManager.
type PartitionBackupOption func(*PartitionBackupManager)

// WithPartitionCreateClientFunc sets a custom function for creating partition client.
func WithPartitionCreateClientFunc(creatClientFunc CreateClientFunc) PartitionBackupOption {
	return func(pbm *PartitionBackupManager) {
		pbm.createClient = creatClientFunc
	}
}

// WithPartitionConcurrencyLimit sets the maximum number of concurrent backup calls.
func WithPartitionConcurrencyLimit(limit int) PartitionBackupOption {
	return func(pbm *PartitionBackupManager) {
		pbm.concurrencyLimit = limit
	}
}

// WithPartitionPaginationLimit sets the pagination page size.
func WithPartitionPaginationLimit(limit int32) PartitionBackupOption {
	return func(pbm *PartitionBackupManager) {
		pbm.paginationLimit = limit
	}
}

// WithPartitionBackupTimeout sets the timeout for individual partition backup calls.
func WithPartitionBackupTimeout(timeout time.Duration) PartitionBackupOption {
	return func(pbm *PartitionBackupManager) {
		pbm.backupTimeout = timeout
	}
}

// NewPartitionBackupManager creates and returns initialized *PartitionBackupManager instance.
func NewPartitionBackupManager(pool *client.Pool, opts ...PartitionBackupOption) *PartitionBackupManager {
	pbm := &PartitionBackupManager{
		pool:             pool,
		concurrencyLimit: defaultConcurrencyLimit,
		paginationLimit:  defaultPaginationLimit,
		backupTimeout:    defaultBackupTimeout,
		createClient: func(ctx context.Context, server storage.ServerInfo) (gitalypb.PartitionServiceClient, error) {
			conn, err := pool.Dial(ctx, server.Address, server.Token)
			if err != nil {
				return nil, err
			}
			return gitalypb.NewPartitionServiceClient(conn), nil
		},
	}

	for _, opt := range opts {
		opt(pbm)
	}

	return pbm
}

// WithPaginationLimit sets the pagination page size. If not specified, the default page size is 100.
func (pbm *PartitionBackupManager) WithPaginationLimit(limit int32) *PartitionBackupManager {
	pbm.paginationLimit = limit

	return pbm
}

// WithBackupTimeout sets the timeout for individual partition backup calls. If not specified, the default timeout is 5 minutes.
func (pbm *PartitionBackupManager) WithBackupTimeout(timeout time.Duration) *PartitionBackupManager {
	pbm.backupTimeout = timeout

	return pbm
}

// Create creates backup for all the partitions of given storage.
func (pbm *PartitionBackupManager) Create(ctx context.Context, serverInfo storage.ServerInfo, storageName string, logger log.Logger) error {
	client, err := pbm.createClient(ctx, serverInfo)
	if err != nil {
		return fmt.Errorf("create partition client: %w", err)
	}

	g, gCtx := errgroup.WithContext(ctx)
	partitionChan := make(chan *gitalypb.Partition)
	var successCount, errCount atomic.Uint64

	// Create a slice to store successful backups
	var successfulBackups []*gitalypb.PartitionBackupInfo
	var mu sync.Mutex

	for i := 0; i < pbm.concurrencyLimit; i++ {
		g.Go(func() error {
			for partition := range partitionChan {
				backupResponse, err := pbm.backupPartition(gCtx, client, storageName, partition.GetId())
				if err != nil {
					logger.Error(fmt.Sprintf("backup partition %s failed: %v", partition.GetId(), err))
					errCount.Add(1)
				} else {
					successCount.Add(1)
					// Store successful backup info
					mu.Lock()
					successfulBackups = append(successfulBackups, &gitalypb.PartitionBackupInfo{
						PartitionId:        partition.GetId(),
						BackupRelativePath: backupResponse.GetBackupRelativePath(),
						Timestamp:          timestamppb.Now(),
					})
					mu.Unlock()
				}
			}
			return nil
		})
	}

	g.Go(func() error {
		defer close(partitionChan)
		return pbm.listAndQueuePartitions(gCtx, client, storageName, partitionChan, logger)
	})

	err = g.Wait()

	// Update manifest with successful backups
	if len(successfulBackups) > 0 {
		_, updateErr := client.CreatePartitionBackupManifest(ctx, &gitalypb.CreatePartitionBackupManifestRequest{
			StorageName: storageName,
			Timestamp:   timestamppb.Now(),
			Partitions:  successfulBackups,
		})
		if updateErr != nil {
			logger.Error(fmt.Sprintf("failed to update manifest: %v", updateErr))
			// Decide whether to return this error or just log it
		} else {
			logger.Info("successfully updated backup manifest")
		}
	}

	success := successCount.Load()
	failure := errCount.Load()
	logger.Info(fmt.Sprintf("Partition backup completed: %d succeeded, %d failed", success, failure))

	if err == nil && errCount.Load() > 0 {
		err = fmt.Errorf("partition backup failed for %d out of %d partition(s)", failure, success+failure)
	}

	return err
}

func (pbm *PartitionBackupManager) backupPartition(ctx context.Context, client gitalypb.PartitionServiceClient, storageName string, partitionID string) (*gitalypb.BackupPartitionResponse, error) {
	backupCtx, backupCancel := context.WithTimeout(ctx, pbm.backupTimeout)
	defer backupCancel()

	response, err := client.BackupPartition(backupCtx, &gitalypb.BackupPartitionRequest{
		StorageName: storageName,
		PartitionId: partitionID,
	})

	return response, err
}

func (pbm *PartitionBackupManager) listAndQueuePartitions(ctx context.Context, client gitalypb.PartitionServiceClient, storageName string, workQueue chan<- *gitalypb.Partition, logger log.Logger) error {
	var cursor *gitalypb.PaginationCursor
	for {
		resp, err := client.ListPartitions(ctx, &gitalypb.ListPartitionsRequest{
			StorageName: storageName,
			PaginationParams: &gitalypb.PaginationParameter{
				PageToken: cursor.GetNextCursor(),
				Limit:     pbm.paginationLimit,
			},
		})
		if err != nil {
			return fmt.Errorf("list partitions: %w", err)
		}

		logger.Info(fmt.Sprintf("processing %d partitions", len(resp.GetPartitions())))
		for _, partition := range resp.GetPartitions() {
			select {
			case workQueue <- partition:
			case <-ctx.Done():
				return ctx.Err()
			}
		}

		cursor = resp.GetPaginationCursor()
		if cursor.GetNextCursor() == "" {
			break
		}
		logger.Info("processing next batch")
	}
	return nil
}
