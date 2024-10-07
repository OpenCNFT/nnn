package backup

import (
	"context"
	"fmt"
	"time"

	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v16/internal/grpc/client"
	"gitlab.com/gitlab-org/gitaly/v16/internal/log"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// PartititonBackupManager manages process of the creating/restoring partition backups.
type PartititonBackupManager struct {
	pool            *client.Pool
	paginationLimit int32         // pagination page size
	backupTimeout   time.Duration // timeout for individual partition backup calls
}

// NewPartititonBackupManager creates and returns initialized *PartititonBackupManager instance.
func NewPartititonBackupManager(pool *client.Pool) *PartititonBackupManager {
	return &PartititonBackupManager{
		pool:            pool,
		paginationLimit: 100,
		backupTimeout:   time.Minute * 5,
	}
}

// WithPaginationLimit sets the pagination page size. If not specified, the default page size is 100.
func (pbm *PartititonBackupManager) WithPaginationLimit(limit int32) *PartititonBackupManager {
	pbm.paginationLimit = limit

	return pbm
}

// WithBackupTimeout sets the timeout for individual partition backup calls. If not specified, the default timeout is 5 minutes.
func (pbm *PartititonBackupManager) WithBackupTimeout(timeout time.Duration) *PartititonBackupManager {
	pbm.backupTimeout = timeout

	return pbm
}

func (pbm *PartititonBackupManager) newPartitionClient(ctx context.Context, server storage.ServerInfo) (gitalypb.PartitionServiceClient, error) {
	conn, err := pbm.pool.Dial(ctx, server.Address, server.Token)
	if err != nil {
		return nil, err
	}

	return gitalypb.NewPartitionServiceClient(conn), nil
}

// Create creates backup for all the partitions of given storage.
func (pbm *PartititonBackupManager) Create(ctx context.Context, serverInfo storage.ServerInfo, storageName string, logger log.Logger) error {
	client, err := pbm.newPartitionClient(ctx, serverInfo)
	if err != nil {
		return fmt.Errorf("create partition client: %w", err)
	}

	var cursor *gitalypb.PaginationCursor
	totalCount := 0
	for {
		logger.Info("getting partitions...")

		resp, err := client.ListPartitions(ctx, &gitalypb.ListPartitionsRequest{
			StorageName: storageName,
			PaginationParams: &gitalypb.PaginationParameter{
				PageToken: cursor.GetNextCursor(),
				Limit:     pbm.paginationLimit,
			},
		})
		if err != nil {
			if status, ok := status.FromError(err); ok && status.Code() == codes.Canceled {
				logger.Info(fmt.Sprintf("backup operation terminated, successfully archived %d partitions.", totalCount))
				return nil
			}

			return fmt.Errorf("list partitions: %w", err)
		}

		partitions := resp.GetPartitions()
		partitionCount := len(partitions)

		logger.Info(fmt.Sprintf("processing %d partitions", partitionCount))

		for _, partition := range partitions {
			if err := pbm.backupPartition(ctx, client, storageName, partition.GetId()); err != nil {
				if status, ok := status.FromError(err); ok && status.Code() == codes.Canceled {
					logger.Info(fmt.Sprintf("backup operation terminated, successfully archived %d partitions.", totalCount))
					return nil
				}

				logger.Info(fmt.Sprintf("successfully archived %d partitions and failed at partition id %s", totalCount, partition.GetId()))
				return fmt.Errorf("backup partition: %w", err)
			}

			totalCount++
		}

		logger.Info(fmt.Sprintf("archived %d partitions", partitionCount))

		paginationCursor := resp.GetPaginationCursor()
		if paginationCursor.GetNextCursor() == "" {
			break
		}
		cursor = paginationCursor

		logger.Info("processing next batch")
	}

	return nil
}

func (pbm *PartititonBackupManager) backupPartition(ctx context.Context, client gitalypb.PartitionServiceClient, storageName string, partitionID string) error {
	backupCtx, backupCancel := context.WithTimeout(ctx, pbm.backupTimeout)
	defer backupCancel()

	_, err := client.BackupPartition(backupCtx, &gitalypb.BackupPartitionRequest{
		StorageName: storageName,
		PartitionId: partitionID,
	})

	if status, ok := status.FromError(err); ok && status.Code() == codes.AlreadyExists {
		// If there is an up to date backup for the partition, we consider this as successful operation.
		return nil
	}

	return err
}
