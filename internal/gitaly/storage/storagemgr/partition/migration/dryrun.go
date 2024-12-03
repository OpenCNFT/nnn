package migration

import (
	"context"
	"sync"

	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/storagemgr"
	"gitlab.com/gitlab-org/gitaly/v16/internal/log"
)

// dryRunMigrations are a set of migrations which we want to dry-run.
//
// These migrations do not write to disk, since we stub the transaction
// to ensure it is never committed.
//
// While we don't write the migration IDs to the KV, we do read from the
// KV to get the last migration ID. To ensure that all dry-run migrations
// are run, migrations will have to use IDs > larget live migration ID.
var dryRunMigrations []migration

// dryRunTransaction stubs the actual transaction to ensure that the `Commit()`
// method doesn't apply. We simply call `Rollback()` instead.
type dryRunTransaction struct {
	storage.Transaction
}

// Commit overrides the default commit method to call `Rollback`, since we are
// running in dry-run mode.
func (d dryRunTransaction) Commit(ctx context.Context) error {
	return d.Rollback(ctx)
}

// dryRunPartition implements the Partition interface but returns a dryRunTransaction.
type dryRunPartition struct {
	storagemgr.Partition
}

// Begin is overrided to return a transaction which stubs the Commit method
// to call Rollback instead.
func (d dryRunPartition) Begin(ctx context.Context, opts storage.BeginOptions) (storage.Transaction, error) {
	txn, err := d.Partition.Begin(ctx, opts)
	if err != nil {
		return txn, err
	}

	return dryRunTransaction{txn}, nil
}

// combinedMigrationPartition implements the Partition interface. It wraps around the
// migration manager. While doing so, it also creates a dry-run migration manager, which uses
// a dryRunPartition and dryRunMigrations.
type combinedMigrationPartition struct {
	storagemgr.Partition
	logger log.Logger
	wg     sync.WaitGroup
	dryRun storagemgr.Partition
}

func newCombinedMigrationPartition(partition storagemgr.Partition, logger log.Logger) storagemgr.Partition {
	return &combinedMigrationPartition{
		Partition: NewPartition(partition, logger, migrations),
		logger:    logger,
		wg:        sync.WaitGroup{},
		dryRun:    NewPartition(dryRunPartition{partition}, logger, dryRunMigrations),
	}
}

// Begin here is overrided to run both the dry-run migrations and the regular migraitons.
// For the dry-run migrations, we simply invoke it in a go-routine and log any failures.
func (c *combinedMigrationPartition) Begin(ctx context.Context, opts storage.BeginOptions) (storage.Transaction, error) {
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()

		txn, err := c.dryRun.Begin(ctx, opts)
		if err != nil {
			c.logger.WithError(err).Info("failed to begin migration dry-run")
		}

		// We shouldn't let the base transaction hanging, roll it back.
		if txn != nil {
			if err := txn.Rollback(ctx); err != nil {
				c.logger.WithError(err).Info("failed to rollback migration dry-run")
			}
		}
	}()

	return c.Partition.Begin(ctx, opts)
}

// Close is also overrided to ensure that all goroutines that have been spawned are closed
// before we close the transaction.
func (c *combinedMigrationPartition) Close() {
	c.wg.Wait()
	c.Partition.Close()
}
