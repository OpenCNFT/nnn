package partition

import (
	"context"
	"fmt"

	"gitlab.com/gitlab-org/gitaly/v16/internal/git/gitcmd"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/keyvalue"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/storagemgr"
	"gitlab.com/gitlab-org/gitaly/v16/internal/log"
)

// LogConsumer is the interface of a log consumer that is passed to a TransactionManager.
// The LogConsumer may perform read-only operations against the on-disk log entry.
// The TransactionManager notifies the consumer of new transactions by invoking the
// NotifyNewTransaction method after they are committed.
type LogConsumer interface {
	// NotifyNewTransactions alerts the LogConsumer that new log entries are available for
	// consumption. The method invoked both when the TransactionManager
	// initializes and when new transactions are committed. Both the low and high water mark
	// LSNs are sent so that a newly initialized consumer is aware of the full range of
	// entries it can process.
	NotifyNewTransactions(storageName string, partitionID storage.PartitionID, lowWaterMark, highWaterMark storage.LSN)
}

// Factory is factory type that can create new partitions.
type Factory struct {
	cmdFactory  gitcmd.CommandFactory
	repoFactory localrepo.Factory
	metrics     Metrics
	logConsumer LogConsumer
}

// New returns a new Partition instance.
func (f Factory) New(
	logger log.Logger,
	partitionID storage.PartitionID,
	db keyvalue.Transactioner,
	storageName string,
	storagePath string,
	absoluteStateDir string,
	stagingDir string,
) storagemgr.Partition {
	// ScopeByStorage takes in context to pass it to the locator. This may be useful in the
	// RPC handlers to rewrite the storage in the future but never here. Requiring a context
	// here is more of a structural issue in the code, and is not useful.
	repoFactory, err := f.repoFactory.ScopeByStorage(context.Background(), storageName)
	if err != nil {
		// ScopeByStorage will only error if accessing a non existent storage. This can't
		// be the case when Factory is used as the storage is already verified.
		// This is a layering issue in the code, and not a realistic error scenario. We
		// thus panic out rather than make the error part of the interface.
		panic(fmt.Errorf("building a partition for a non-existent storage: %q", storageName))
	}

	return NewTransactionManager(
		partitionID,
		logger,
		db,
		storageName,
		storagePath,
		absoluteStateDir,
		stagingDir,
		f.cmdFactory,
		repoFactory,
		f.metrics.Scope(storageName),
		f.logConsumer,
		nil,
	)
}

// NewFactory returns a new Factory.
func NewFactory(
	cmdFactory gitcmd.CommandFactory,
	repoFactory localrepo.Factory,
	metrics Metrics,
	logConsumer LogConsumer,
) Factory {
	return Factory{
		cmdFactory:  cmdFactory,
		repoFactory: repoFactory,
		metrics:     metrics,
		logConsumer: logConsumer,
	}
}
