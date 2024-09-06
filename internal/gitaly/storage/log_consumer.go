package storage

import (
	"context"
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
	NotifyNewTransactions(storageName string, partitionID PartitionID, lowWaterMark, highWaterMark LSN)
}

// LogManagerAccessor is the interface used by the LogManager coordinator. It is called by
// by LogConsumers to access LogManagers. A LogManager that notified a LogConsumer of a transaction
// may have closed by the time the consumer has finished acting on the log entry. The LogManagerAccessor
// ensures that the LogManager is available to receive the consumer's response.
type LogManagerAccessor interface {
	// CallLogManager executes the provided function against the requested LogManager, starting it
	// if necessary.
	CallLogManager(ctx context.Context, storageName string, partitionID PartitionID, fn func(LogManager)) error
}

// LogManager is the interface used on the consumer side of the integration. The consumer
// has the ability to acknowledge transactions as having been processed with AcknowledgeTransaction.
type LogManager interface {
	// AcknowledgeTransaction acknowledges log entries up and including lsn as successfully processed
	// for the specified LogConsumer.
	AcknowledgeTransaction(consumer LogConsumer, lsn LSN)
	// GetTransactionPath returns the path of the log entry's root directory.
	GetTransactionPath(lsn LSN) string
}
