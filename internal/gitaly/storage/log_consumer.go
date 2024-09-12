package storage

// LogManager is the interface used on the consumer side of the integration. The consumer
// has the ability to acknowledge transactions as having been processed with AcknowledgeTransaction.
type LogManager interface {
	// AcknowledgeTransaction acknowledges log entries up and including lsn as successfully processed
	// for the specified LogConsumer.
	AcknowledgeTransaction(lsn LSN)
	// GetTransactionPath returns the path of the log entry's root directory.
	GetTransactionPath(lsn LSN) string
}
