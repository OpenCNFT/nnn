package partition

import (
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/housekeeping"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/storagemgr/snapshot"
)

// Metrics contains the metrics collected across all TransactionManagers.
type Metrics struct {
	housekeeping *housekeeping.Metrics
	snapshot     snapshot.Metrics
}

// NewMetrics returns a new Metrics instance.
func NewMetrics(housekeeping *housekeeping.Metrics, snapshot snapshot.Metrics) Metrics {
	return Metrics{
		housekeeping: housekeeping,
		snapshot:     snapshot,
	}
}

// Scope scopes the metrics to a TransactionManager.
func (m Metrics) Scope(storageName string) ManagerMetrics {
	return ManagerMetrics{
		housekeeping: m.housekeeping,
		snapshot:     m.snapshot.Scope(storageName),
	}
}

// ManagerMetrics contains the metrics collected by a TransactionManager.
type ManagerMetrics struct {
	housekeeping *housekeeping.Metrics
	snapshot     snapshot.ManagerMetrics
}

// transactionMetrics contains the metrics collected by a Transaction.
type transactionMetrics struct {
	housekeeping *housekeeping.Metrics
}
