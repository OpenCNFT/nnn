package partition

import (
	"github.com/prometheus/client_golang/prometheus"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/housekeeping"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/storagemgr/snapshot"
)

// Metrics contains the metrics collected across all TransactionManagers.
type Metrics struct {
	// These are injected metrics that are needed by TransactionManagers. Collecting
	// them is the responsibility of the caller.
	housekeeping *housekeeping.Metrics
	snapshot     snapshot.Metrics

	commitQueueDepth *prometheus.GaugeVec
}

// NewMetrics returns a new Metrics instance.
func NewMetrics(housekeeping *housekeeping.Metrics, snapshot snapshot.Metrics) Metrics {
	storage := []string{"storage"}
	return Metrics{
		housekeeping: housekeeping,
		snapshot:     snapshot,
		commitQueueDepth: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "gitaly_transaction_commit_queue_depth",
			Help: "Records the number transactions waiting in the commit queue.",
		}, storage),
	}
}

// Describe implements prometheus.Collector.
func (m Metrics) Describe(out chan<- *prometheus.Desc) {
	prometheus.DescribeByCollect(m, out)
}

// Collect implements prometheus.Collector.
func (m Metrics) Collect(out chan<- prometheus.Metric) {
	m.commitQueueDepth.Collect(out)
}

// Scope scopes the metrics to a TransactionManager.
func (m Metrics) Scope(storageName string) ManagerMetrics {
	return ManagerMetrics{
		housekeeping:     m.housekeeping,
		snapshot:         m.snapshot.Scope(storageName),
		commitQueueDepth: m.commitQueueDepth.WithLabelValues(storageName),
	}
}

// ManagerMetrics contains the metrics collected by a TransactionManager.
type ManagerMetrics struct {
	housekeeping     *housekeeping.Metrics
	snapshot         snapshot.ManagerMetrics
	commitQueueDepth prometheus.Gauge
}
