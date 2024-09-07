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

	commitQueueDepth                           *prometheus.GaugeVec
	commitQueueWaitSeconds                     *prometheus.HistogramVec
	transactionControlStatementDurationSeconds *prometheus.HistogramVec
}

// NewMetrics returns a new Metrics instance.
func NewMetrics(housekeeping *housekeeping.Metrics, snapshot snapshot.Metrics) Metrics {
	storage := []string{"storage"}
	storageAccessMode := append(storage, "access_mode")

	buckets := prometheus.ExponentialBuckets(0.01, 2, 10)

	return Metrics{
		housekeeping: housekeeping,
		snapshot:     snapshot,
		commitQueueDepth: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "gitaly_transaction_commit_queue_depth",
			Help: "Records the number transactions waiting in the commit queue.",
		}, storage),
		commitQueueWaitSeconds: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "gitaly_transaction_commit_queue_wait_seconds",
			Help:    "Records the duration transactions are waiting in the commit queue.",
			Buckets: buckets,
		}, storage),
		transactionControlStatementDurationSeconds: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "gitaly_transaction_control_statement_duration_seconds",
			Help:    "Records the time taken to execute a transaction control statement.",
			Buckets: buckets,
		}, append(storageAccessMode, "control_statement")),
	}
}

// Describe implements prometheus.Collector.
func (m Metrics) Describe(out chan<- *prometheus.Desc) {
	prometheus.DescribeByCollect(m, out)
}

// Collect implements prometheus.Collector.
func (m Metrics) Collect(out chan<- prometheus.Metric) {
	m.commitQueueDepth.Collect(out)
	m.commitQueueWaitSeconds.Collect(out)
	m.transactionControlStatementDurationSeconds.Collect(out)
}

// Scope scopes the metrics to a TransactionManager.
func (m Metrics) Scope(storageName string) ManagerMetrics {
	const (
		read     = "read"
		write    = "write"
		begin    = "begin"
		commit   = "commit"
		rollback = "rollback"
	)

	return ManagerMetrics{
		housekeeping:                            m.housekeeping,
		snapshot:                                m.snapshot.Scope(storageName),
		commitQueueDepth:                        m.commitQueueDepth.WithLabelValues(storageName),
		commitQueueWaitSeconds:                  m.commitQueueWaitSeconds.WithLabelValues(storageName),
		readBeginDurationSeconds:                m.transactionControlStatementDurationSeconds.WithLabelValues(storageName, read, begin),
		writeBeginDurationSeconds:               m.transactionControlStatementDurationSeconds.WithLabelValues(storageName, write, begin),
		readTransactionCommitDurationSeconds:    m.transactionControlStatementDurationSeconds.WithLabelValues(storageName, read, commit),
		writeTransactionCommitDurationSeconds:   m.transactionControlStatementDurationSeconds.WithLabelValues(storageName, write, commit),
		readTransactionRollbackDurationSeconds:  m.transactionControlStatementDurationSeconds.WithLabelValues(storageName, read, rollback),
		writeTransactionRollbackDurationSeconds: m.transactionControlStatementDurationSeconds.WithLabelValues(storageName, write, rollback),
	}
}

// ManagerMetrics contains the metrics collected by a TransactionManager.
type ManagerMetrics struct {
	housekeeping                            *housekeeping.Metrics
	snapshot                                snapshot.ManagerMetrics
	commitQueueDepth                        prometheus.Gauge
	commitQueueWaitSeconds                  prometheus.Observer
	readBeginDurationSeconds                prometheus.Observer
	writeBeginDurationSeconds               prometheus.Observer
	readTransactionCommitDurationSeconds    prometheus.Observer
	writeTransactionCommitDurationSeconds   prometheus.Observer
	readTransactionRollbackDurationSeconds  prometheus.Observer
	writeTransactionRollbackDurationSeconds prometheus.Observer
}

func (m ManagerMetrics) beginDuration(write bool) prometheus.Observer {
	if write {
		return m.writeBeginDurationSeconds
	}

	return m.readBeginDurationSeconds
}

func (m ManagerMetrics) commitDuration(write bool) prometheus.Observer {
	if write {
		return m.writeTransactionCommitDurationSeconds
	}

	return m.readTransactionCommitDurationSeconds
}

func (m ManagerMetrics) rollbackDuration(write bool) prometheus.Observer {
	if write {
		return m.writeTransactionRollbackDurationSeconds
	}

	return m.readTransactionRollbackDurationSeconds
}
