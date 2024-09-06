package storagemgr

import (
	"github.com/prometheus/client_golang/prometheus"
	gitalycfgprom "gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/config/prometheus"
)

type metrics struct {
	partitionsStarted *prometheus.CounterVec
	partitionsStopped *prometheus.CounterVec
}

func newMetrics(promCfg gitalycfgprom.Config) *metrics {
	labels := []string{"storage"}
	return &metrics{
		partitionsStarted: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "gitaly_partitions_started_total",
			Help: "Number of partitions started.",
		}, labels),
		partitionsStopped: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "gitaly_partitions_stopped_total",
			Help: "Number of partitions stopped.",
		}, labels),
	}
}

// Describe is used to describe Prometheus metrics.
func (m *metrics) Describe(metrics chan<- *prometheus.Desc) {
	prometheus.DescribeByCollect(m, metrics)
}

// Collect is used to collect Prometheus metrics.
func (m *metrics) Collect(metrics chan<- prometheus.Metric) {
	m.partitionsStarted.Collect(metrics)
	m.partitionsStopped.Collect(metrics)
}

// storageManageMetrics returns metrics scoped for a specific storageManager.
func (m *metrics) storageManagerMetrics(storage string) storageManagerMetrics {
	labels := prometheus.Labels{"storage": storage}
	return storageManagerMetrics{
		partitionsStarted: m.partitionsStarted.With(labels),
		partitionsStopped: m.partitionsStopped.With(labels),
	}
}
