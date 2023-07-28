package cgroups

import (
	"os/exec"

	"github.com/prometheus/client_golang/prometheus"
	log "github.com/sirupsen/logrus"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/config/cgroups"
)

type addCommandCfg struct {
	cgroupKey string
}

// CgroupStats stores the current usage statistics of the resources managed by
// cgroup manager. They are fetched from the cgroupfs statistic files.
type CgroupStats struct {
	// CPUThrottledCount is much the CPU was throttled, this field is fetched from the `nr_throttled` field of
	// `cpu.stat` file.
	CPUThrottledCount uint64
	// CPUThrottledCount is the accumulated duration of CPU throttled time in seconds. It is from the
	// `throttled_time` (V1) or the `throttled_usec` (v2) field of the `cpu.stat` file.
	CPUThrottledDuration float64
	// MemoryUsage is the current memory usage in bytes of a cgroup. It's fetched from the `memory.usage_in_bytes`
	// (V1) or the `memory.current` (V2) files.
	MemoryUsage uint64
	// MemoryLimit is the current memory limit in bytes of a cgroup. It's from the `memory.limit_in_bytes` (V1) or
	// the `memory.max` (V2) files.
	MemoryLimit uint64
	// OOMKills is the accumulated count when the cgroup is being OOM-ed. It's read from the `memory.oom_control`
	// (V1) and the `memory.events` (V2) files.
	OOMKills uint64
	// UnderOOM is the current OOM status of a cgroup. This information is available for Cgroup V1 only. It's read
	// from the `memory.oom_control` file.
	UnderOOM bool
}

// Stats stores statistics of all cgroups managed by a manager
type Stats struct {
	// ParentStats stores the statistics of the parent cgroup. There should be
	// an array of per-repository cgroups, but we haven't used that information
	// yet.
	ParentStats CgroupStats
}

// AddCommandOption is an option that can be passed to AddCommand.
type AddCommandOption func(*addCommandCfg)

// WithCgroupKey overrides the key used to derive the Cgroup bucket. If not passed, then the command
// arguments will be used as the cgroup key.
func WithCgroupKey(cgroupKey string) AddCommandOption {
	return func(cfg *addCommandCfg) {
		cfg.cgroupKey = cgroupKey
	}
}

// Manager supplies an interface for interacting with cgroups
type Manager interface {
	// Setup creates cgroups and assigns configured limitations.
	// It is expected to be called once at Gitaly startup from any
	// instance of the Manager.
	Setup() error
	// Ready returns true if this cgroup manager is configured and
	// ready to use.
	Ready() bool
	// AddCommand adds a Cmd to a cgroup.
	AddCommand(*exec.Cmd, ...AddCommandOption) (string, error)
	// Cleanup cleans up cgroups created in Setup.
	// It is expected to be called once at Gitaly shutdown from any
	// instance of the Manager.
	Cleanup() error
	// Stats returns cgroup accounting statistics collected by reading
	// cgroupfs files. Those statistics are generic for both Cgroup V1
	// and Cgroup V2.
	Stats() (Stats, error)
	Describe(ch chan<- *prometheus.Desc)
	Collect(ch chan<- prometheus.Metric)
}

// NewManager returns the appropriate Cgroups manager
func NewManager(cfg cgroups.Config, pid int) Manager {
	if cfg.Repositories.Count > 0 {
		return newCgroupManager(cfg, pid)
	}

	return &NoopManager{}
}

// PruneOldCgroups prunes old cgroups for both the memory and cpu subsystems
func PruneOldCgroups(cfg cgroups.Config, logger log.FieldLogger) {
	pruneOldCgroups(cfg, logger)
}
