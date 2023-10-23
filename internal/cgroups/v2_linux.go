//go:build linux

package cgroups

import (
	"errors"
	"fmt"
	"io/fs"
	"path/filepath"
	"strings"
	"time"

	"github.com/containerd/cgroups/v3/cgroup2"
	"github.com/opencontainers/runtime-spec/specs-go"
	"github.com/prometheus/client_golang/prometheus"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/config"
	cgroupscfg "gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/config/cgroups"
	"gitlab.com/gitlab-org/gitaly/v16/internal/kernel"
	"gitlab.com/gitlab-org/gitaly/v16/internal/log"
)

type cgroupV2Handler struct {
	cfg    cgroupscfg.Config
	logger log.Logger

	*cgroupsMetrics
	pid             int
	cloneIntoCgroup bool
}

func newV2Handler(cfg cgroupscfg.Config, logger log.Logger, pid int) *cgroupV2Handler {
	cloneIntoCgroup, err := kernel.IsAtLeast(kernel.Version{Major: 5, Minor: 7})
	if err != nil {
		// Log the error for now as we're only rolling out functionality behind feature flag.
		logger.WithError(err).Error("failed detecting kernel version, CLONE_INTO_CGROUP support disabled")
	}

	return &cgroupV2Handler{
		cfg:             cfg,
		logger:          logger,
		pid:             pid,
		cgroupsMetrics:  newV2CgroupsMetrics(),
		cloneIntoCgroup: cloneIntoCgroup,
	}
}

func (cvh *cgroupV2Handler) setupParent(parentResources *specs.LinuxResources) error {
	if _, err := cgroup2.NewManager(cvh.cfg.Mountpoint, "/"+cvh.currentProcessCgroup(), cgroup2.ToResources(parentResources)); err != nil {
		return fmt.Errorf("failed creating parent cgroup: %w", err)
	}

	return nil
}

func (cvh *cgroupV2Handler) setupRepository(reposResources *specs.LinuxResources) error {
	for i := 0; i < int(cvh.cfg.Repositories.Count); i++ {
		if _, err := cgroup2.NewManager(
			cvh.cfg.Mountpoint,
			"/"+cvh.repoPath(i),
			cgroup2.ToResources(reposResources),
		); err != nil {
			return fmt.Errorf("failed creating repository cgroup: %w", err)
		}
	}
	return nil
}

func (cvh *cgroupV2Handler) addToCgroup(pid int, cgroupPath string) error {
	control, err := cgroup2.Load("/"+cgroupPath, cgroup2.WithMountpoint(cvh.cfg.Mountpoint))
	if err != nil {
		return fmt.Errorf("failed loading %s cgroup: %w", cgroupPath, err)
	}

	if err := control.AddProc(uint64(pid)); err != nil {
		// Command could finish so quickly before we can add it to a cgroup, so
		// we don't consider it an error.
		if strings.Contains(err.Error(), "no such process") {
			return nil
		}
		return fmt.Errorf("failed adding process to cgroup: %w", err)
	}

	return nil
}

func (cvh *cgroupV2Handler) collect(ch chan<- prometheus.Metric) {
	if !cvh.cfg.MetricsEnabled {
		return
	}

	for i := 0; i < int(cvh.cfg.Repositories.Count); i++ {
		repoPath := cvh.repoPath(i)
		logger := cvh.logger.WithField("cgroup_path", repoPath)
		control, err := cgroup2.Load("/"+repoPath, cgroup2.WithMountpoint(cvh.cfg.Mountpoint))
		if err != nil {
			logger.WithError(err).Warn("unable to load cgroup controller")
			return
		}

		if metrics, err := control.Stat(); err != nil {
			logger.WithError(err).Warn("unable to get cgroup stats")
		} else {
			cpuUserMetric := cvh.cpuUsage.WithLabelValues(repoPath, "user")
			cpuUserMetric.Set(float64(metrics.CPU.UserUsec))
			ch <- cpuUserMetric

			ch <- prometheus.MustNewConstMetric(
				cvh.cpuCFSPeriods,
				prometheus.CounterValue,
				float64(metrics.CPU.NrPeriods),
				repoPath,
			)

			ch <- prometheus.MustNewConstMetric(
				cvh.cpuCFSThrottledPeriods,
				prometheus.CounterValue,
				float64(metrics.CPU.NrThrottled),
				repoPath,
			)

			ch <- prometheus.MustNewConstMetric(
				cvh.cpuCFSThrottledTime,
				prometheus.CounterValue,
				float64(metrics.CPU.ThrottledUsec)/float64(time.Second),
				repoPath,
			)

			cpuKernelMetric := cvh.cpuUsage.WithLabelValues(repoPath, "kernel")
			cpuKernelMetric.Set(float64(metrics.CPU.SystemUsec))
			ch <- cpuKernelMetric
		}

		if subsystems, err := control.Controllers(); err != nil {
			logger.WithError(err).Warn("unable to get cgroup hierarchy")
		} else {
			processes, err := control.Procs(true)
			if err != nil {
				logger.WithError(err).
					Warn("unable to get process list")
				continue
			}

			for _, subsystem := range subsystems {
				procsMetric := cvh.procs.WithLabelValues(repoPath, subsystem)
				procsMetric.Set(float64(len(processes)))
				ch <- procsMetric
			}
		}
	}
}

func (cvh *cgroupV2Handler) cleanup() error {
	processCgroupPath := cvh.currentProcessCgroup()

	control, err := cgroup2.Load("/"+processCgroupPath, cgroup2.WithMountpoint(cvh.cfg.Mountpoint))
	if err != nil {
		return fmt.Errorf("failed loading cgroup %s: %w", processCgroupPath, err)
	}

	if err := control.Delete(); err != nil {
		return fmt.Errorf("failed cleaning up cgroup %s: %w", processCgroupPath, err)
	}

	return nil
}

func (cvh *cgroupV2Handler) repoPath(groupID int) string {
	return filepath.Join(cvh.currentProcessCgroup(), fmt.Sprintf("repos-%d", groupID))
}

func (cvh *cgroupV2Handler) currentProcessCgroup() string {
	return config.GetGitalyProcessTempDir(cvh.cfg.HierarchyRoot, cvh.pid)
}

func (cvh *cgroupV2Handler) stats() (Stats, error) {
	processCgroupPath := cvh.currentProcessCgroup()

	control, err := cgroup2.Load("/"+processCgroupPath, cgroup2.WithMountpoint(cvh.cfg.Mountpoint))
	if err != nil {
		return Stats{}, fmt.Errorf("failed loading cgroup %s: %w", processCgroupPath, err)
	}

	metrics, err := control.Stat()
	if err != nil {
		return Stats{}, fmt.Errorf("failed to fetch metrics %s: %w", processCgroupPath, err)
	}

	stats := Stats{
		ParentStats: CgroupStats{
			CPUThrottledCount:    metrics.CPU.NrThrottled,
			CPUThrottledDuration: float64(metrics.CPU.ThrottledUsec) / float64(time.Second),
			MemoryUsage:          metrics.Memory.Usage,
			MemoryLimit:          metrics.Memory.UsageLimit,
		},
	}
	if metrics.MemoryEvents != nil {
		stats.ParentStats.OOMKills = metrics.MemoryEvents.OomKill
	}
	return stats, nil
}

func (cvh *cgroupV2Handler) supportsCloneIntoCgroup() bool {
	return cvh.cloneIntoCgroup
}

func pruneOldCgroupsV2(cfg cgroupscfg.Config, logger log.Logger) {
	if err := config.PruneOldGitalyProcessDirectories(
		logger,
		filepath.Join(cfg.Mountpoint, cfg.HierarchyRoot),
	); err != nil {
		var pathError *fs.PathError
		if !errors.As(err, &pathError) {
			logger.WithError(err).Error("failed to clean up cpu cgroups")
		}
	}
}
