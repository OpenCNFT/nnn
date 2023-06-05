package housekeeping

import (
	"context"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/localrepo"
	gitalycfgprom "gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/config/prometheus"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/transaction"
)

// Manager is a housekeeping manager. It is supposed to handle housekeeping tasks for repositories
// such as the cleanup of unneeded files and optimizations for the repository's data structures.
type Manager interface {
	// CleanStaleData removes any stale data in the repository as per the provided configuration.
	CleanStaleData(ctx context.Context, repo *localrepo.Repo, cfg CleanStaleDataConfig) error
	// OptimizeRepository optimizes the repository's data structures such that it can be more
	// efficiently served.
	OptimizeRepository(context.Context, *localrepo.Repo, ...OptimizeRepositoryOption) error
	// AddPackRefsInhibitor allows clients to block housekeeping from running git-pack-refs(1).
	AddPackRefsInhibitor(ctx context.Context, repoPath string) (bool, func(), error)
}

// repositoryState holds the housekeeping state for individual repositories. This structure can be
// used to sync between different housekeeping goroutines. It is safer to access this via the methods
// of repositoryStates structure.
type repositoryState struct {
	sync.Mutex

	// packRefsDone is a channel used to denote when the ongoing (if any) call to packRefsIfNeeded
	// is completed.
	packRefsDone chan struct{}
	// packRefsCancel is the context cancellation function which can be used to cancel any
	// running git-pack-refs(1).
	packRefsCancel context.CancelFunc
	// packRefsInhibitors keeps a count of the number of inhibitors on running packRefsIfNeeded.
	packRefsInhibitors int32
	// isRunning is used to indicate if housekeeping is running.
	isRunning bool
}

// refCountedState keeps count of number of goroutines using a paritcular repository state, this is used
// to ensure that we only delete a particular state of a repository when there are no goroutines which
// are accessing it.
type refCountedState struct {
	// state is a pointer to a single repositories state.
	state *repositoryState
	// rc keeps count of the number of goroutines using the state.
	rc uint32
}

// repositoryStates holds per-repsitory information to sync between different goroutines.
// Access to the internal fields should be done via the methods provided by the struct.
type repositoryStates struct {
	sync.Mutex
	// values is map which denotes per-repository housekeeping state.
	values map[string]*refCountedState
}

// getState provides the state and cleanup function for a given repository path.
// The cleanup function deletes the state if the caller is the last goroutine referencing
// the state.
func (s *repositoryStates) getState(repoPath string) (*repositoryState, func()) {
	s.Lock()
	defer s.Unlock()

	value, ok := s.values[repoPath]
	if !ok {
		s.values[repoPath] = &refCountedState{
			rc:    0,
			state: &repositoryState{},
		}
		value = s.values[repoPath]
	}

	value.rc++

	return value.state, func() {
		s.Lock()
		defer s.Unlock()

		value.rc--
		if value.rc == 0 {
			delete(s.values, repoPath)
		}
	}
}

// tryRunningHousekeeping denotes if housekeeping can be run on a given repository.
// If successful, it also provides a cleanup function which resets the state so other
// goroutines can run housekeeping on the repository.
func (s *repositoryStates) tryRunningHousekeeping(repoPath string) (successful bool, _ func()) {
	state, cleanup := s.getState(repoPath)
	defer func() {
		if !successful {
			cleanup()
		}
	}()

	state.Lock()
	defer state.Unlock()

	if state.isRunning {
		return false, nil
	}
	state.isRunning = true

	return true, func() {
		defer cleanup()

		state.Lock()
		defer state.Unlock()

		state.isRunning = false
	}
}

// addPackRefsInhibitor is used to add an inhibitor over running `git-pack-refs(1)`.
// If `git-pack-refs(1)` is currently running, the caller waits till it finishes. The caller
// can also exit early by using a context cancellation instead.
//
// The function returns a cleanup function which decreases the inhibitor count and must
// be called by the client when it no longer blocks `git-pack-refs(1)`.
func (s *repositoryStates) addPackRefsInhibitor(ctx context.Context, repoPath string) (successful bool, _ func(), err error) {
	state, cleanup := s.getState(repoPath)
	defer func() {
		if !successful {
			cleanup()
		}
	}()

	// We don't defer unlock here to ensure that a single inhibitor doesn't lock the others.
	// We want inhibitors to be able to use context cancellation to exit early without being
	// locked by other inhibitors.
	state.Lock()

	for {
		packRefsDone := state.packRefsDone
		if packRefsDone == nil {
			break
		}

		// Instead of waiting for git-pack-refs to finish, we ask it to
		// stop.
		if state.packRefsCancel != nil {
			state.packRefsCancel()
		}

		state.Unlock()

		select {
		case <-ctx.Done():
			return false, nil, ctx.Err()
		case <-packRefsDone:
			// We don't use state.packRefsDone, cause there is possibility that it is set
			// to `nil` by the cleanup function after running `git-pack-refs(1)`.
			//
			// We obtain a lock and continue the loop here to avoid a race wherein another
			// goroutine has invoked git-pack-refs(1). By continuing the loop and checking
			// the value of packRefsDone, we can avoid that scenario.
			state.Lock()
			continue
		}
	}

	defer state.Unlock()

	state.packRefsInhibitors++

	return true, func() {
		defer cleanup()

		state.Lock()
		defer state.Unlock()

		state.packRefsInhibitors--
	}, nil
}

// tryRunningPackRefs checks if we can run `git-pack-refs(1)` for a given repository. If there
// is at least one inhibitors then we return false. If there are no inhibitors, we setup the `packRefsDone`
// channel to denote when `git-pack-refs(1)` finishes, this is handled when the caller calls the
// cleanup function returned by this function.
func (s *repositoryStates) tryRunningPackRefs(ctx context.Context, repoPath string) (successful bool, _ context.Context, _ func()) {
	state, cleanup := s.getState(repoPath)
	defer func() {
		if !successful {
			cleanup()
		}
	}()

	state.Lock()
	defer state.Unlock()

	if state.packRefsInhibitors > 0 || state.packRefsDone != nil {
		return false, ctx, nil
	}

	state.packRefsDone = make(chan struct{})

	ctx, cancel := context.WithCancel(ctx)
	state.packRefsCancel = cancel

	return true, ctx, func() {
		defer cleanup()

		state.Lock()
		defer state.Unlock()

		close(state.packRefsDone)
		state.packRefsDone = nil
		state.packRefsCancel = nil
	}
}

// RepositoryManager is an implementation of the Manager interface.
type RepositoryManager struct {
	txManager transaction.Manager

	tasksTotal                             *prometheus.CounterVec
	tasksLatency                           *prometheus.HistogramVec
	prunedFilesTotal                       *prometheus.CounterVec
	dataStructureExistence                 *prometheus.CounterVec
	dataStructureCount                     *prometheus.HistogramVec
	dataStructureSize                      *prometheus.HistogramVec
	dataStructureTimeSinceLastOptimization *prometheus.HistogramVec
	optimizeFunc                           func(context.Context, *RepositoryManager, *localrepo.Repo, OptimizationStrategy) error
	repositoryStates                       repositoryStates
}

// NewManager creates a new RepositoryManager.
func NewManager(promCfg gitalycfgprom.Config, txManager transaction.Manager) *RepositoryManager {
	return &RepositoryManager{
		txManager: txManager,

		tasksTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "gitaly_housekeeping_tasks_total",
				Help: "Total number of housekeeping tasks performed in the repository",
			},
			[]string{"housekeeping_task", "status"},
		),
		tasksLatency: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "gitaly_housekeeping_tasks_latency",
				Help:    "Latency of the housekeeping tasks performed",
				Buckets: promCfg.GRPCLatencyBuckets,
			},
			[]string{"housekeeping_task"},
		),
		prunedFilesTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "gitaly_housekeeping_pruned_files_total",
				Help: "Total number of files pruned",
			},
			[]string{"filetype"},
		),
		dataStructureExistence: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "gitaly_housekeeping_data_structure_existence_total",
				Help: "Total number of data structures that exist in the repository",
			},
			[]string{"data_structure", "exists"},
		),
		dataStructureCount: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "gitaly_housekeeping_data_structure_count",
				Help:    "Total count of the data structures that exist in the repository",
				Buckets: prometheus.ExponentialBucketsRange(1, 10_000_000, 32),
			},
			[]string{"data_structure"},
		),
		dataStructureSize: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "gitaly_housekeeping_data_structure_size",
				Help:    "Total size of the data structures that exist in the repository",
				Buckets: prometheus.ExponentialBucketsRange(1, 50_000_000_000, 32),
			},
			[]string{"data_structure"},
		),
		dataStructureTimeSinceLastOptimization: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name: "gitaly_housekeeping_time_since_last_optimization_seconds",
				Help: "Absolute time in seconds since a given optimization has last been performed",
				Buckets: []float64{
					time.Second.Seconds(),
					time.Minute.Seconds(),
					(5 * time.Minute).Seconds(),
					(10 * time.Minute).Seconds(),
					(30 * time.Minute).Seconds(),
					(1 * time.Hour).Seconds(),
					(3 * time.Hour).Seconds(),
					(6 * time.Hour).Seconds(),
					(12 * time.Hour).Seconds(),
					(18 * time.Hour).Seconds(),
					(1 * 24 * time.Hour).Seconds(),
					(2 * 24 * time.Hour).Seconds(),
					(3 * 24 * time.Hour).Seconds(),
					(5 * 24 * time.Hour).Seconds(),
					(7 * 24 * time.Hour).Seconds(),
					(14 * 24 * time.Hour).Seconds(),
					(21 * 24 * time.Hour).Seconds(),
					(28 * 24 * time.Hour).Seconds(),
				},
			},
			[]string{"data_structure"},
		),
		optimizeFunc: optimizeRepository,
		repositoryStates: repositoryStates{
			values: make(map[string]*refCountedState),
		},
	}
}

// Describe is used to describe Prometheus metrics.
func (m *RepositoryManager) Describe(descs chan<- *prometheus.Desc) {
	prometheus.DescribeByCollect(m, descs)
}

// Collect is used to collect Prometheus metrics.
func (m *RepositoryManager) Collect(metrics chan<- prometheus.Metric) {
	m.tasksTotal.Collect(metrics)
	m.tasksLatency.Collect(metrics)
	m.prunedFilesTotal.Collect(metrics)
	m.dataStructureExistence.Collect(metrics)
	m.dataStructureCount.Collect(metrics)
	m.dataStructureSize.Collect(metrics)
	m.dataStructureTimeSinceLastOptimization.Collect(metrics)
}

// AddPackRefsInhibitor exposes the internal function addPackRefsInhibitor on the
// RepositoryManager level. This can then be used by other clients to block housekeeping
// from running git-pack-refs(1).
func (m *RepositoryManager) AddPackRefsInhibitor(ctx context.Context, repoPath string) (successful bool, _ func(), err error) {
	return m.repositoryStates.addPackRefsInhibitor(ctx, repoPath)
}
