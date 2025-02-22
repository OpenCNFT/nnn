package datastore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v16/internal/log"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper/testdb"
)

func TestRepositoryStoreCollector(t *testing.T) {
	t.Parallel()
	ctx := testhelper.Context(t)

	type replicas map[string]struct {
		generation int
		assigned   bool
	}

	type repositories []struct {
		deleted      bool
		relativePath string
		replicas     replicas
	}

	db := testdb.New(t)

	for _, tc := range []struct {
		desc         string
		healthyNodes []string
		repositories repositories
		timeout      bool
		count        int
		error        error
	}{
		{
			desc: "no repositories",
		},
		{
			desc: "deleted repositories are not considered unavailable",
			repositories: repositories{
				{
					deleted:      true,
					relativePath: "repository-1",
					replicas: replicas{
						"storage-1": {generation: 0},
					},
				},
			},
		},
		{
			desc: "repositories without any healthy replicas are counted",
			repositories: repositories{
				{
					relativePath: "repository-1",
					replicas: replicas{
						"storage-1": {generation: 0},
					},
				},
			},
			count: 1,
		},
		{
			desc:         "repositories with healthy replicas are not counted",
			healthyNodes: []string{"storage-1"},
			repositories: repositories{
				{
					relativePath: "repository-1",
					replicas: replicas{
						"storage-1": {generation: 0},
					},
				},
			},
		},
		{
			desc:         "repositories with only outdated healthy replicas are counted",
			healthyNodes: []string{"storage-1"},
			repositories: repositories{
				{
					relativePath: "repository-1",
					replicas: replicas{
						"storage-1": {generation: 0},
						"storage-2": {generation: 1},
					},
				},
			},
			count: 1,
		},
		{
			desc:         "repositories with unassigned fully up to date healthy replicas are not counted",
			healthyNodes: []string{"storage-2"},
			repositories: repositories{
				{
					relativePath: "repository-1",
					replicas: replicas{
						"storage-1": {generation: 1, assigned: true},
						"storage-2": {generation: 1},
					},
				},
			},
		},
		{
			desc:         "repositories with unassigned, outdated replicas is not unavailable",
			healthyNodes: []string{"storage-1"},
			repositories: repositories{
				{
					relativePath: "repository-1",
					replicas: replicas{
						"storage-1": {generation: 1, assigned: true},
						"storage-2": {generation: 0},
					},
				},
			},
		},
		{
			desc:         "multiple unavailable repositories are counted correctly",
			healthyNodes: []string{"storage-2"},
			repositories: repositories{
				{
					relativePath: "repository-1",
					replicas: replicas{
						"storage-1": {generation: 1},
						"storage-2": {generation: 0},
					},
				},
				{
					relativePath: "repository-2",
					replicas: replicas{
						"storage-1": {generation: 1},
						"storage-2": {generation: 0},
					},
				},
			},
			count: 2,
		},
		{
			desc:    "query timeout",
			timeout: true,
			error:   fmt.Errorf("query: %w", context.DeadlineExceeded),
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			tx := db.Begin(t)
			defer tx.Rollback(t)

			testdb.SetHealthyNodes(t, ctx, tx, map[string]map[string][]string{
				"praefect-0": {"virtual-storage-1": tc.healthyNodes},
			})

			for _, repository := range tc.repositories {

				rs := NewPostgresRepositoryStore(tx, nil)

				var repositoryID int64
				for storage, replica := range repository.replicas {
					if repositoryID == 0 {
						const virtualStorage = "virtual-storage-1"

						var err error
						repositoryID, err = rs.ReserveRepositoryID(ctx, virtualStorage, repository.relativePath)
						require.NoError(t, err)

						require.NoError(t, rs.CreateRepository(ctx, repositoryID, virtualStorage, repository.relativePath, repository.relativePath, storage, nil, nil, false, false))
					}

					if replica.assigned {
						_, err := tx.ExecContext(ctx, `
							INSERT INTO repository_assignments (repository_id, virtual_storage, relative_path, storage)
							VALUES ($1, 'virtual-storage-1', $2, $3)
						`, repositoryID, repository.relativePath, storage,
						)
						require.NoError(t, err)
					}

					require.NoError(t, rs.SetGeneration(ctx, repositoryID, storage, repository.relativePath, replica.generation))
				}

				if repository.deleted {
					_, err := tx.ExecContext(ctx, `
						DELETE FROM repositories WHERE repository_id = $1
					`, repositoryID,
					)
					require.NoError(t, err)
				}
			}

			timeout := time.Hour
			if tc.timeout {
				timeout = 0
			}

			logger := testhelper.NewLogger(t)
			hook := testhelper.AddLoggerHook(logger)

			c := NewRepositoryStoreCollector(logger, []string{"virtual-storage-1", "virtual-storage-2"}, tx, timeout)
			err := testhelper.ComparePromMetrics(t, c, fmt.Sprintf(`
# HELP gitaly_praefect_unavailable_repositories Number of repositories that have no healthy, up to date replicas.
# TYPE gitaly_praefect_unavailable_repositories gauge
gitaly_praefect_unavailable_repositories{virtual_storage="virtual-storage-1"} %d
gitaly_praefect_unavailable_repositories{virtual_storage="virtual-storage-2"} 0
			`, tc.count))

			if tc.error != nil {
				require.Equal(t, "failed collecting unavailable repository count metric", hook.AllEntries()[0].Message)
				require.Equal(t, log.Fields{"error": tc.error, "component": "RepositoryStoreCollector"}, hook.AllEntries()[0].Data)
				return
			}

			require.NoError(t, err)
		})
	}
}

type checkIfQueriedDB struct {
	queried bool
}

func (c *checkIfQueriedDB) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	c.queried = true
	return nil, errors.New("QueryContext should not be called")
}

func (c *checkIfQueriedDB) QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row {
	c.queried = true
	return &sql.Row{}
}

func (c *checkIfQueriedDB) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	c.queried = true
	return nil, errors.New("ExecContext should not be called")
}

func TestRepositoryStoreCollector_CollectNotCalledOnRegister(t *testing.T) {
	logger := testhelper.NewLogger(t)

	var db checkIfQueriedDB
	c := NewRepositoryStoreCollector(logger, []string{"virtual-storage-1", "virtual-storage-2"}, &db, 2*time.Second)
	registry := prometheus.NewRegistry()
	registry.MustRegister(c)

	assert.False(t, db.queried)
}

func TestRepositoryStoreCollector_ReplicationQueueDepth(t *testing.T) {
	db := testdb.New(t)
	ctx := testhelper.Context(t)

	log := testhelper.SharedLogger(t)

	storageNames := map[string][]string{
		"praefect-0": {"storage-0", "storage-1", "storage-2"},
		"praefect-1": {"storage-3", "storage-4", "storage-5"},
	}
	queue := NewPostgresReplicationEventQueue(db.DB)

	readyJobs := 5
	for virtualStorage, nodes := range storageNames {
		for i := 0; i < readyJobs; i++ {
			_, err := queue.Enqueue(ctx, ReplicationEvent{
				Job: ReplicationJob{
					Change:            UpdateRepo,
					RelativePath:      fmt.Sprintf("/project/path-%d", i),
					TargetNodeStorage: nodes[1],
					SourceNodeStorage: nodes[0],
					VirtualStorage:    virtualStorage,
					RepositoryID:      int64(i),
					Params:            nil,
				},
			})
			require.NoError(t, err)
		}
	}

	collector := NewQueueDepthCollector(log, db, 1*time.Hour)

	testhelper.RequirePromMetrics(t, collector, fmt.Sprintf(`
# HELP gitaly_praefect_replication_queue_depth Number of jobs in the replication queue
# TYPE gitaly_praefect_replication_queue_depth gauge
gitaly_praefect_replication_queue_depth{state="ready",target_node="storage-1",virtual_storage="praefect-0"} %d
gitaly_praefect_replication_queue_depth{state="ready",target_node="storage-4",virtual_storage="praefect-1"} %d
`, readyJobs, readyJobs))

	var eventIDs []uint64
	events, err := queue.Dequeue(ctx, "praefect-0", "storage-1", 1)
	require.NoError(t, err)
	require.Len(t, events, 1)
	eventIDs = append(eventIDs, events[0].ID)

	events, err = queue.Dequeue(ctx, "praefect-1", "storage-4", 1)
	require.NoError(t, err)
	require.Len(t, events, 1)
	eventIDs = append(eventIDs, events[0].ID)

	testhelper.RequirePromMetrics(t, collector, fmt.Sprintf(`
# HELP gitaly_praefect_replication_queue_depth Number of jobs in the replication queue
# TYPE gitaly_praefect_replication_queue_depth gauge
gitaly_praefect_replication_queue_depth{state="in_progress",target_node="storage-1",virtual_storage="praefect-0"} %d
gitaly_praefect_replication_queue_depth{state="in_progress",target_node="storage-4",virtual_storage="praefect-1"} %d
gitaly_praefect_replication_queue_depth{state="ready",target_node="storage-1",virtual_storage="praefect-0"} %d
gitaly_praefect_replication_queue_depth{state="ready",target_node="storage-4",virtual_storage="praefect-1"} %d
`, 1, 1, readyJobs-1, readyJobs-1))

	_, err = queue.Acknowledge(ctx, JobStateFailed, eventIDs)
	require.NoError(t, err)

	testhelper.RequirePromMetrics(t, collector, fmt.Sprintf(`
# HELP gitaly_praefect_replication_queue_depth Number of jobs in the replication queue
# TYPE gitaly_praefect_replication_queue_depth gauge
gitaly_praefect_replication_queue_depth{state="failed",target_node="storage-1",virtual_storage="praefect-0"} %d
gitaly_praefect_replication_queue_depth{state="failed",target_node="storage-4",virtual_storage="praefect-1"} %d
gitaly_praefect_replication_queue_depth{state="ready",target_node="storage-1",virtual_storage="praefect-0"} %d
gitaly_praefect_replication_queue_depth{state="ready",target_node="storage-4",virtual_storage="praefect-1"} %d
`, 1, 1, readyJobs-1, readyJobs-1))
}

func TestVerificationQueueDepthCollector(t *testing.T) {
	ctx := testhelper.Context(t)

	tx := testdb.New(t).Begin(t)
	defer tx.Rollback(t)

	rs := NewPostgresRepositoryStore(tx, nil)
	require.NoError(t,
		rs.CreateRepository(ctx, 1, "virtual-storage-1", "relative-path-1", "replica-path-1", "gitaly-1", []string{"gitaly-2", "gitaly-3"}, nil, true, false),
	)
	require.NoError(t,
		rs.CreateRepository(ctx, 2, "virtual-storage-1", "relative-path-2", "replica-path-2", "gitaly-1", []string{"gitaly-2", "gitaly-3"}, nil, true, false),
	)
	require.NoError(t,
		rs.CreateRepository(ctx, 3, "virtual-storage-2", "relative-path-1", "replica-path-3", "gitaly-1", []string{"gitaly-2", "gitaly-3"}, nil, true, false),
	)

	_, err := tx.ExecContext(ctx, `
UPDATE storage_repositories
SET verified_at = CASE
	WHEN storage = 'gitaly-2' THEN now() - '30 seconds'::interval
	ELSE now() - '30 seconds'::interval - '1 microsecond'::interval
END
WHERE virtual_storage = 'virtual-storage-1' AND storage != 'gitaly-1'
	`)
	require.NoError(t, err)

	logger := testhelper.NewLogger(t)
	hook := testhelper.AddLoggerHook(logger)
	testhelper.RequirePromMetrics(
		t,
		NewVerificationQueueDepthCollector(logger, tx, time.Minute, 30*time.Second, map[string][]string{
			"virtual-storage-1": {"gitaly-1", "gitaly-2", "gitaly-3"},
			"virtual-storage-2": {"gitaly-1", "gitaly-2", "gitaly-3"},
		}),
		`
# HELP gitaly_praefect_verification_queue_depth Number of replicas pending verification.
# TYPE gitaly_praefect_verification_queue_depth gauge
gitaly_praefect_verification_queue_depth{status="expired",storage="gitaly-1",virtual_storage="virtual-storage-1"} 0
gitaly_praefect_verification_queue_depth{status="expired",storage="gitaly-1",virtual_storage="virtual-storage-2"} 0
gitaly_praefect_verification_queue_depth{status="expired",storage="gitaly-2",virtual_storage="virtual-storage-1"} 0
gitaly_praefect_verification_queue_depth{status="expired",storage="gitaly-2",virtual_storage="virtual-storage-2"} 0
gitaly_praefect_verification_queue_depth{status="expired",storage="gitaly-3",virtual_storage="virtual-storage-1"} 2
gitaly_praefect_verification_queue_depth{status="expired",storage="gitaly-3",virtual_storage="virtual-storage-2"} 0
gitaly_praefect_verification_queue_depth{status="unverified",storage="gitaly-1",virtual_storage="virtual-storage-1"} 2
gitaly_praefect_verification_queue_depth{status="unverified",storage="gitaly-1",virtual_storage="virtual-storage-2"} 1
gitaly_praefect_verification_queue_depth{status="unverified",storage="gitaly-2",virtual_storage="virtual-storage-1"} 0
gitaly_praefect_verification_queue_depth{status="unverified",storage="gitaly-2",virtual_storage="virtual-storage-2"} 1
gitaly_praefect_verification_queue_depth{status="unverified",storage="gitaly-3",virtual_storage="virtual-storage-1"} 0
gitaly_praefect_verification_queue_depth{status="unverified",storage="gitaly-3",virtual_storage="virtual-storage-2"} 1
		`,
	)
	require.Empty(t, hook.AllEntries())
}
