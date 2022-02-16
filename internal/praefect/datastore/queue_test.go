package datastore

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/datastore/glsql"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper/testdb"
)

func TestPostgresReplicationEventQueue_DeleteReplicaUniqueIndex(t *testing.T) {
	t.Parallel()
	db := testdb.New(t)
	for _, tc := range []struct {
		desc        string
		existingJob *ReplicationEvent
		succeeds    bool
	}{
		{
			desc:     "allowed when no events",
			succeeds: true,
		},
		{
			desc: "allowed if existing completed job",
			existingJob: &ReplicationEvent{
				State: JobStateCompleted,
				Job: ReplicationJob{
					Change:         DeleteReplica,
					VirtualStorage: "praefect",
					RelativePath:   "relative-path",
				},
			},
			succeeds: true,
		},
		{
			desc: "allowed if existing dead job",
			existingJob: &ReplicationEvent{
				State: JobStateDead,
				Job: ReplicationJob{
					Change:         DeleteReplica,
					VirtualStorage: "praefect",
					RelativePath:   "relative-path",
				},
			},
			succeeds: true,
		},
		{
			desc: "allowed if existing different virtual storage",
			existingJob: &ReplicationEvent{
				State: JobStateReady,
				Job: ReplicationJob{
					Change:         DeleteReplica,
					VirtualStorage: "wrong-virtual-storage",
					RelativePath:   "relative-path",
				},
			},
			succeeds: true,
		},
		{
			desc: "allowed if existing different relative path",
			existingJob: &ReplicationEvent{
				State: JobStateReady,
				Job: ReplicationJob{
					Change:         DeleteReplica,
					VirtualStorage: "praefect",
					RelativePath:   "wrong-relative-path",
				},
			},
			succeeds: true,
		},
		{
			desc: "not allowed if existing ready job",
			existingJob: &ReplicationEvent{
				State: JobStateReady,
				Job: ReplicationJob{
					Change:         DeleteReplica,
					VirtualStorage: "praefect",
					RelativePath:   "relative-path",
				},
			},
		},
		{
			desc: "not allowed if existing in_progress job",
			existingJob: &ReplicationEvent{
				State: JobStateInProgress,
				Job: ReplicationJob{
					Change:         DeleteReplica,
					VirtualStorage: "praefect",
					RelativePath:   "relative-path",
				},
			},
		},
		{
			desc: "not allowed if existing failed job",
			existingJob: &ReplicationEvent{
				State: JobStateFailed,
				Job: ReplicationJob{
					Change:         DeleteReplica,
					VirtualStorage: "praefect",
					RelativePath:   "relative-path",
				},
			},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			db.TruncateAll(t)
			ctx := testhelper.Context(t)

			if tc.existingJob != nil {
				insertRepository(t, db, ctx, tc.existingJob.Job.VirtualStorage, tc.existingJob.Job.RelativePath, "stub")
				_, err := db.ExecContext(ctx, `
					INSERT INTO replication_queue (state, job)
					VALUES ($1, $2)
				`, tc.existingJob.State, tc.existingJob.Job)
				require.NoError(t, err)
			}

			job := ReplicationJob{
				Change:            DeleteReplica,
				VirtualStorage:    "praefect",
				RelativePath:      "relative-path",
				TargetNodeStorage: "gitaly-1",
			}
			insertRepository(t, db, ctx, job.VirtualStorage, job.RelativePath, "stub")
			_, err := NewPostgresReplicationEventQueue(db).Enqueue(ctx, ReplicationEvent{
				State: JobStateReady,
				Job:   job,
			})

			if tc.succeeds {
				require.NoError(t, err)
				return
			}

			require.EqualError(t, err, `query: ERROR: duplicate key value violates unique constraint "delete_replica_unique_index" (SQLSTATE 23505)`)
		})
	}
}

func TestPostgresReplicationEventQueue_Enqueue(t *testing.T) {
	t.Parallel()
	db := testdb.New(t)
	ctx := testhelper.Context(t)

	queue := PostgresReplicationEventQueue{db.DB}

	eventType := ReplicationEvent{
		Job: ReplicationJob{
			Change:            UpdateRepo,
			RelativePath:      "/project/path-1",
			TargetNodeStorage: "gitaly-1",
			SourceNodeStorage: "gitaly-0",
			VirtualStorage:    "praefect",
			Params:            nil,
		},
	}
	repositoryID := insertRepository(t, db, ctx, eventType.Job.VirtualStorage, eventType.Job.RelativePath, eventType.Job.SourceNodeStorage)

	actualEvent, err := queue.Enqueue(ctx, eventType) // initial event
	require.NoError(t, err)
	actualEvent.CreatedAt = time.Time{} // we need to setup it to default because it is not possible to get it beforehand for expected

	expLock := LockRow{ID: "praefect|gitaly-1|/project/path-1", Acquired: false}

	expEvent := ReplicationEvent{
		ID:      1,
		State:   JobStateReady,
		Attempt: 3,
		LockID:  "praefect|gitaly-1|/project/path-1",
		Job: ReplicationJob{
			RepositoryID:      repositoryID,
			Change:            UpdateRepo,
			RelativePath:      "/project/path-1",
			TargetNodeStorage: "gitaly-1",
			SourceNodeStorage: "gitaly-0",
			VirtualStorage:    "praefect",
			Params:            nil,
		},
	}

	require.Equal(t, expEvent, actualEvent)
	requireEvents(t, ctx, db, []ReplicationEvent{expEvent})
	requireLocks(t, ctx, db, []LockRow{expLock}) // expected a new lock for new event
	db.RequireRowsInTable(t, "replication_queue_job_lock", 0)
}

func TestPostgresReplicationEventQueue_Enqueue_triggerPopulatesColumns(t *testing.T) {
	t.Parallel()
	db := testdb.New(t)
	ctx := testhelper.Context(t)

	t.Run("no repository record exists", func(t *testing.T) {
		tx := db.Begin(t)
		defer tx.Rollback(t)

		job := ReplicationJob{
			Change:            UpdateRepo,
			RepositoryID:      1,
			RelativePath:      "/project/path-1",
			ReplicaPath:       "relative/project/path-1",
			TargetNodeStorage: "gitaly-1",
			SourceNodeStorage: "gitaly-0",
			VirtualStorage:    "praefect",
			Params:            nil,
		}
		queue := NewPostgresReplicationEventQueue(tx)
		event := ReplicationEvent{Job: job}
		_, err := queue.Enqueue(ctx, event)
		ok := glsql.IsForeignKeyViolation(err, "replication_queue_repository_id_fkey")
		require.Truef(t, ok, "returned error is not expected: %+v", err)
	})

	t.Run("repository id not set on job, but found in repositories table", func(t *testing.T) {
		tx := db.Begin(t)
		defer tx.Rollback(t)

		job := ReplicationJob{
			Change:            RenameRepo,
			RelativePath:      "/project/path-1",
			ReplicaPath:       "relative/project/path-1",
			TargetNodeStorage: "gitaly-1",
			SourceNodeStorage: "gitaly-0",
			VirtualStorage:    "praefect",
			Params:            Params{"RelativePath": "new/path"},
		}
		repositoryID := insertRepository(t, tx, ctx, job.VirtualStorage, job.RelativePath, job.SourceNodeStorage)
		queue := NewPostgresReplicationEventQueue(tx)
		event := ReplicationEvent{Job: job}
		enqueued, err := queue.Enqueue(ctx, event)
		require.NoError(t, err)

		actual := extractReplicationJob(t, ctx, tx, enqueued.ID)
		job.RepositoryID = repositoryID
		require.Equal(t, job, actual)
	})

	t.Run("repository id set on job", func(t *testing.T) {
		tx := db.Begin(t)
		defer tx.Rollback(t)

		job := ReplicationJob{
			Change:            RenameRepo,
			RelativePath:      "/project/path-1",
			ReplicaPath:       "relative/project/path-1",
			TargetNodeStorage: "gitaly-1",
			SourceNodeStorage: "gitaly-0",
			VirtualStorage:    "praefect",
			Params:            Params{"RelativePath": "new/path"},
		}
		job.RepositoryID = insertRepository(t, tx, ctx, job.VirtualStorage, job.RelativePath, job.SourceNodeStorage)

		queue := NewPostgresReplicationEventQueue(tx)
		event := ReplicationEvent{Job: job}
		enqueued, err := queue.Enqueue(ctx, event)
		require.NoError(t, err)

		actual := extractReplicationJob(t, ctx, tx, enqueued.ID)
		require.Equal(t, job, actual)
	})
}

func extractReplicationJob(t *testing.T, ctx context.Context, tx *testdb.TxWrapper, id uint64) ReplicationJob {
	t.Helper()
	const selectJob = `
			SELECT
			       change, repository_id, replica_path, relative_path, target_node_storage,
			       source_node_storage, virtual_storage, params
			FROM replication_queue WHERE id = $1`
	var job ReplicationJob
	require.NoError(t, tx.QueryRowContext(ctx, selectJob, id).Scan(
		&job.Change, &job.RepositoryID, &job.ReplicaPath, &job.RelativePath, &job.TargetNodeStorage,
		&job.SourceNodeStorage, &job.VirtualStorage, &job.Params,
	))
	return job
}

func TestPostgresReplicationEventQueue_DeleteReplicaInfiniteAttempts(t *testing.T) {
	t.Parallel()
	db := testdb.New(t)
	queue := NewPostgresReplicationEventQueue(db)
	ctx := testhelper.Context(t)

	job := ReplicationJob{
		Change:            DeleteReplica,
		RelativePath:      "/project/path-1",
		TargetNodeStorage: "gitaly-1",
		VirtualStorage:    "praefect",
	}
	job.RepositoryID = insertRepository(t, db, ctx, job.VirtualStorage, job.RelativePath, "stub")
	actualEvent, err := queue.Enqueue(ctx, ReplicationEvent{
		Job: job,
	})
	require.NoError(t, err)

	expectedEvent := ReplicationEvent{
		ID:      1,
		State:   JobStateReady,
		Attempt: 3,
		LockID:  "praefect|gitaly-1|/project/path-1",
		Job: ReplicationJob{
			RepositoryID:      job.RepositoryID,
			Change:            DeleteReplica,
			RelativePath:      "/project/path-1",
			TargetNodeStorage: "gitaly-1",
			VirtualStorage:    "praefect",
			Params:            nil,
		},
		CreatedAt: actualEvent.CreatedAt,
	}

	require.Equal(t, expectedEvent, actualEvent)

	for i := 0; i < 2*actualEvent.Attempt; i++ {
		actualEvents, err := queue.Dequeue(ctx, "praefect", "gitaly-1", 9999)
		require.NoError(t, err)
		require.Len(t, actualEvents, 1)

		expectedEvent.State = JobStateInProgress
		expectedEvent.UpdatedAt = actualEvents[0].UpdatedAt

		require.Equal(t, expectedEvent, actualEvents[0])

		eventIDs := []uint64{actualEvent.ID}
		ackedIDs, err := queue.Acknowledge(ctx, JobStateFailed, eventIDs)
		require.NoError(t, err)
		require.Equal(t, eventIDs, ackedIDs)
	}
}

func TestPostgresReplicationEventQueue_EnqueueMultiple(t *testing.T) {
	t.Parallel()
	db := testdb.New(t)
	ctx := testhelper.Context(t)

	queue := PostgresReplicationEventQueue{db.DB}

	eventType1 := ReplicationEvent{
		Job: ReplicationJob{
			Change:            UpdateRepo,
			RelativePath:      "/project/path-1",
			TargetNodeStorage: "gitaly-1",
			SourceNodeStorage: "gitaly-0",
			VirtualStorage:    "praefect-0",
			Params:            nil,
		},
	}

	eventType2 := ReplicationEvent{
		Job: ReplicationJob{
			Change:            RenameRepo,
			RelativePath:      "/project/path-1",
			TargetNodeStorage: "gitaly-2",
			SourceNodeStorage: "",
			VirtualStorage:    "praefect-0",
			Params:            Params{"RelativePath": "/project/path-1-renamed"},
		},
	}

	eventType3 := ReplicationEvent{
		Job: ReplicationJob{
			Change:            UpdateRepo,
			RelativePath:      "/project/path-2",
			TargetNodeStorage: "gitaly-1",
			SourceNodeStorage: "gitaly-0",
			VirtualStorage:    "praefect-1",
			Params:            nil,
		},
	}

	eventType1.Job.RepositoryID = insertRepository(t, db, ctx, eventType1.Job.VirtualStorage, eventType1.Job.RelativePath, eventType1.Job.SourceNodeStorage)
	event1, err := queue.Enqueue(ctx, eventType1) // initial event
	require.NoError(t, err)

	expLock1 := LockRow{ID: "praefect-0|gitaly-1|/project/path-1", Acquired: false}
	expLock2 := LockRow{ID: "praefect-0|gitaly-2|/project/path-1", Acquired: false}
	expLock3 := LockRow{ID: "praefect-1|gitaly-1|/project/path-2", Acquired: false}

	expEvent1 := ReplicationEvent{
		ID:      event1.ID,
		State:   "ready",
		Attempt: 3,
		LockID:  "praefect-0|gitaly-1|/project/path-1",
		Job: ReplicationJob{
			Change:            UpdateRepo,
			RepositoryID:      eventType1.Job.RepositoryID,
			RelativePath:      "/project/path-1",
			TargetNodeStorage: "gitaly-1",
			SourceNodeStorage: "gitaly-0",
			VirtualStorage:    "praefect-0",
			Params:            nil,
		},
	}

	requireEvents(t, ctx, db, []ReplicationEvent{expEvent1})
	requireLocks(t, ctx, db, []LockRow{expLock1}) // expected a new lock for new event
	db.RequireRowsInTable(t, "replication_queue_job_lock", 0)

	event2, err := queue.Enqueue(ctx, eventType1) // repeat of the same event
	require.NoError(t, err)

	expEvent2 := ReplicationEvent{
		ID:      event2.ID,
		State:   "ready",
		Attempt: 3,
		LockID:  "praefect-0|gitaly-1|/project/path-1",
		Job: ReplicationJob{
			Change:            UpdateRepo,
			RepositoryID:      eventType1.Job.RepositoryID,
			RelativePath:      "/project/path-1",
			TargetNodeStorage: "gitaly-1",
			SourceNodeStorage: "gitaly-0",
			VirtualStorage:    "praefect-0",
			Params:            nil,
		},
	}

	requireEvents(t, ctx, db, []ReplicationEvent{expEvent1, expEvent2})
	requireLocks(t, ctx, db, []LockRow{expLock1}) // expected still one the same lock for repeated event

	event3, err := queue.Enqueue(ctx, eventType2) // event for another target
	require.NoError(t, err)

	expEvent3 := ReplicationEvent{
		ID:      event3.ID,
		State:   JobStateReady,
		Attempt: 3,
		LockID:  "praefect-0|gitaly-2|/project/path-1",
		Job: ReplicationJob{
			Change:            RenameRepo,
			RepositoryID:      eventType1.Job.RepositoryID,
			RelativePath:      "/project/path-1",
			TargetNodeStorage: "gitaly-2",
			SourceNodeStorage: "",
			VirtualStorage:    "praefect-0",
			Params:            Params{"RelativePath": "/project/path-1-renamed"},
		},
	}

	requireEvents(t, ctx, db, []ReplicationEvent{expEvent1, expEvent2, expEvent3})
	requireLocks(t, ctx, db, []LockRow{expLock1, expLock2}) // the new lock for another target repeated event

	eventType3.Job.RepositoryID = insertRepository(t, db, ctx, eventType3.Job.VirtualStorage, eventType3.Job.RelativePath, eventType3.Job.SourceNodeStorage)
	event4, err := queue.Enqueue(ctx, eventType3) // event for another repo
	require.NoError(t, err)

	expEvent4 := ReplicationEvent{
		ID:      event4.ID,
		State:   JobStateReady,
		Attempt: 3,
		LockID:  "praefect-1|gitaly-1|/project/path-2",
		Job: ReplicationJob{
			Change:            UpdateRepo,
			RepositoryID:      eventType3.Job.RepositoryID,
			RelativePath:      "/project/path-2",
			TargetNodeStorage: "gitaly-1",
			SourceNodeStorage: "gitaly-0",
			VirtualStorage:    "praefect-1",
			Params:            nil,
		},
	}

	requireEvents(t, ctx, db, []ReplicationEvent{expEvent1, expEvent2, expEvent3, expEvent4})
	requireLocks(t, ctx, db, []LockRow{expLock1, expLock2, expLock3}) // the new lock for same target but for another repo

	db.RequireRowsInTable(t, "replication_queue_job_lock", 0) // there is no fetches it must be empty
}

func TestPostgresReplicationEventQueue_Dequeue(t *testing.T) {
	t.Parallel()
	db := testdb.New(t)
	ctx := testhelper.Context(t)

	queue := PostgresReplicationEventQueue{db.DB}

	event := ReplicationEvent{
		Job: ReplicationJob{
			Change:            UpdateRepo,
			RelativePath:      "/project/path-1",
			TargetNodeStorage: "gitaly-1",
			SourceNodeStorage: "gitaly-0",
			VirtualStorage:    "praefect",
			Params:            nil,
		},
	}
	event.Job.RepositoryID = insertRepository(t, db, ctx, event.Job.VirtualStorage, event.Job.RelativePath, event.Job.SourceNodeStorage)

	event, err := queue.Enqueue(ctx, event)
	require.NoError(t, err, "failed to fill in event queue")

	noEvents, err := queue.Dequeue(ctx, "praefect", "not existing storage", 5)
	require.NoError(t, err)
	require.Len(t, noEvents, 0, "there must be no events dequeued for not existing storage")

	expectedEvent := event
	expectedEvent.State = JobStateInProgress
	expectedEvent.Attempt = 2

	expectedLock := LockRow{ID: event.LockID, Acquired: true} // as we deque events we acquire lock for processing

	expectedJobLock := JobLockRow{JobID: event.ID, LockID: event.LockID} // and there is a track if job is under processing in separate table

	actual, err := queue.Dequeue(ctx, event.Job.VirtualStorage, event.Job.TargetNodeStorage, 5)
	require.NoError(t, err)

	for i := range actual {
		actual[i].UpdatedAt = nil // it is not possible to determine update_at value as it is generated on UPDATE in database
	}
	require.Equal(t, []ReplicationEvent{expectedEvent}, actual)

	// there is only one single lock for all fetched events
	requireLocks(t, ctx, db, []LockRow{expectedLock})
	requireJobLocks(t, ctx, db, []JobLockRow{expectedJobLock})
}

// expected results are listed as literals on purpose to be more explicit about what is going on with data
func TestPostgresReplicationEventQueue_DequeueMultiple(t *testing.T) {
	t.Parallel()
	db := testdb.New(t)
	ctx := testhelper.Context(t)

	queue := PostgresReplicationEventQueue{db.DB}

	eventType1 := ReplicationEvent{
		Job: ReplicationJob{
			Change:            UpdateRepo,
			RelativePath:      "/project/path-1",
			TargetNodeStorage: "gitaly-1",
			SourceNodeStorage: "gitaly-0",
			VirtualStorage:    "praefect",
			Params:            nil,
		},
	}

	eventType2 := ReplicationEvent{
		Job: ReplicationJob{
			Change:            DeleteRepo,
			RelativePath:      "/project/path-1",
			TargetNodeStorage: "gitaly-1",
			SourceNodeStorage: "",
			VirtualStorage:    "praefect",
			Params:            nil,
		},
	}

	eventType3 := ReplicationEvent{
		Job: ReplicationJob{
			Change:            RenameRepo,
			RelativePath:      "/project/path-2",
			TargetNodeStorage: "gitaly-1",
			SourceNodeStorage: "gitaly-0",
			VirtualStorage:    "praefect",
			Params:            Params{"RelativePath": "/project/path-2-renamed"},
		},
	}

	eventType4 := ReplicationEvent{
		Job: ReplicationJob{
			Change:            UpdateRepo,
			RelativePath:      "/project/path-1",
			TargetNodeStorage: "gitaly-1",
			SourceNodeStorage: "gitaly-0",
			VirtualStorage:    "backup",
		},
	}

	// events to fill in the queue
	events := []ReplicationEvent{eventType1, eventType1, eventType2, eventType1, eventType3, eventType4}
	for i := range events {
		events[i].Job.RepositoryID = insertRepository(t, db, ctx, events[i].Job.VirtualStorage, events[i].Job.RelativePath, events[i].Job.SourceNodeStorage)

		var err error
		events[i], err = queue.Enqueue(ctx, events[i])
		require.NoError(t, err, "failed to fill in event queue")
	}

	// first request to deque
	expectedEvents1 := []ReplicationEvent{events[0], events[2], events[4]}
	expectedJobLocks1 := []JobLockRow{
		{JobID: events[0].ID, LockID: "praefect|gitaly-1|/project/path-1"},
		{JobID: events[2].ID, LockID: "praefect|gitaly-1|/project/path-1"},
		{JobID: events[4].ID, LockID: "praefect|gitaly-1|/project/path-2"},
	}

	// we expect only first two types of events by limiting count to 3
	dequeuedEvents1, err := queue.Dequeue(ctx, "praefect", "gitaly-1", 3)
	require.NoError(t, err)
	require.Len(t, dequeuedEvents1, len(expectedEvents1))
	for i := range dequeuedEvents1 {
		dequeuedEvents1[i].UpdatedAt = nil // it is not possible to determine update_at value as it is generated on UPDATE in database
		expectedEvents1[i].State = JobStateInProgress
		expectedEvents1[i].Attempt--
	}
	require.Equal(t, expectedEvents1, dequeuedEvents1)

	requireLocks(t, ctx, db, []LockRow{
		// there is only one single lock for all fetched events because of their 'repo' and 'target' combination
		{ID: "praefect|gitaly-1|/project/path-1", Acquired: true},
		{ID: "praefect|gitaly-1|/project/path-2", Acquired: true},
		{ID: "backup|gitaly-1|/project/path-1", Acquired: false},
	})
	requireJobLocks(t, ctx, db, expectedJobLocks1)

	// second request to deque
	// there must be only last event fetched from the queue
	expectedEvents2 := []ReplicationEvent{events[5]}
	expectedEvents2[0].State = JobStateInProgress
	expectedEvents2[0].Attempt = 2

	expectedJobLocks2 := []JobLockRow{{JobID: 6, LockID: "backup|gitaly-1|/project/path-1"}}

	dequeuedEvents2, err := queue.Dequeue(ctx, "backup", "gitaly-1", 100500)
	require.NoError(t, err)
	require.Len(t, dequeuedEvents2, 1, "only one event must be fetched from the queue")

	dequeuedEvents2[0].UpdatedAt = nil // it is not possible to determine update_at value as it is generated on UPDATE in database
	require.Equal(t, expectedEvents2, dequeuedEvents2)

	requireLocks(t, ctx, db, []LockRow{
		{ID: "praefect|gitaly-1|/project/path-1", Acquired: true},
		{ID: "praefect|gitaly-1|/project/path-2", Acquired: true},
		{ID: "backup|gitaly-1|/project/path-1", Acquired: true},
	})
	requireJobLocks(t, ctx, db, append(expectedJobLocks1, expectedJobLocks2...))
}

func TestPostgresReplicationEventQueue_DequeueSameStorageOtherRepository(t *testing.T) {
	t.Parallel()
	db := testdb.New(t)
	ctx := testhelper.Context(t)

	queue := PostgresReplicationEventQueue{db.DB}

	eventType1 := ReplicationEvent{
		Job: ReplicationJob{
			Change:            UpdateRepo,
			RelativePath:      "/project/path-1",
			TargetNodeStorage: "gitaly-1",
			SourceNodeStorage: "gitaly-0",
			VirtualStorage:    "praefect",
			Params:            nil,
		},
	}

	eventType2 := ReplicationEvent{
		Job: ReplicationJob{
			Change:            UpdateRepo,
			RelativePath:      "/project/path-2",
			TargetNodeStorage: "gitaly-1",
			SourceNodeStorage: "gitaly-0",
			VirtualStorage:    "praefect",
			Params:            nil,
		},
	}

	for i := 0; i < 2; i++ {
		eventType1.Job.RepositoryID = insertRepository(t, db, ctx, eventType1.Job.VirtualStorage, eventType1.Job.RelativePath, eventType1.Job.SourceNodeStorage)
		_, err := queue.Enqueue(ctx, eventType1)
		require.NoError(t, err, "failed to fill in event queue")
	}

	dequeuedEvents1, err := queue.Dequeue(ctx, "praefect", "gitaly-1", 1)
	require.NoError(t, err)
	require.Len(t, dequeuedEvents1, 1)
	requireLocks(t, ctx, db, []LockRow{
		// there is only one single lock for all fetched events because of their 'repo' and 'target' combination
		{ID: "praefect|gitaly-1|/project/path-1", Acquired: true},
	})
	requireJobLocks(t, ctx, db, []JobLockRow{{JobID: 1, LockID: "praefect|gitaly-1|/project/path-1"}})

	for i := 0; i < 2; i++ {
		eventType2.Job.RepositoryID = insertRepository(t, db, ctx, eventType2.Job.VirtualStorage, eventType2.Job.RelativePath, eventType2.Job.SourceNodeStorage)
		_, err := queue.Enqueue(ctx, eventType2)
		require.NoError(t, err, "failed to fill in event queue")
	}

	dequeuedEvents2, err := queue.Dequeue(ctx, "praefect", "gitaly-1", 1)
	require.NoError(t, err)
	require.Len(t, dequeuedEvents2, 1)
	requireLocks(t, ctx, db, []LockRow{
		{ID: "praefect|gitaly-1|/project/path-1", Acquired: true},
		{ID: "praefect|gitaly-1|/project/path-2", Acquired: true},
	})
	requireJobLocks(t, ctx, db, []JobLockRow{
		{JobID: 1, LockID: "praefect|gitaly-1|/project/path-1"},
		{JobID: 3, LockID: "praefect|gitaly-1|/project/path-2"},
	})
}

func TestPostgresReplicationEventQueue_Acknowledge(t *testing.T) {
	t.Parallel()
	db := testdb.New(t)
	ctx := testhelper.Context(t)

	queue := PostgresReplicationEventQueue{db.DB}

	event := ReplicationEvent{
		Job: ReplicationJob{
			Change:            UpdateRepo,
			RelativePath:      "/project/path-1",
			TargetNodeStorage: "gitaly-1",
			SourceNodeStorage: "gitaly-0",
			VirtualStorage:    "praefect",
			Params:            nil,
		},
	}

	repositoryID := insertRepository(t, db, ctx, event.Job.VirtualStorage, event.Job.RelativePath, event.Job.SourceNodeStorage)
	event, err := queue.Enqueue(ctx, event)
	event.Job.RepositoryID = repositoryID
	require.NoError(t, err, "failed to fill in event queue")

	actual, err := queue.Dequeue(ctx, event.Job.VirtualStorage, event.Job.TargetNodeStorage, 100)
	require.NoError(t, err)

	// as we deque events we acquire lock for processing
	requireLocks(t, ctx, db, []LockRow{{ID: event.LockID, Acquired: true}})
	requireJobLocks(t, ctx, db, []JobLockRow{{JobID: event.ID, LockID: event.LockID}})

	acknowledged, err := queue.Acknowledge(ctx, JobStateCompleted, []uint64{actual[0].ID, 100500})
	require.NoError(t, err)
	require.Equal(t, []uint64{actual[0].ID}, acknowledged)

	event.State = JobStateCompleted
	event.Attempt = 2
	// events acknowledged with 'completed' or 'dead' states expected to be removed
	db.RequireRowsInTable(t, "replication_queue", 0)
	// all associated with acknowledged event tracking bindings between lock and event must be removed
	db.RequireRowsInTable(t, "replication_queue_job_lock", 0)
	// lock must be released as the event was acknowledged and there are no other events left protected under this lock
	requireLocks(t, ctx, db, []LockRow{{ID: event.LockID, Acquired: false}})
}

func TestPostgresReplicationEventQueue_AcknowledgeMultiple(t *testing.T) {
	t.Parallel()
	db := testdb.New(t)
	ctx := testhelper.Context(t)

	queue := PostgresReplicationEventQueue{db.DB}

	eventType1 := ReplicationEvent{
		Job: ReplicationJob{
			Change:            UpdateRepo,
			RelativePath:      "/project/path-1",
			TargetNodeStorage: "gitaly-1",
			SourceNodeStorage: "gitaly-0",
			VirtualStorage:    "praefect",
			Params:            nil,
		},
	}

	eventType2 := ReplicationEvent{
		Job: ReplicationJob{
			Change:            DeleteRepo,
			RelativePath:      "/project/path-2",
			TargetNodeStorage: "gitaly-1",
			SourceNodeStorage: "",
			VirtualStorage:    "praefect",
			Params:            nil,
		},
	}

	eventType3 := ReplicationEvent{
		Job: ReplicationJob{
			Change:            UpdateRepo,
			RelativePath:      "/project/path-3",
			TargetNodeStorage: "gitaly-1",
			SourceNodeStorage: "gitaly-0",
			VirtualStorage:    "praefect",
			Params:            nil,
		},
	}

	eventType4 := ReplicationEvent{
		Job: ReplicationJob{
			Change:            UpdateRepo,
			RelativePath:      "/project/path-1",
			TargetNodeStorage: "gitaly-2",
			SourceNodeStorage: "gitaly-0",
			VirtualStorage:    "praefect",
			Params:            nil,
		},
	}

	events := []ReplicationEvent{eventType1, eventType1, eventType2, eventType1, eventType3, eventType2, eventType4} // events to fill in the queue
	for i := range events {
		events[i].Job.RepositoryID = insertRepository(t, db, ctx, events[i].Job.VirtualStorage, events[i].Job.RelativePath, events[i].Job.SourceNodeStorage)
		var err error
		events[i], err = queue.Enqueue(ctx, events[i])
		require.NoError(t, err, "failed to fill in event queue")
	}

	// we expect only first three types of events by limiting count to 3
	dequeuedEvents1, err := queue.Dequeue(ctx, "praefect", "gitaly-1", 3)
	require.NoError(t, err)
	require.Len(t, dequeuedEvents1, 3)
	requireLocks(t, ctx, db, []LockRow{
		{ID: events[0].LockID, Acquired: true},
		{ID: events[2].LockID, Acquired: true},
		{ID: events[4].LockID, Acquired: true},
		{ID: events[6].LockID, Acquired: false},
	})
	requireJobLocks(t, ctx, db, []JobLockRow{
		{JobID: events[0].ID, LockID: events[0].LockID},
		{JobID: events[2].ID, LockID: events[2].LockID},
		{JobID: events[4].ID, LockID: events[4].LockID},
	})

	// release lock for events of second type
	acknowledge1, err := queue.Acknowledge(ctx, JobStateFailed, []uint64{events[2].ID})
	require.NoError(t, err)
	require.Equal(t, []uint64{3}, acknowledge1)
	requireLocks(t, ctx, db, []LockRow{
		{ID: events[0].LockID, Acquired: true},
		{ID: events[2].LockID, Acquired: false},
		{ID: events[4].LockID, Acquired: true},
		{ID: events[6].LockID, Acquired: false},
	})
	requireJobLocks(t, ctx, db, []JobLockRow{
		{JobID: events[0].ID, LockID: events[0].LockID},
		{JobID: events[4].ID, LockID: events[4].LockID},
	})

	dequeuedEvents2, err := queue.Dequeue(ctx, "praefect", "gitaly-1", 3)
	require.NoError(t, err)
	require.Len(t, dequeuedEvents2, 1, "expected: events of type 2 ('failed' will  be fetched for retry)")
	requireLocks(t, ctx, db, []LockRow{
		{ID: events[0].LockID, Acquired: true},
		{ID: events[2].LockID, Acquired: true},
		{ID: events[4].LockID, Acquired: true},
		{ID: events[6].LockID, Acquired: false},
	})
	requireJobLocks(t, ctx, db, []JobLockRow{
		{JobID: events[0].ID, LockID: events[0].LockID},
		{JobID: events[2].ID, LockID: events[2].LockID},
		{JobID: events[4].ID, LockID: events[4].LockID},
	})

	// creation of the new event that is equal to those already dequeue and processed
	// it is used to verify that the event created after consuming events from queue won't be marked
	// with previously created events as it may cause delay in replication
	_, err = queue.Enqueue(ctx, eventType1)
	require.NoError(t, err)

	acknowledge2, err := queue.Acknowledge(ctx, JobStateCompleted, []uint64{events[0].ID, events[4].ID})
	require.NoError(t, err)
	require.Equal(t, []uint64{events[0].ID, events[4].ID}, acknowledge2)
	requireLocks(t, ctx, db, []LockRow{
		{ID: events[0].LockID, Acquired: false},
		{ID: events[2].LockID, Acquired: true},
		{ID: events[4].LockID, Acquired: false},
		{ID: events[6].LockID, Acquired: false},
	})
	requireJobLocks(t, ctx, db, []JobLockRow{
		{JobID: events[2].ID, LockID: events[2].LockID},
	})
	db.RequireRowsInTable(t, "replication_queue", 4)

	dequeuedEvents3, err := queue.Dequeue(ctx, "praefect", "gitaly-2", 3)
	require.NoError(t, err)
	require.Len(t, dequeuedEvents3, 1, "expected: event of type 4")
	requireLocks(t, ctx, db, []LockRow{
		{ID: events[0].LockID, Acquired: false},
		{ID: events[2].LockID, Acquired: true},
		{ID: events[4].LockID, Acquired: false},
		{ID: events[6].LockID, Acquired: true},
	})
	requireJobLocks(t, ctx, db, []JobLockRow{
		{JobID: events[2].ID, LockID: events[2].LockID},
		{JobID: events[6].ID, LockID: events[6].LockID},
	})

	acknowledged3, err := queue.Acknowledge(ctx, JobStateCompleted, []uint64{events[2].ID, events[6].ID})
	require.NoError(t, err)
	require.Equal(t, []uint64{events[2].ID, events[6].ID}, acknowledged3)
	requireLocks(t, ctx, db, []LockRow{
		{ID: events[0].LockID, Acquired: false},
		{ID: events[2].LockID, Acquired: false},
		{ID: events[4].LockID, Acquired: false},
		{ID: events[6].LockID, Acquired: false},
	})
	requireJobLocks(t, ctx, db, nil)
	db.RequireRowsInTable(t, "replication_queue", 1)

	newEvent, err := queue.Enqueue(ctx, eventType1)
	require.NoError(t, err)

	acknowledge4, err := queue.Acknowledge(ctx, JobStateCompleted, []uint64{newEvent.ID})
	require.NoError(t, err)
	require.Equal(t, ([]uint64)(nil), acknowledge4) // event that was not dequeued can't be acknowledged
	db.RequireRowsInTable(t, "replication_queue", 2)

	var newEventState string
	require.NoError(t, db.QueryRow("SELECT state FROM replication_queue WHERE id = $1", newEvent.ID).Scan(&newEventState))
	require.Equal(t, "ready", newEventState, "no way to acknowledge event that is not in in_progress state(was not dequeued)")
	requireLocks(t, ctx, db, []LockRow{
		{ID: events[0].LockID, Acquired: false},
		{ID: events[2].LockID, Acquired: false},
		{ID: events[4].LockID, Acquired: false},
		{ID: events[6].LockID, Acquired: false},
	})
	requireJobLocks(t, ctx, db, nil)
}

func TestPostgresReplicationEventQueue_StartHealthUpdate(t *testing.T) {
	t.Parallel()
	eventType1 := ReplicationEvent{Job: ReplicationJob{
		Change:            UpdateRepo,
		VirtualStorage:    "vs-1",
		TargetNodeStorage: "s-1",
		SourceNodeStorage: "s-0",
		RelativePath:      "/path/1",
	}}

	eventType2 := eventType1
	eventType2.Job.RelativePath = "/path/2"

	eventType3 := eventType1
	eventType3.Job.VirtualStorage = "vs-2"

	eventType4 := eventType1
	eventType4.Job.TargetNodeStorage = "s-2"

	db := testdb.New(t)

	t.Run("no events is valid", func(t *testing.T) {
		// 'qc' is not initialized, so the test will fail if there will be an attempt to make SQL operation
		queue := NewPostgresReplicationEventQueue(nil)
		ctx := testhelper.Context(t)

		require.NoError(t, queue.StartHealthUpdate(ctx, nil, nil))
	})

	t.Run("can be terminated by the passed in context", func(t *testing.T) {
		ctx, cancel := context.WithCancel(testhelper.Context(t))

		// 'qc' is not initialized, so the test will fail if there will be an attempt to make SQL operation
		queue := NewPostgresReplicationEventQueue(nil)
		cancel()
		require.NoError(t, queue.StartHealthUpdate(ctx, nil, []ReplicationEvent{eventType1}))
	})

	t.Run("stops after first error", func(t *testing.T) {
		db.TruncateAll(t)
		ctx := testhelper.Context(t)

		qc, err := db.DB.BeginTx(ctx, nil)
		require.NoError(t, err)
		require.NoError(t, qc.Rollback())

		// 'qc' is initialized with invalid connection (transaction is finished), so operations on it will fail
		queue := NewPostgresReplicationEventQueue(qc)

		trigger := make(chan time.Time, 1)
		trigger <- time.Time{}

		require.Error(t, queue.StartHealthUpdate(ctx, trigger, []ReplicationEvent{eventType1}))
	})

	t.Run("stops if nothing to update (extended coverage)", func(t *testing.T) {
		db.TruncateAll(t)
		ctx := testhelper.Context(t)

		done := make(chan struct{})
		queue := NewPostgresReplicationEventQueue(db)
		go func() {
			trigger := make(chan time.Time)
			close(trigger)

			defer close(done)
			assert.NoError(t, queue.StartHealthUpdate(ctx, trigger, []ReplicationEvent{eventType1}))
		}()

		select {
		case <-done:
			return // happy path
		case <-time.After(time.Second):
			require.FailNow(t, "method should return almost immediately as there is nothing to process")
		}
	})

	t.Run("triggers all passed in events", func(t *testing.T) {
		db.TruncateAll(t)

		var wg sync.WaitGroup
		ctx, cancel := context.WithCancel(testhelper.Context(t))
		defer func() {
			cancel()
			wg.Wait()
		}()

		queue := NewPostgresReplicationEventQueue(db)
		events := []ReplicationEvent{eventType1, eventType2, eventType3, eventType4}
		for i := range events {
			events[i].Job.RepositoryID = insertRepository(t, db, ctx, events[i].Job.VirtualStorage, events[i].Job.RelativePath, events[i].Job.SourceNodeStorage)
			var err error
			events[i], err = queue.Enqueue(ctx, events[i])
			require.NoError(t, err, "failed to fill in event queue")
		}

		dequeuedEventsToTrigger, err := queue.Dequeue(ctx, eventType1.Job.VirtualStorage, eventType1.Job.TargetNodeStorage, 10)
		require.NoError(t, err)
		require.Len(t, dequeuedEventsToTrigger, 2, "eventType3 and eventType4 should not be fetched")
		ids := []uint64{dequeuedEventsToTrigger[0].ID, dequeuedEventsToTrigger[1].ID}

		dequeuedEventsUntriggered, err := queue.Dequeue(ctx, eventType3.Job.VirtualStorage, eventType3.Job.TargetNodeStorage, 10)
		require.NoError(t, err)
		require.Len(t, dequeuedEventsUntriggered, 1, "only eventType3 should be fetched")

		initialJobLocks := fetchJobLocks(t, ctx, db)

		trigger := make(chan time.Time, 1)
		wg.Add(1)
		go func() {
			defer wg.Done()
			assert.NoError(t, queue.StartHealthUpdate(ctx, trigger, dequeuedEventsToTrigger))
		}()

		trigger <- time.Time{}
		// once this consumed we are sure that the previous update has been executed
		trigger <- time.Time{}

		updatedJobLocks := fetchJobLocks(t, ctx, db)
		for i := range initialJobLocks {
			if updatedJobLocks[i].JobID == dequeuedEventsUntriggered[0].ID {
				require.Equal(t, initialJobLocks[i].TriggeredAt, updatedJobLocks[i].TriggeredAt, "no update expected as it was not submitted")
			} else {
				require.GreaterOrEqual(t, updatedJobLocks[i].TriggeredAt.UnixNano(), initialJobLocks[i].TriggeredAt.UnixNano())
			}
		}

		ackIDs, err := queue.Acknowledge(ctx, JobStateFailed, ids)
		require.NoError(t, err)
		require.ElementsMatch(t, ackIDs, ids)

		require.Len(t, fetchJobLocks(t, ctx, db), 1, "bindings should be removed after acknowledgment")
	})
}

func TestPostgresReplicationEventQueue_AcknowledgeStale(t *testing.T) {
	t.Parallel()
	ctx := testhelper.Context(t)

	eventType := ReplicationEvent{Job: ReplicationJob{
		Change:            UpdateRepo,
		RelativePath:      "/project/path-1",
		TargetNodeStorage: "gitaly-1",
		SourceNodeStorage: "gitaly-0",
		VirtualStorage:    "praefect-1",
	}}

	eventType1 := eventType

	eventType2 := eventType
	eventType2.Job.VirtualStorage = "praefect-2"

	eventType3 := eventType2
	eventType3.Job.RelativePath = "/project/path-2"
	eventType3.Job.TargetNodeStorage = "gitaly-2"

	eventType4 := eventType3
	eventType4.Job.TargetNodeStorage = "gitaly-3"

	db := testdb.New(t)

	t.Run("no stale jobs yet", func(t *testing.T) {
		db.TruncateAll(t)
		source := NewPostgresReplicationEventQueue(db)

		insertRepository(t, db, ctx, eventType1.Job.VirtualStorage, eventType1.Job.RelativePath, eventType1.Job.SourceNodeStorage)
		event, err := source.Enqueue(ctx, eventType1)
		require.NoError(t, err)

		devents, err := source.Dequeue(ctx, event.Job.VirtualStorage, event.Job.TargetNodeStorage, 1)
		require.NoError(t, err)

		// events triggered just now (< 1 sec ago), so nothing considered stale
		n, err := source.AcknowledgeStale(ctx, time.Second)
		require.NoError(t, err)
		requireEvents(t, ctx, db, devents)
		require.Equal(t, n, int64(0))
	})

	t.Run("jobs considered stale only at 'in_progress' state", func(t *testing.T) {
		db.TruncateAll(t)
		source := NewPostgresReplicationEventQueue(db)

		insertRepository(t, db, ctx, eventType1.Job.VirtualStorage, eventType1.Job.RelativePath, eventType1.Job.SourceNodeStorage)
		// move event to 'ready' state
		event1, err := source.Enqueue(ctx, eventType1)
		require.NoError(t, err)

		insertRepository(t, db, ctx, eventType2.Job.VirtualStorage, eventType2.Job.RelativePath, eventType2.Job.SourceNodeStorage)
		// move event to 'failed' state
		event2, err := source.Enqueue(ctx, eventType2)
		require.NoError(t, err)
		devents2, err := source.Dequeue(ctx, event2.Job.VirtualStorage, event2.Job.TargetNodeStorage, 1)
		require.NoError(t, err)
		require.Equal(t, event2.ID, devents2[0].ID)
		_, err = source.Acknowledge(ctx, JobStateFailed, []uint64{devents2[0].ID})
		require.NoError(t, err)

		insertRepository(t, db, ctx, eventType3.Job.VirtualStorage, eventType3.Job.RelativePath, eventType3.Job.SourceNodeStorage)
		// move event to 'dead' state
		event3, err := source.Enqueue(ctx, eventType3)
		require.NoError(t, err)
		devents3, err := source.Dequeue(ctx, event3.Job.VirtualStorage, event3.Job.TargetNodeStorage, 1)
		require.NoError(t, err)
		require.Equal(t, event3.ID, devents3[0].ID)
		_, err = source.Acknowledge(ctx, JobStateDead, []uint64{devents3[0].ID})
		require.NoError(t, err)

		insertRepository(t, db, ctx, eventType4.Job.VirtualStorage, eventType4.Job.RelativePath, eventType4.Job.SourceNodeStorage)
		event4, err := source.Enqueue(ctx, eventType4)
		require.NoError(t, err)
		devents4, err := source.Dequeue(ctx, event4.Job.VirtualStorage, event4.Job.TargetNodeStorage, 1)
		require.NoError(t, err)

		n, err := source.AcknowledgeStale(ctx, time.Microsecond)
		require.NoError(t, err)

		devents2[0].State = JobStateFailed
		devents4[0].Attempt = 2
		devents4[0].State = JobStateFailed
		requireEvents(t, ctx, db, []ReplicationEvent{event1, devents2[0], devents4[0]})
		require.Equal(t, n, int64(1))
	})

	t.Run("stale jobs updated for all virtual storages and storages at once", func(t *testing.T) {
		db.TruncateAll(t)
		source := NewPostgresReplicationEventQueue(db)

		var events []ReplicationEvent
		for _, eventType := range []ReplicationEvent{eventType1, eventType2, eventType3} {
			insertRepository(t, db, ctx, eventType.Job.VirtualStorage, eventType.Job.RelativePath, eventType.Job.SourceNodeStorage)
			event, err := source.Enqueue(ctx, eventType)
			require.NoError(t, err)
			devents, err := source.Dequeue(ctx, event.Job.VirtualStorage, event.Job.TargetNodeStorage, 1)
			require.NoError(t, err)
			events = append(events, devents...)
		}

		for event, i := events[0], 0; i < 2; i++ { // consume all processing attempts to verify that state will be changed to 'dead'
			_, err := source.Acknowledge(ctx, JobStateFailed, []uint64{event.ID})
			require.NoError(t, err)
			_, err = source.Dequeue(ctx, event.Job.VirtualStorage, event.Job.TargetNodeStorage, 1)
			require.NoError(t, err)
		}

		n, err := source.AcknowledgeStale(ctx, time.Microsecond)
		require.NoError(t, err)
		require.Equal(t, n, int64(3))

		var exp []ReplicationEvent
		// The first event should be removed from table as its state changed to 'dead'.
		for _, e := range events[1:] {
			e.State = JobStateFailed
			exp = append(exp, e)
		}

		requireEvents(t, ctx, db, exp)
	})
}

func TestLockRowIsRemovedOnceRepositoryIsRemoved(t *testing.T) {
	t.Parallel()
	db := testdb.New(t)
	ctx := testhelper.Context(t)

	const (
		virtualStorage = "praefect"
		relativePath   = "/project/path-1"
		primaryStorage = "gitaly-0"
	)

	enqueueJobs := []ReplicationJob{
		// This event will be moved to in_progress state and lock will be with acquired=true.
		{
			Change:            UpdateRepo,
			RepositoryID:      1,
			RelativePath:      relativePath,
			ReplicaPath:       "relative/project/path-1",
			TargetNodeStorage: "gitaly-1",
			SourceNodeStorage: primaryStorage,
			VirtualStorage:    virtualStorage,
			Params:            nil,
		},
		// This event is for another storage, but for the same repository.
		{
			Change:            UpdateRepo,
			RepositoryID:      1,
			RelativePath:      relativePath,
			ReplicaPath:       "relative/project/path-2",
			TargetNodeStorage: "gitaly-2",
			SourceNodeStorage: primaryStorage,
			VirtualStorage:    virtualStorage,
			Params:            nil,
		},
	}
	repositoryID := insertRepository(t, db, ctx, virtualStorage, relativePath, primaryStorage)

	queue := NewPostgresReplicationEventQueue(db)
	for _, job := range enqueueJobs {
		event := ReplicationEvent{Job: job}
		_, err := queue.Enqueue(ctx, event)
		require.NoError(t, err)
	}
	_, err := queue.Dequeue(ctx, enqueueJobs[0].VirtualStorage, enqueueJobs[0].TargetNodeStorage, 1)
	require.NoError(t, err, "pickup one job to change it's status and create additional rows")

	_, err = db.ExecContext(ctx, `DELETE FROM repositories where repository_id = $1`, repositoryID)
	require.NoError(t, err)

	db.RequireRowsInTable(t, "replication_queue_lock", 0)
}

func insertRepository(t *testing.T, db glsql.Querier, ctx context.Context, virtualStorage, relativePath, primary string) int64 {
	t.Helper()
	const query = `
		INSERT INTO repositories(virtual_storage, relative_path, generation, "primary")
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (virtual_storage, relative_path)
		DO UPDATE SET relative_path = excluded.relative_path
		RETURNING repository_id`
	var repositoryID int64
	err := db.QueryRowContext(ctx, query, virtualStorage, relativePath, 1, primary).
		Scan(&repositoryID)
	require.NoError(t, err, "create repository record")
	return repositoryID
}

func requireEvents(t *testing.T, ctx context.Context, db testdb.DB, expected []ReplicationEvent) {
	t.Helper()

	// as it is not possible to expect exact time of entity creation/update we do not fetch it from database
	// and we do not take it into account from expected values.
	exp := make([]ReplicationEvent, len(expected)) // make a copy to avoid side effects for passed values
	for i, e := range expected {
		exp[i] = e
		// set to default values as they would not be initialized from database
		exp[i].CreatedAt = time.Time{}
		exp[i].UpdatedAt = nil
	}

	sqlStmt := `SELECT id, state, created_at, updated_at, lock_id, attempt, meta, change,
			repository_id, replica_path, relative_path, target_node_storage,
			source_node_storage, virtual_storage, params
		FROM replication_queue ORDER BY id`
	rows, err := db.QueryContext(ctx, sqlStmt)
	require.NoError(t, err)

	actual, err := scanReplicationEvents(rows)
	require.NoError(t, err)
	for i := 0; i < len(actual); i++ {
		actual[i].CreatedAt = time.Time{}
		actual[i].UpdatedAt = nil
	}
	require.Equal(t, exp, actual)
}

// LockRow exists only for testing purposes and represents entries from replication_queue_lock table.
type LockRow struct {
	ID       string
	Acquired bool
}

func requireLocks(t *testing.T, ctx context.Context, db testdb.DB, expected []LockRow) {
	t.Helper()

	sqlStmt := `SELECT id, acquired FROM replication_queue_lock`
	rows, err := db.QueryContext(ctx, sqlStmt)
	require.NoError(t, err)
	defer func() { require.NoError(t, rows.Close(), "completion of result fetching") }()

	var actual []LockRow
	for rows.Next() {
		var entry LockRow
		require.NoError(t, rows.Scan(&entry.ID, &entry.Acquired), "failed to scan entry")
		actual = append(actual, entry)
	}
	require.NoError(t, rows.Err(), "completion of result loop scan")
	require.ElementsMatch(t, expected, actual)
}

// JobLockRow exists only for testing purposes and represents entries from replication_queue_job_lock table.
type JobLockRow struct {
	JobID       uint64
	LockID      string
	TriggeredAt time.Time
}

func requireJobLocks(t *testing.T, ctx context.Context, db testdb.DB, expected []JobLockRow) {
	t.Helper()

	actual := fetchJobLocks(t, ctx, db)
	for i := range actual {
		actual[i].TriggeredAt = time.Time{}
	}
	require.ElementsMatch(t, expected, actual)
}

func fetchJobLocks(t *testing.T, ctx context.Context, db testdb.DB) []JobLockRow {
	t.Helper()
	sqlStmt := `SELECT job_id, lock_id, triggered_at FROM replication_queue_job_lock ORDER BY job_id`
	rows, err := db.QueryContext(ctx, sqlStmt)
	require.NoError(t, err)
	defer func() { require.NoError(t, rows.Close(), "completion of result fetching") }()

	var entries []JobLockRow
	for rows.Next() {
		var entry JobLockRow
		require.NoError(t, rows.Scan(&entry.JobID, &entry.LockID, &entry.TriggeredAt), "failed to scan entry")
		entries = append(entries, entry)
	}
	require.NoError(t, rows.Err(), "completion of result loop scan")

	return entries
}
