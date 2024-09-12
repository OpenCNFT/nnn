package backup

import (
	"context"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"testing/fstest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v16/internal/archive"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/mode"
	"gitlab.com/gitlab-org/gitaly/v16/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper"
)

type mockNode struct {
	managers map[partitionInfo]*mockLogManager
	t        *testing.T

	sync.Mutex
}

func (n *mockNode) GetStorage(storageName string) (storage.Storage, error) {
	return mockStorage{storageName: storageName, node: n}, nil
}

type mockStorage struct {
	storageName string
	node        *mockNode
	storage.Storage
}

func (m mockStorage) GetPartition(ctx context.Context, partitionID storage.PartitionID) (storage.Partition, error) {
	m.node.Lock()
	defer m.node.Unlock()

	info := partitionInfo{m.storageName, partitionID}
	mgr, ok := m.node.managers[info]
	assert.True(m.node.t, ok)

	return mgr, nil
}

type mockLogManager struct {
	partitionInfo partitionInfo
	archiver      *LogEntryArchiver
	notifications []notification
	acknowledged  []storage.LSN
	finalLSN      storage.LSN
	entryRootPath string
	finishFunc    func()
	finishCount   int

	sync.Mutex
	storage.Partition
}

func (lm *mockLogManager) Close() {}

func (lm *mockLogManager) AcknowledgeTransaction(_ storage.LogConsumer, lsn storage.LSN) {
	lm.Lock()
	defer lm.Unlock()

	lm.acknowledged = append(lm.acknowledged, lsn)

	// If the archiver has completed enough entries to reach our new low water mark,
	// send the next notification.
	if len(lm.notifications) > 0 && lsn >= lm.notifications[0].sendAt {
		lm.SendNotification()
	}

	// It's possible that the archiver acknowledges a LSN more than once. When the LogEntryArchiver
	// processes a notification, it sets the current state's high water mark = notification's high
	// water mark and notifies another goroutine for processing. The processing goroutine processes
	// log entries sequentially from the current nextLSN -> high watermark. After each iteration, it
	// acknowledges the corresponding log entry. If the next notification contains the same high
	// watermark as the prior, there's a chance the processing goroutine already handles all log
	// entries. It then re-acknowledges the high watermark and skips the notification.
	//
	// For example:
	// - Ingest notification [3, 5]
	// - Process 3, ack 3 -> nextLSN = 4
	// - Process 4, ack 4 -> nextLSN = 5
	// - Process 5, ack 5 -> nextLSN = 6
	// - Ingest notification [4, 5] -> re-ack 5
	//
	// As a result, we allow at most finishCount calls because of redundant acknowledgement. The
	// exceeding calls could be ignored.
	if lsn == lm.finalLSN && len(lm.notifications) == 0 && lm.finishCount > 0 {
		lm.finishCount--
		lm.finishFunc()
	}
}

func (lm *mockLogManager) SendNotification() {
	n := lm.notifications[0]
	lm.archiver.NotifyNewTransactions(lm.partitionInfo.storageName, lm.partitionInfo.partitionID, n.lowWaterMark, n.highWaterMark)

	lm.notifications = lm.notifications[1:]
}

func (lm *mockLogManager) GetTransactionPath(lsn storage.LSN) string {
	return filepath.Join(partitionPath(lm.entryRootPath, lm.partitionInfo.storageName, lm.partitionInfo.partitionID), lsn.String())
}

type notification struct {
	lowWaterMark, highWaterMark, sendAt storage.LSN
}

func TestLogEntryArchiver(t *testing.T) {
	t.Parallel()
	ctx := testhelper.Context(t)

	for _, tc := range []struct {
		desc                string
		workerCount         uint
		partitions          []storage.PartitionID
		notifications       []notification
		waitCount           int
		finalLSN            storage.LSN
		expectedBackupCount int
		expectedLogMessage  string
	}{
		{
			desc:        "notify one entry",
			workerCount: 1,
			partitions:  []storage.PartitionID{1},
			notifications: []notification{
				{
					lowWaterMark:  1,
					highWaterMark: 1,
				},
			},
			finalLSN:            1,
			expectedBackupCount: 1,
		},
		{
			desc:        "start from later LSN",
			workerCount: 1,
			partitions:  []storage.PartitionID{1},
			notifications: []notification{
				{
					lowWaterMark:  11,
					highWaterMark: 11,
				},
			},
			finalLSN:            11,
			expectedBackupCount: 1,
		},
		{
			desc:        "notify range of entries",
			workerCount: 1,
			partitions:  []storage.PartitionID{1},
			notifications: []notification{
				{
					lowWaterMark:  1,
					highWaterMark: 5,
				},
			},
			finalLSN:            5,
			expectedBackupCount: 5,
		},
		{
			desc:        "increasing high water mark",
			workerCount: 1,
			partitions:  []storage.PartitionID{1},
			notifications: []notification{
				{
					lowWaterMark:  1,
					highWaterMark: 5,
				},
				{
					lowWaterMark:  1,
					highWaterMark: 6,
				},
				{
					lowWaterMark:  1,
					highWaterMark: 7,
				},
			},
			finalLSN:            7,
			expectedBackupCount: 7,
		},
		{
			desc:        "increasing low water mark",
			workerCount: 1,
			partitions:  []storage.PartitionID{1},
			notifications: []notification{
				{
					lowWaterMark:  1,
					highWaterMark: 5,
				},
				{
					lowWaterMark:  2,
					highWaterMark: 5,
					sendAt:        2,
				},
				{
					lowWaterMark:  3,
					highWaterMark: 5,
					sendAt:        3,
				},
			},
			finalLSN:            5,
			expectedBackupCount: 5,
		},
		{
			desc:        "multiple partitions",
			workerCount: 1,
			partitions:  []storage.PartitionID{1, 2, 3},
			notifications: []notification{
				{
					lowWaterMark:  1,
					highWaterMark: 3,
				},
			},
			finalLSN:            3,
			expectedBackupCount: 9,
		},
		{
			desc:        "multiple partitions, multi-threaded",
			workerCount: 4,
			partitions:  []storage.PartitionID{1, 2, 3},
			notifications: []notification{
				{
					lowWaterMark:  1,
					highWaterMark: 3,
				},
			},
			finalLSN:            3,
			expectedBackupCount: 9,
		},
		{
			desc:        "resent items processed once",
			workerCount: 1,
			partitions:  []storage.PartitionID{1},
			notifications: []notification{
				{
					lowWaterMark:  1,
					highWaterMark: 3,
				},
				{
					lowWaterMark:  1,
					highWaterMark: 3,
					sendAt:        3,
				},
			},
			waitCount:           2,
			finalLSN:            3,
			expectedBackupCount: 3,
		},
		{
			desc:        "gap in sequence",
			workerCount: 1,
			partitions:  []storage.PartitionID{1},
			notifications: []notification{
				{
					lowWaterMark:  1,
					highWaterMark: 3,
				},
				{
					lowWaterMark:  11,
					highWaterMark: 13,
					sendAt:        3,
				},
			},
			finalLSN:            13,
			expectedBackupCount: 6,
			expectedLogMessage:  "log entry archiver: gap in log sequence",
		},
	} {
		tc := tc
		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			ctx, cancel := context.WithCancel(ctx)
			defer cancel()

			entryRootPath := testhelper.TempDir(t)

			archivePath := testhelper.TempDir(t)
			archiveSink, err := ResolveSink(ctx, archivePath)
			require.NoError(t, err)

			logger := testhelper.NewLogger(t)
			hook := testhelper.AddLoggerHook(logger)
			var wg sync.WaitGroup

			accessor := &mockNode{
				managers: make(map[partitionInfo]*mockLogManager, len(tc.partitions)),
				t:        t,
			}

			node := storage.Node(accessor)
			archiver := NewLogEntryArchiver(logger, archiveSink, tc.workerCount, &node)
			archiver.Run()

			const storageName = "default"

			managers := make(map[partitionInfo]*mockLogManager, len(tc.partitions))
			for _, partitionID := range tc.partitions {
				info := partitionInfo{
					storageName: storageName,
					partitionID: partitionID,
				}

				waitCount := tc.waitCount
				if waitCount == 0 {
					waitCount = 1
				}
				wg.Add(waitCount)

				manager := &mockLogManager{
					partitionInfo: info,
					entryRootPath: entryRootPath,
					archiver:      archiver,
					notifications: tc.notifications,
					finalLSN:      tc.finalLSN,
					finishFunc:    wg.Done,
					finishCount:   waitCount,
				}
				managers[info] = manager

				accessor.Lock()
				accessor.managers[info] = manager
				accessor.Unlock()

				partitionID := partitionID

				// Send partitions in parallel to mimic real usage.
				go func() {
					sentLSNs := make([]storage.LSN, 0, tc.finalLSN)
					for _, notification := range tc.notifications {
						for lsn := notification.lowWaterMark; lsn <= notification.highWaterMark; lsn++ {
							// Don't recreate entries.
							if !slices.Contains(sentLSNs, lsn) {
								createEntryDir(t, entryRootPath, info.storageName, partitionID, lsn)
							}

							sentLSNs = append(sentLSNs, lsn)
						}
					}
					manager.Lock()
					defer manager.Unlock()
					manager.SendNotification()
				}()
			}

			wg.Wait()
			archiver.Close()

			cmpDir := testhelper.TempDir(t)
			require.NoError(t, os.Mkdir(filepath.Join(cmpDir, storageName), mode.Directory))

			for info, manager := range accessor.managers {
				lastAck := manager.acknowledged[len(manager.acknowledged)-1]
				require.Equal(t, tc.finalLSN, lastAck)

				partitionDir := partitionPath(cmpDir, info.storageName, info.partitionID)
				require.NoError(t, os.Mkdir(partitionDir, mode.Directory))

				for _, lsn := range manager.acknowledged {
					tarPath := filepath.Join(partitionPath(archivePath, info.storageName, info.partitionID), lsn.String()+".tar")
					tar, err := os.Open(tarPath)
					require.NoError(t, err)
					testhelper.RequireTarState(t, tar, testhelper.DirectoryState{
						lsn.String():                                {Mode: archive.TarFileMode | archive.ExecuteMode | fs.ModeDir},
						filepath.Join(lsn.String(), "LSN"):          {Mode: archive.TarFileMode, Content: []byte(lsn.String())},
						filepath.Join(lsn.String(), "PARTITION_ID"): {Mode: archive.TarFileMode, Content: []byte(fmt.Sprintf("%d", info.partitionID))},
					})
				}
			}

			if tc.expectedLogMessage != "" {
				var logs []string
				for _, entry := range hook.AllEntries() {
					logs = append(logs, entry.Message)
				}
				require.Contains(t, logs, tc.expectedLogMessage)
			}

			testhelper.RequirePromMetrics(t, archiver.backupCounter, buildMetrics(t, tc.expectedBackupCount, 0))
		})
	}
}

func TestLogEntryArchiver_retry(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(testhelper.Context(t))
	defer cancel()

	const lsn = storage.LSN(1)
	const workerCount = 1

	entryRootPath := testhelper.TempDir(t)

	var wg sync.WaitGroup

	logger := testhelper.NewLogger(t)
	hook := testhelper.AddLoggerHook(logger)
	ticker := helper.NewManualTicker()

	// Finish waiting when logEntry.backoff is hit after a failure.
	tickerFunc := func(time.Duration) helper.Ticker {
		wg.Done()
		return ticker
	}

	archivePath := testhelper.TempDir(t)
	archiveSink, err := ResolveSink(ctx, archivePath)
	require.NoError(t, err)

	accessor := &mockNode{
		managers: make(map[partitionInfo]*mockLogManager, 1),
		t:        t,
	}

	node := storage.Node(accessor)
	archiver := newLogEntryArchiver(logger, archiveSink, workerCount, &node, tickerFunc)
	archiver.Run()
	defer archiver.Close()

	info := partitionInfo{
		storageName: "default",
		partitionID: 1,
	}

	manager := &mockLogManager{
		partitionInfo: info,
		entryRootPath: entryRootPath,
		archiver:      archiver,
		notifications: []notification{
			{
				lowWaterMark:  1,
				highWaterMark: 1,
			},
		},
		finalLSN:    1,
		finishFunc:  wg.Done,
		finishCount: 1,
	}

	accessor.Lock()
	accessor.managers[info] = manager
	accessor.Unlock()

	wg.Add(1)
	manager.SendNotification()

	// Wait for initial request to fail and backoff.
	wg.Wait()

	require.Equal(t, "log entry archiver: failed to backup log entry", hook.LastEntry().Message)

	// Create entry so retry will succeed.
	createEntryDir(t, entryRootPath, info.storageName, info.partitionID, lsn)

	// Add to wg, this will be decremented when the retry completes.
	wg.Add(1)

	// Finish backoff.
	ticker.Tick()

	// Wait for retry to complete.
	wg.Wait()

	cmpDir := testhelper.TempDir(t)
	partitionDir := partitionPath(cmpDir, info.storageName, info.partitionID)
	require.NoError(t, os.MkdirAll(partitionDir, mode.Directory))

	tarPath := filepath.Join(partitionPath(archivePath, info.storageName, info.partitionID), lsn.String()) + ".tar"
	tar, err := os.Open(tarPath)
	require.NoError(t, err)

	testhelper.RequireTarState(t, tar, testhelper.DirectoryState{
		lsn.String():                                {Mode: archive.TarFileMode | archive.ExecuteMode | fs.ModeDir},
		filepath.Join(lsn.String(), "LSN"):          {Mode: archive.TarFileMode, Content: []byte(lsn.String())},
		filepath.Join(lsn.String(), "PARTITION_ID"): {Mode: archive.TarFileMode, Content: []byte(fmt.Sprintf("%d", info.partitionID))},
	})

	testhelper.RequirePromMetrics(t, archiver.backupCounter, buildMetrics(t, 1, 1))
}

func partitionPath(root string, storageName string, partitionID storage.PartitionID) string {
	return filepath.Join(root, storageName, fmt.Sprintf("%d", partitionID))
}

func createEntryDir(t *testing.T, entryRootPath string, storageName string, partitionID storage.PartitionID, lsn storage.LSN) {
	t.Helper()

	partitionPath := partitionPath(entryRootPath, storageName, partitionID)
	require.NoError(t, os.MkdirAll(partitionPath, mode.Directory))

	testhelper.CreateFS(t, filepath.Join(partitionPath, lsn.String()), fstest.MapFS{
		".":            {Mode: mode.Directory},
		"LSN":          {Mode: mode.File, Data: []byte(lsn.String())},
		"PARTITION_ID": {Mode: mode.File, Data: []byte(fmt.Sprintf("%d", partitionID))},
	})
}

func buildMetrics(t *testing.T, successCt, failCt int) string {
	t.Helper()

	var builder strings.Builder
	_, err := builder.WriteString("# HELP gitaly_wal_backup_count Counter of the number of WAL entries backed up by status\n")
	require.NoError(t, err)
	_, err = builder.WriteString("# TYPE gitaly_wal_backup_count counter\n")
	require.NoError(t, err)

	_, err = builder.WriteString(
		fmt.Sprintf("gitaly_wal_backup_count{status=\"success\"} %d\n", successCt))
	require.NoError(t, err)

	if failCt > 0 {
		_, err = builder.WriteString(
			fmt.Sprintf("gitaly_wal_backup_count{status=\"fail\"} %d\n", failCt))
		require.NoError(t, err)
	}

	return builder.String()
}
