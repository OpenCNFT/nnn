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
	"gitlab.com/gitlab-org/gitaly/v16/internal/git"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/mode"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/wal"
	"gitlab.com/gitlab-org/gitaly/v16/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
)

type mockPartition struct {
	t       *testing.T
	manager storage.LogManager
}

func (p *mockPartition) Begin(context.Context, storage.BeginOptions) (storage.Transaction, error) {
	p.t.Errorf("should not be called")
	return nil, nil
}

func (p *mockPartition) Close() {}

func (p *mockPartition) GetLogManager() storage.LogManager {
	return p.manager
}

type mockNode struct {
	partitions map[partitionInfo]*mockPartition
	t          *testing.T

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
	mgr, ok := m.node.partitions[info]
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
}

func (lm *mockLogManager) Close() {}

func (lm *mockLogManager) AcknowledgeConsumerPos(lsn storage.LSN) {
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
	lm.archiver.NotifyNewEntries(lm.partitionInfo.storageName, lm.partitionInfo.partitionID, n.lowWaterMark, n.highWaterMark)

	lm.notifications = lm.notifications[1:]
}

func (lm *mockLogManager) GetEntryPath(lsn storage.LSN) string {
	return filepath.Join(partitionPath(lm.entryRootPath, lm.partitionInfo.storageName, lm.partitionInfo.partitionID), lsn.String())
}

var (
	_ = storage.LogManager(&mockLogManager{})
	_ = storage.Partition(&mockPartition{})
)

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
				partitions: make(map[partitionInfo]*mockPartition, len(tc.partitions)),
				t:          t,
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
				accessor.partitions[info] = &mockPartition{t: t, manager: manager}
				accessor.Unlock()

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

			for info, partition := range accessor.partitions {
				manager := partition.GetLogManager().(*mockLogManager)
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
		partitions: make(map[partitionInfo]*mockPartition, 1),
		t:          t,
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
	accessor.partitions[info] = &mockPartition{t: t, manager: manager}
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

// TestLogEntryArchiver_WithRealLogManager runs the log archiver with a real WAL log manager. This test aims to assert
// the flow between the two components to avoid uncaught errors due to mocking. Test setup and verification are
// extremely verbose. There's no reliable way to simulate more sophisticated scenarios because the test could not
// intercept internal states of log archiver or log manager. The log manager doesn't expose the current consumer
// position. Thus, this test verifies the most basic archiving. Unit tests using mocking will cover the rest.
func TestLogEntryArchiver_WithRealLogManager(t *testing.T) {
	t.Parallel()
	ctx := testhelper.Context(t)

	// Setup node
	accessor := &mockNode{
		partitions: make(map[partitionInfo]*mockPartition),
		t:          t,
	}
	node := storage.Node(accessor)

	// Setup archiver
	archivePath := testhelper.TempDir(t)
	archiveSink, err := ResolveSink(ctx, archivePath)
	require.NoError(t, err)

	archiver := NewLogEntryArchiver(testhelper.NewLogger(t), archiveSink, 1, &node)
	archiver.Run()

	// Setup WAL log manager and plug it to storage
	const storageName = "default"
	var logManagers []*wal.LogManager
	for i := 1; i <= 3; i++ {
		info := partitionInfo{
			storageName: storageName,
			partitionID: storage.PartitionID(i),
		}

		logManager := wal.NewLogManager(storageName, storage.PartitionID(i), testhelper.TempDir(t), testhelper.TempDir(t), archiver)
		require.NoError(t, logManager.Initialize(ctx, 0))

		accessor.Lock()
		accessor.partitions[info] = &mockPartition{t: t, manager: logManager}
		accessor.Unlock()

		logManagers = append(logManagers, logManager)
	}

	// Update 1 ref
	relativePath1, commitIDs1 := setupTestRepo(t, ctx, 1)
	entry1 := wal.MultiplerefChangesLogEntry(relativePath1, []wal.RefChange{{Ref: "branch-1", Oid: commitIDs1[0]}})

	appendLogEntry(t, ctx, logManagers[0], entry1, map[string][]byte{
		"1": []byte(commitIDs1[0] + "\n"),
	})

	// Update 2 refs
	relativePath2, commitIDs2 := setupTestRepo(t, ctx, 2)
	entry2 := wal.MultiplerefChangesLogEntry(relativePath2, []wal.RefChange{
		{Ref: "branch-1", Oid: commitIDs2[0]},
		{Ref: "branch-2", Oid: commitIDs2[1]},
	})
	appendLogEntry(t, ctx, logManagers[1], entry2, map[string][]byte{
		"1": []byte(commitIDs2[0] + "\n"),
		"2": []byte(commitIDs2[1] + "\n"),
	})

	// KV operation without any file.
	relativePath3, _ := setupTestRepo(t, ctx, 0)
	entry3 := wal.MultipleKVLogEntry(relativePath3, []wal.KVOperation{
		{Key: []byte("key-1"), Value: []byte("value-1")},
		{Key: []byte("key-2"), Value: []byte("value-2")},
	})
	appendLogEntry(t, ctx, logManagers[2], entry3, nil)

	// Wait for acknolwedgements
	for _, manager := range logManagers {
		<-manager.NotifyQueue()
	}
	archiver.Close()

	cmpDir := testhelper.TempDir(t)
	require.NoError(t, os.Mkdir(filepath.Join(cmpDir, storageName), mode.Directory))

	lsnPrefix := storage.LSN(1).String()

	// Assert manager 1
	tarPath := filepath.Join(partitionPath(archivePath, storageName, storage.PartitionID(1)), lsnPrefix+".tar")
	tar, err := os.Open(tarPath)
	require.NoError(t, err)
	testhelper.RequireTarState(t, tar, testhelper.DirectoryState{
		lsnPrefix:                            {Mode: archive.TarFileMode | archive.ExecuteMode | fs.ModeDir},
		filepath.Join(lsnPrefix, "MANIFEST"): wal.ManifestDirectoryEntryInTar(entry1),
		filepath.Join(lsnPrefix, "1"):        {Mode: archive.TarFileMode, Content: []byte(commitIDs1[0] + "\n")},
	})
	testhelper.MustClose(t, tar)

	// Assert manager 2
	tarPath = filepath.Join(partitionPath(archivePath, storageName, storage.PartitionID(2)), lsnPrefix+".tar")
	tar, err = os.Open(tarPath)
	require.NoError(t, err)
	testhelper.RequireTarState(t, tar, testhelper.DirectoryState{
		lsnPrefix:                            {Mode: archive.TarFileMode | archive.ExecuteMode | fs.ModeDir},
		filepath.Join(lsnPrefix, "MANIFEST"): wal.ManifestDirectoryEntryInTar(entry2),
		filepath.Join(lsnPrefix, "1"):        {Mode: archive.TarFileMode, Content: []byte(commitIDs2[0] + "\n")},
		filepath.Join(lsnPrefix, "2"):        {Mode: archive.TarFileMode, Content: []byte(commitIDs2[1] + "\n")},
	})
	testhelper.MustClose(t, tar)

	// Assert manager 3
	tarPath = filepath.Join(partitionPath(archivePath, storageName, storage.PartitionID(3)), lsnPrefix+".tar")
	tar, err = os.Open(tarPath)
	require.NoError(t, err)
	testhelper.RequireTarState(t, tar, testhelper.DirectoryState{
		lsnPrefix:                            {Mode: archive.TarFileMode | archive.ExecuteMode | fs.ModeDir},
		filepath.Join(lsnPrefix, "MANIFEST"): wal.ManifestDirectoryEntryInTar(entry3), // No file
	})
	testhelper.MustClose(t, tar)

	// Finally, assert the metrics.
	testhelper.RequirePromMetrics(t, archiver.backupCounter, buildMetrics(t, 3, 0))
}

func appendLogEntry(t *testing.T, ctx context.Context, manager *wal.LogManager, logEntry *gitalypb.LogEntry, files map[string][]byte) storage.LSN {
	t.Helper()

	logEntryPath := testhelper.TempDir(t)
	for name, value := range files {
		path := filepath.Join(logEntryPath, name)
		require.NoError(t, os.WriteFile(path, value, mode.File))
	}

	nextLSN, err := manager.AppendLogEntry(ctx, logEntry, logEntryPath)
	require.NoError(t, err)

	return nextLSN
}

func setupTestRepo(t *testing.T, ctx context.Context, numCommits int) (string, []git.ObjectID) {
	relativePath := gittest.NewRepositoryName(t)
	cfg := testcfg.Build(t)
	_, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
		SkipCreationViaService: true,
		RelativePath:           relativePath,
	})

	var commitIDs []git.ObjectID
	for i := 0; i < numCommits; i++ {
		commitID := gittest.WriteCommit(t, cfg, repoPath, gittest.WithParents())
		commitIDs = append(commitIDs, commitID)
	}

	return relativePath, commitIDs
}
