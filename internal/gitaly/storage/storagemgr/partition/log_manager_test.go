package partition

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/mode"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper"
)

func appendLogEntry(t *testing.T, ctx context.Context, manager *LogManager, files map[string][]byte) storage.LSN {
	t.Helper()

	logEntryPath := testhelper.TempDir(t)
	for name, value := range files {
		path := filepath.Join(logEntryPath, name)
		require.NoError(t, os.WriteFile(path, value, mode.File))
	}

	nextLSN, err := manager.AppendLogEntry(ctx, logEntryPath)
	require.NoError(t, err)

	return nextLSN
}

func setupLogManager(t *testing.T, ctx context.Context, consumer storage.LogConsumer) *LogManager {
	logManager := NewLogManager("test-storage", 1, testhelper.TempDir(t), testhelper.TempDir(t), consumer)
	require.NoError(t, logManager.Initialize(ctx, 0))

	return logManager
}

func TestLogManager_Initialize(t *testing.T) {
	t.Parallel()

	t.Run("initial state without prior log entries", func(t *testing.T) {
		t.Parallel()
		ctx := testhelper.Context(t)
		stateDir := testhelper.TempDir(t)

		logManager := NewLogManager("test-storage", 1, testhelper.TempDir(t), stateDir, nil)
		require.NoError(t, logManager.Initialize(ctx, 0))

		require.Equal(t, storage.LSN(1), logManager.oldestLSN)
		require.Equal(t, storage.LSN(0), logManager.appendedLSN)
		require.Equal(t, storage.LSN(1), logManager.lowWaterMark())
		testhelper.RequireDirectoryState(t, logManager.stateDirectory, "", testhelper.DirectoryState{
			"/":    {Mode: mode.Directory},
			"/wal": {Mode: mode.Directory},
		})
	})

	t.Run("existing WAL entries without existing appliedLSN", func(t *testing.T) {
		t.Parallel()
		ctx := testhelper.Context(t)
		stateDir := testhelper.TempDir(t)

		logManager := NewLogManager("test-storage", 1, testhelper.TempDir(t), stateDir, nil)
		require.NoError(t, logManager.Initialize(ctx, 0))

		appendLogEntry(t, ctx, logManager, map[string][]byte{"1": []byte("content-1")})
		appendLogEntry(t, ctx, logManager, map[string][]byte{"1": []byte("content-2")})

		logManager = NewLogManager("test-storage", 1, testhelper.TempDir(t), stateDir, nil)
		require.NoError(t, logManager.Initialize(ctx, 0))
		require.Equal(t, storage.LSN(1), logManager.oldestLSN)
		require.Equal(t, storage.LSN(2), logManager.appendedLSN)
		require.Equal(t, storage.LSN(1), logManager.lowWaterMark())

		testhelper.RequireDirectoryState(t, logManager.stateDirectory, "", testhelper.DirectoryState{
			"/":                    {Mode: mode.Directory},
			"/wal":                 {Mode: mode.Directory},
			"/wal/0000000000001":   {Mode: mode.Directory},
			"/wal/0000000000001/1": {Mode: mode.File, Content: []byte("content-1")},
			"/wal/0000000000002":   {Mode: mode.Directory},
			"/wal/0000000000002/1": {Mode: mode.File, Content: []byte("content-2")},
		})
	})

	t.Run("existing WAL entries with appliedLSN in-between", func(t *testing.T) {
		t.Parallel()
		ctx := testhelper.Context(t)
		stateDir := testhelper.TempDir(t)

		logManager := NewLogManager("test-storage", 1, testhelper.TempDir(t), stateDir, nil)
		require.NoError(t, logManager.createStateDirectory(ctx))

		for i := 0; i < 3; i++ {
			appendLogEntry(t, ctx, logManager, map[string][]byte{"1": []byte(fmt.Sprintf("content-%d", i+1))})
		}

		logManager = NewLogManager("test-storage", 1, testhelper.TempDir(t), stateDir, nil)
		require.NoError(t, logManager.Initialize(ctx, 2))

		require.Equal(t, storage.LSN(1), logManager.oldestLSN)
		require.Equal(t, storage.LSN(3), logManager.appendedLSN)
		require.Equal(t, storage.LSN(3), logManager.lowWaterMark())

		testhelper.RequireDirectoryState(t, logManager.stateDirectory, "", testhelper.DirectoryState{
			"/":                    {Mode: mode.Directory},
			"/wal":                 {Mode: mode.Directory},
			"/wal/0000000000001":   {Mode: mode.Directory},
			"/wal/0000000000001/1": {Mode: mode.File, Content: []byte("content-1")},
			"/wal/0000000000002":   {Mode: mode.Directory},
			"/wal/0000000000002/1": {Mode: mode.File, Content: []byte("content-2")},
			"/wal/0000000000003":   {Mode: mode.Directory},
			"/wal/0000000000003/1": {Mode: mode.File, Content: []byte("content-3")},
		})
	})

	t.Run("existing WAL entries with up-to-date appliedLSN", func(t *testing.T) {
		t.Parallel()
		ctx := testhelper.Context(t)
		stateDir := testhelper.TempDir(t)
		logManager := NewLogManager("test-storage", 1, testhelper.TempDir(t), stateDir, nil)
		require.NoError(t, logManager.Initialize(ctx, 0))

		for i := 0; i < 3; i++ {
			appendLogEntry(t, ctx, logManager, map[string][]byte{"1": []byte(fmt.Sprintf("content-%d", i+1))})
		}

		logManager = NewLogManager("test-storage", 1, testhelper.TempDir(t), stateDir, nil)
		require.NoError(t, logManager.Initialize(ctx, 3))

		require.Equal(t, storage.LSN(1), logManager.oldestLSN)
		require.Equal(t, storage.LSN(3), logManager.appendedLSN)
		require.Equal(t, storage.LSN(4), logManager.lowWaterMark())

		testhelper.RequireDirectoryState(t, logManager.stateDirectory, "", testhelper.DirectoryState{
			"/":                    {Mode: mode.Directory},
			"/wal":                 {Mode: mode.Directory},
			"/wal/0000000000001":   {Mode: mode.Directory},
			"/wal/0000000000001/1": {Mode: mode.File, Content: []byte("content-1")},
			"/wal/0000000000002":   {Mode: mode.Directory},
			"/wal/0000000000002/1": {Mode: mode.File, Content: []byte("content-2")},
			"/wal/0000000000003":   {Mode: mode.Directory},
			"/wal/0000000000003/1": {Mode: mode.File, Content: []byte("content-3")},
		})
	})
}

func TestLogManager_PruneLogEntries(t *testing.T) {
	t.Parallel()

	t.Run("no entries to remove", func(t *testing.T) {
		t.Parallel()
		ctx := testhelper.Context(t)
		logManager := setupLogManager(t, ctx, nil)

		// Expect no entries to be removed
		require.NoError(t, logManager.PruneLogEntries(ctx))
		require.Equal(t, storage.LSN(1), logManager.oldestLSN)

		// Assert on-disk state
		testhelper.RequireDirectoryState(t, logManager.stateDirectory, "", testhelper.DirectoryState{
			"/":    {Mode: mode.Directory},
			"/wal": {Mode: mode.Directory},
		})
	})

	t.Run("remove single applied entry", func(t *testing.T) {
		t.Parallel()
		ctx := testhelper.Context(t)
		logManager := setupLogManager(t, ctx, nil)

		// Inject a single log entry
		appendLogEntry(t, ctx, logManager, map[string][]byte{"1": []byte("content-1")})

		// Set this entry as applied
		logManager.AcknowledgeAppliedPosition(1)

		// Before removal
		testhelper.RequireDirectoryState(t, logManager.stateDirectory, "", testhelper.DirectoryState{
			"/":                    {Mode: mode.Directory},
			"/wal":                 {Mode: mode.Directory},
			"/wal/0000000000001":   {Mode: mode.Directory},
			"/wal/0000000000001/1": {Mode: mode.File, Content: []byte("content-1")},
		})

		// Attempt to remove applied log entries
		require.NoError(t, logManager.PruneLogEntries(ctx))

		// After removal
		require.Equal(t, storage.LSN(2), logManager.oldestLSN)
		testhelper.RequireDirectoryState(t, logManager.stateDirectory, "", testhelper.DirectoryState{
			"/":    {Mode: mode.Directory},
			"/wal": {Mode: mode.Directory},
		})
	})

	t.Run("retain entry due to low-water mark constraint", func(t *testing.T) {
		t.Parallel()
		ctx := testhelper.Context(t)
		logManager := setupLogManager(t, ctx, &mockLogConsumer{})

		// Inject multiple log entries
		for i := 0; i < 3; i++ {
			appendLogEntry(t, ctx, logManager, map[string][]byte{"1": []byte(fmt.Sprintf("content-%d", i+1))})
		}

		// Set the applied LSN to 2
		logManager.AcknowledgeAppliedPosition(2)
		// Manually set the consumer's position to the first entry, forcing low-water mark to retain it
		logManager.AcknowledgeConsumerPosition(1)

		// Before removal
		testhelper.RequireDirectoryState(t, logManager.stateDirectory, "", testhelper.DirectoryState{
			"/":                    {Mode: mode.Directory},
			"/wal":                 {Mode: mode.Directory},
			"/wal/0000000000001":   {Mode: mode.Directory},
			"/wal/0000000000001/1": {Mode: mode.File, Content: []byte("content-1")},
			"/wal/0000000000002":   {Mode: mode.Directory},
			"/wal/0000000000002/1": {Mode: mode.File, Content: []byte("content-2")},
			"/wal/0000000000003":   {Mode: mode.Directory},
			"/wal/0000000000003/1": {Mode: mode.File, Content: []byte("content-3")},
		})

		require.NoError(t, logManager.PruneLogEntries(ctx))
		require.Equal(t, storage.LSN(2), logManager.oldestLSN)

		// Assert on-disk state to ensure no entries were removed
		testhelper.RequireDirectoryState(t, logManager.stateDirectory, "", testhelper.DirectoryState{
			"/":                    {Mode: mode.Directory},
			"/wal":                 {Mode: mode.Directory},
			"/wal/0000000000002":   {Mode: mode.Directory},
			"/wal/0000000000002/1": {Mode: mode.File, Content: []byte("content-2")},
			"/wal/0000000000003":   {Mode: mode.Directory},
			"/wal/0000000000003/1": {Mode: mode.File, Content: []byte("content-3")},
		})
	})

	t.Run("remove multiple applied entries", func(t *testing.T) {
		t.Parallel()
		ctx := testhelper.Context(t)
		logManager := setupLogManager(t, ctx, nil)

		// Inject multiple log entries
		for i := 0; i < 5; i++ {
			appendLogEntry(t, ctx, logManager, map[string][]byte{"1": []byte(fmt.Sprintf("content-%d", i+1))})
		}

		// Set the applied LSN to 3, allowing the first three entries to be pruned
		logManager.AcknowledgeAppliedPosition(3)

		// Before removal
		testhelper.RequireDirectoryState(t, logManager.stateDirectory, "", testhelper.DirectoryState{
			"/":                    {Mode: mode.Directory},
			"/wal":                 {Mode: mode.Directory},
			"/wal/0000000000001":   {Mode: mode.Directory},
			"/wal/0000000000001/1": {Mode: mode.File, Content: []byte("content-1")},
			"/wal/0000000000002":   {Mode: mode.Directory},
			"/wal/0000000000002/1": {Mode: mode.File, Content: []byte("content-2")},
			"/wal/0000000000003":   {Mode: mode.Directory},
			"/wal/0000000000003/1": {Mode: mode.File, Content: []byte("content-3")},
			"/wal/0000000000004":   {Mode: mode.Directory},
			"/wal/0000000000004/1": {Mode: mode.File, Content: []byte("content-4")},
			"/wal/0000000000005":   {Mode: mode.Directory},
			"/wal/0000000000005/1": {Mode: mode.File, Content: []byte("content-5")},
		})

		require.NoError(t, logManager.PruneLogEntries(ctx))

		// Ensure only entries starting from LSN 4 are retained
		require.Equal(t, storage.LSN(4), logManager.oldestLSN)

		// Assert on-disk state after removals
		testhelper.RequireDirectoryState(t, logManager.stateDirectory, "", testhelper.DirectoryState{
			"/":                    {Mode: mode.Directory},
			"/wal":                 {Mode: mode.Directory},
			"/wal/0000000000004":   {Mode: mode.Directory},
			"/wal/0000000000004/1": {Mode: mode.File, Content: []byte("content-4")},
			"/wal/0000000000005":   {Mode: mode.Directory},
			"/wal/0000000000005/1": {Mode: mode.File, Content: []byte("content-5")},
		})
	})
}

func TestLogManager_AppendLogEntry(t *testing.T) {
	t.Parallel()

	t.Run("append a log entry with a single file", func(t *testing.T) {
		t.Parallel()

		ctx := testhelper.Context(t)
		logManager := setupLogManager(t, ctx, nil)

		require.Equal(t, logManager.appendedLSN, storage.LSN(0))

		appendLogEntry(t, ctx, logManager, map[string][]byte{"1": []byte("content-1")})

		require.Equal(t, logManager.appendedLSN, storage.LSN(1))
		testhelper.RequireDirectoryState(t, logManager.stateDirectory, "", testhelper.DirectoryState{
			"/":                    {Mode: mode.Directory},
			"/wal":                 {Mode: mode.Directory},
			"/wal/0000000000001":   {Mode: mode.Directory},
			"/wal/0000000000001/1": {Mode: mode.File, Content: []byte("content-1")},
		})
	})

	t.Run("append a log entry with multiple files", func(t *testing.T) {
		t.Parallel()

		ctx := testhelper.Context(t)
		logManager := setupLogManager(t, ctx, nil)

		require.Equal(t, logManager.appendedLSN, storage.LSN(0))

		appendLogEntry(t, ctx, logManager, map[string][]byte{
			"1": []byte("content-1"),
			"2": []byte("content-2"),
			"3": []byte("content-3"),
		})

		require.Equal(t, logManager.appendedLSN, storage.LSN(1))
		testhelper.RequireDirectoryState(t, logManager.stateDirectory, "", testhelper.DirectoryState{
			"/":                    {Mode: mode.Directory},
			"/wal":                 {Mode: mode.Directory},
			"/wal/0000000000001":   {Mode: mode.Directory},
			"/wal/0000000000001/1": {Mode: mode.File, Content: []byte("content-1")},
			"/wal/0000000000001/2": {Mode: mode.File, Content: []byte("content-2")},
			"/wal/0000000000001/3": {Mode: mode.File, Content: []byte("content-3")},
		})
	})

	t.Run("append multiple entries", func(t *testing.T) {
		t.Parallel()

		ctx := testhelper.Context(t)
		logManager := setupLogManager(t, ctx, nil)

		require.Equal(t, logManager.appendedLSN, storage.LSN(0))

		appendLogEntry(t, ctx, logManager, map[string][]byte{"1": []byte("content-1")})
		appendLogEntry(t, ctx, logManager, map[string][]byte{"1": []byte("content-2-1"), "2": []byte("content-2-2")})
		appendLogEntry(t, ctx, logManager, nil)

		require.Equal(t, logManager.appendedLSN, storage.LSN(3))
		testhelper.RequireDirectoryState(t, logManager.stateDirectory, "", testhelper.DirectoryState{
			"/":                    {Mode: mode.Directory},
			"/wal":                 {Mode: mode.Directory},
			"/wal/0000000000001":   {Mode: mode.Directory},
			"/wal/0000000000001/1": {Mode: mode.File, Content: []byte("content-1")},
			"/wal/0000000000002":   {Mode: mode.Directory},
			"/wal/0000000000002/1": {Mode: mode.File, Content: []byte("content-2-1")},
			"/wal/0000000000002/2": {Mode: mode.File, Content: []byte("content-2-2")},
			"/wal/0000000000003":   {Mode: mode.Directory},
		})
	})
}

type mockLogConsumer struct {
	mu        sync.Mutex
	positions [][]storage.LSN
}

func (c *mockLogConsumer) NotifyNewTransactions(storageName string, partitionID storage.PartitionID, oldestLSN, appendedLSN storage.LSN) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.positions = append(c.positions, []storage.LSN{oldestLSN, appendedLSN})
}

func TestLogManager_Positions(t *testing.T) {
	ctx := testhelper.Context(t)

	simulatePositions := func(t *testing.T, logManager *LogManager, consumed storage.LSN, applied storage.LSN) {
		logManager.AcknowledgeConsumerPosition(consumed)
		logManager.AcknowledgeAppliedPosition(applied)
		require.NoError(t, logManager.PruneLogEntries(ctx))
	}

	t.Run("consumer pos is set to 0 after initialized", func(t *testing.T) {
		mockConsumer := &mockLogConsumer{}
		logManager := setupLogManager(t, ctx, mockConsumer)

		require.Equal(t, [][]storage.LSN(nil), mockConsumer.positions)
		require.Equal(t, storage.LSN(1), logManager.lowWaterMark())

		testhelper.RequireDirectoryState(t, logManager.stateDirectory, "", testhelper.DirectoryState{
			"/":    {Mode: mode.Directory},
			"/wal": {Mode: mode.Directory},
		})
	})

	t.Run("notify consumer after restart", func(t *testing.T) {
		stateDir := testhelper.TempDir(t)

		// Before restart
		mockConsumer := &mockLogConsumer{}

		logManager := NewLogManager("test-storage", 1, testhelper.TempDir(t), stateDir, mockConsumer)
		require.NoError(t, logManager.Initialize(ctx, 0))

		appendLogEntry(t, ctx, logManager, map[string][]byte{"1": []byte("content-1")})
		appendLogEntry(t, ctx, logManager, map[string][]byte{"1": []byte("content-2")})

		// Apply to 3 but consume to 1
		simulatePositions(t, logManager, 1, 2)
		require.Equal(t, [][]storage.LSN{{1, 1}, {1, 2}}, mockConsumer.positions)
		require.Equal(t, storage.LSN(2), logManager.lowWaterMark())

		// Inject 3, 4
		appendLogEntry(t, ctx, logManager, map[string][]byte{"1": []byte("content-3")})
		appendLogEntry(t, ctx, logManager, map[string][]byte{"1": []byte("content-4")})

		testhelper.RequireDirectoryState(t, logManager.stateDirectory, "", testhelper.DirectoryState{
			"/":                    {Mode: mode.Directory},
			"/wal":                 {Mode: mode.Directory},
			"/wal/0000000000002":   {Mode: mode.Directory},
			"/wal/0000000000002/1": {Mode: mode.File, Content: []byte("content-2")},
			"/wal/0000000000003":   {Mode: mode.Directory},
			"/wal/0000000000003/1": {Mode: mode.File, Content: []byte("content-3")},
			"/wal/0000000000004":   {Mode: mode.Directory},
			"/wal/0000000000004/1": {Mode: mode.File, Content: []byte("content-4")},
		})

		// Restart the log consumer.
		mockConsumer = &mockLogConsumer{}
		logManager = NewLogManager("test-storage", 1, testhelper.TempDir(t), stateDir, mockConsumer)
		require.NoError(t, logManager.Initialize(ctx, 2))

		// Notify consumer to consume from 2 -> 4
		require.Equal(t, [][]storage.LSN{{2, 4}}, mockConsumer.positions)

		// Both consumer and applier catch up.
		simulatePositions(t, logManager, 4, 4)

		// All log entries are pruned at this point. The consumer should not be notified again.
		require.Equal(t, [][]storage.LSN{{2, 4}}, mockConsumer.positions)
		require.Equal(t, storage.LSN(5), logManager.lowWaterMark())
		testhelper.RequireDirectoryState(t, logManager.stateDirectory, "", testhelper.DirectoryState{
			"/":    {Mode: mode.Directory},
			"/wal": {Mode: mode.Directory},
		})
	})

	t.Run("unacknowledged entries are not pruned", func(t *testing.T) {
		mockConsumer := &mockLogConsumer{}
		logManager := setupLogManager(t, ctx, mockConsumer)

		appendLogEntry(t, ctx, logManager, map[string][]byte{"1": []byte("content-1")})
		appendLogEntry(t, ctx, logManager, map[string][]byte{"1": []byte("content-2")})

		simulatePositions(t, logManager, 0, 2)

		require.Equal(t, [][]storage.LSN{{1, 1}, {1, 2}}, mockConsumer.positions)
		require.Equal(t, storage.LSN(1), logManager.lowWaterMark())

		testhelper.RequireDirectoryState(t, logManager.stateDirectory, "", testhelper.DirectoryState{
			"/":                    {Mode: mode.Directory},
			"/wal":                 {Mode: mode.Directory},
			"/wal/0000000000001":   {Mode: mode.Directory},
			"/wal/0000000000001/1": {Mode: mode.File, Content: []byte("content-1")},
			"/wal/0000000000002":   {Mode: mode.Directory},
			"/wal/0000000000002/1": {Mode: mode.File, Content: []byte("content-2")},
		})
	})

	t.Run("acknowledged entries got pruned", func(t *testing.T) {
		mockConsumer := &mockLogConsumer{}
		logManager := setupLogManager(t, ctx, mockConsumer)

		appendLogEntry(t, ctx, logManager, map[string][]byte{"1": []byte("content-1")})
		appendLogEntry(t, ctx, logManager, map[string][]byte{"1": []byte("content-2")})

		simulatePositions(t, logManager, 1, 2)

		require.Equal(t, [][]storage.LSN{{1, 1}, {1, 2}}, mockConsumer.positions)
		require.Equal(t, storage.LSN(2), logManager.lowWaterMark())

		testhelper.RequireDirectoryState(t, logManager.stateDirectory, "", testhelper.DirectoryState{
			"/":                    {Mode: mode.Directory},
			"/wal":                 {Mode: mode.Directory},
			"/wal/0000000000002":   {Mode: mode.Directory},
			"/wal/0000000000002/1": {Mode: mode.File, Content: []byte("content-2")},
		})
	})

	t.Run("entries consumed faster than applied", func(t *testing.T) {
		mockConsumer := &mockLogConsumer{}
		logManager := setupLogManager(t, ctx, mockConsumer)

		appendLogEntry(t, ctx, logManager, map[string][]byte{"1": []byte("content-1")})
		appendLogEntry(t, ctx, logManager, map[string][]byte{"1": []byte("content-2")})

		simulatePositions(t, logManager, 2, 0)

		require.Equal(t, [][]storage.LSN{{1, 1}, {1, 2}}, mockConsumer.positions)
		require.Equal(t, storage.LSN(1), logManager.lowWaterMark())

		testhelper.RequireDirectoryState(t, logManager.stateDirectory, "", testhelper.DirectoryState{
			"/":                    {Mode: mode.Directory},
			"/wal":                 {Mode: mode.Directory},
			"/wal/0000000000001":   {Mode: mode.Directory},
			"/wal/0000000000001/1": {Mode: mode.File, Content: []byte("content-1")},
			"/wal/0000000000002":   {Mode: mode.Directory},
			"/wal/0000000000002/1": {Mode: mode.File, Content: []byte("content-2")},
		})
	})

	t.Run("acknowledge entries one by one", func(t *testing.T) {
		mockConsumer := &mockLogConsumer{}
		logManager := setupLogManager(t, ctx, mockConsumer)

		appendLogEntry(t, ctx, logManager, map[string][]byte{"1": []byte("content-1")})
		simulatePositions(t, logManager, 1, 1)

		appendLogEntry(t, ctx, logManager, map[string][]byte{"1": []byte("content-2")})
		simulatePositions(t, logManager, 2, 2)

		// The oldest LSN changes after each acknowledgement
		require.Equal(t, [][]storage.LSN{{1, 1}, {2, 2}}, mockConsumer.positions)
		require.Equal(t, storage.LSN(3), logManager.lowWaterMark())

		testhelper.RequireDirectoryState(t, logManager.stateDirectory, "", testhelper.DirectoryState{
			"/":    {Mode: mode.Directory},
			"/wal": {Mode: mode.Directory},
		})
	})

	t.Run("append while consumer is busy with prior entries", func(t *testing.T) {
		mockConsumer := &mockLogConsumer{}
		logManager := setupLogManager(t, ctx, mockConsumer)

		appendLogEntry(t, ctx, logManager, map[string][]byte{"1": []byte("content-1")})
		simulatePositions(t, logManager, 0, 1)

		appendLogEntry(t, ctx, logManager, map[string][]byte{"1": []byte("content-2")})
		simulatePositions(t, logManager, 0, 2)

		appendLogEntry(t, ctx, logManager, map[string][]byte{"1": []byte("content-3")})
		simulatePositions(t, logManager, 3, 3)

		require.Equal(t, storage.LSN(4), logManager.lowWaterMark())
		testhelper.RequireDirectoryState(t, logManager.stateDirectory, "", testhelper.DirectoryState{
			"/":    {Mode: mode.Directory},
			"/wal": {Mode: mode.Directory},
		})
	})

	t.Run("acknowledged entries not pruned if not applied", func(t *testing.T) {
		mockConsumer := &mockLogConsumer{}
		logManager := setupLogManager(t, ctx, mockConsumer)

		appendLogEntry(t, ctx, logManager, map[string][]byte{"1": []byte("content-1")})
		appendLogEntry(t, ctx, logManager, map[string][]byte{"1": []byte("content-2")})
		appendLogEntry(t, ctx, logManager, map[string][]byte{"1": []byte("content-3")})

		// 2 and 3 are not applied, hence kept intact.
		simulatePositions(t, logManager, 3, 1)

		require.Equal(t, storage.LSN(2), logManager.lowWaterMark())

		testhelper.RequireDirectoryState(t, logManager.stateDirectory, "", testhelper.DirectoryState{
			"/":                    {Mode: mode.Directory},
			"/wal":                 {Mode: mode.Directory},
			"/wal/0000000000002":   {Mode: mode.Directory},
			"/wal/0000000000002/1": {Mode: mode.File, Content: []byte("content-2")},
			"/wal/0000000000003":   {Mode: mode.Directory},
			"/wal/0000000000003/1": {Mode: mode.File, Content: []byte("content-3")},
		})

		simulatePositions(t, logManager, 3, 3)
		require.Equal(t, storage.LSN(4), logManager.lowWaterMark())
		testhelper.RequireDirectoryState(t, logManager.stateDirectory, "", testhelper.DirectoryState{
			"/":    {Mode: mode.Directory},
			"/wal": {Mode: mode.Directory},
		})
	})
}
