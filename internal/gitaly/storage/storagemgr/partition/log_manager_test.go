package partition

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/mode"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
	"google.golang.org/protobuf/proto"
)

// RefChange represents a reference change operation.
type RefChange struct {
	Ref string
	Oid git.ObjectID
}

// MultiplerefChangesLogEntry returns a log entry as a result of multiple reference update operations.
func MultiplerefChangesLogEntry(relativePath string, changes []RefChange) *gitalypb.LogEntry {
	entry := &gitalypb.LogEntry{RelativePath: relativePath}

	for i, change := range changes {
		entry.ReferenceTransactions = append(entry.ReferenceTransactions, &gitalypb.LogEntry_ReferenceTransaction{
			Changes: []*gitalypb.LogEntry_ReferenceTransaction_Change{
				{
					ReferenceName: []byte(change.Ref),
					NewOid:        []byte(change.Oid),
				},
			},
		})
		entry.Operations = append(entry.Operations, &gitalypb.LogEntry_Operation{
			Operation: &gitalypb.LogEntry_Operation_CreateHardLink_{
				CreateHardLink: &gitalypb.LogEntry_Operation_CreateHardLink{
					SourcePath:      []byte(fmt.Sprintf("%d", i+1)),
					DestinationPath: []byte(filepath.Join(relativePath, change.Ref)),
				},
			},
		})
	}

	if testhelper.IsReftableEnabled() {
		entry.Operations = append(entry.Operations, &gitalypb.LogEntry_Operation{
			Operation: &gitalypb.LogEntry_Operation_RemoveDirectoryEntry_{
				RemoveDirectoryEntry: &gitalypb.LogEntry_Operation_RemoveDirectoryEntry{
					Path: []byte(filepath.Join(relativePath, "reftable/tables.list")),
				},
			},
		}, &gitalypb.LogEntry_Operation{
			Operation: &gitalypb.LogEntry_Operation_CreateHardLink_{
				CreateHardLink: &gitalypb.LogEntry_Operation_CreateHardLink{
					SourcePath:      []byte("2"),
					DestinationPath: []byte(filepath.Join(relativePath, "reftable/tables.list")),
				},
			},
		})
	}

	return entry
}

// KVOperation represents a KV operation. Setting Value to nil is translated to key deletion.
type KVOperation struct {
	Key   []byte
	Value []byte
}

// MultipleKVLogEntry returns a log entry as a result of multiple key-value operations.
func MultipleKVLogEntry(relativePath string, ops []KVOperation) *gitalypb.LogEntry {
	entry := &gitalypb.LogEntry{RelativePath: relativePath}

	for _, op := range ops {
		if op.Value == nil {
			entry.Operations = append(entry.Operations, &gitalypb.LogEntry_Operation{
				Operation: &gitalypb.LogEntry_Operation_DeleteKey_{
					DeleteKey: &gitalypb.LogEntry_Operation_DeleteKey{
						Key: op.Key,
					},
				},
			})
		} else {
			entry.Operations = append(entry.Operations, &gitalypb.LogEntry_Operation{
				Operation: &gitalypb.LogEntry_Operation_SetKey_{
					SetKey: &gitalypb.LogEntry_Operation_SetKey{
						Key:   op.Key,
						Value: op.Value,
					},
				},
			})
		}
	}
	return entry
}

func appendLogEntry(t *testing.T, ctx context.Context, manager *LogManager, logEntry *gitalypb.LogEntry, files map[string][]byte) storage.LSN {
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

func injectLogEntry(t *testing.T, ctx context.Context, manager *LogManager, lsn storage.LSN, logEntry *gitalypb.LogEntry, files map[string][]byte) {
	t.Helper()

	require.NoError(t, manager.createStateDirectory(ctx))
	logEntryPath := manager.GetEntryPath(lsn)
	require.NoError(t, os.MkdirAll(logEntryPath, mode.Directory))

	for name, value := range files {
		path := filepath.Join(logEntryPath, name)
		require.NoError(t, os.WriteFile(path, value, mode.File))
	}

	manifestBytes, err := proto.Marshal(logEntry)
	require.NoError(t, err)

	manifestPath := manifestPath(logEntryPath)
	require.NoError(t, os.WriteFile(manifestPath, manifestBytes, mode.File))
}

type testLogSetup struct {
	relativePath string
	commitIDs    []git.ObjectID
}

func setupTestRepo(t *testing.T, ctx context.Context, numCommits int) testLogSetup {
	relativePath := gittest.NewRepositoryName(t)
	cfg := testcfg.Build(t)
	_, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
		SkipCreationViaService: true,
		RelativePath:           relativePath,
	})

	setup := testLogSetup{relativePath: repoPath}

	for i := 0; i < numCommits; i++ {
		commitID := gittest.WriteCommit(t, cfg, repoPath, gittest.WithParents())
		setup.commitIDs = append(setup.commitIDs, commitID)
	}

	return setup
}

// GetFileMappingForEntry returns a map where each key is a file name (starting from "1")
// and each value is the corresponding file content.
func (ts *testLogSetup) GetFileMappingForEntry(indices ...int) map[string][]byte {
	mapping := map[string][]byte{}
	for order, index := range indices {
		mapping[fmt.Sprintf("%d", order+1)] = ts.GetFileContent(index)
	}
	return mapping
}

// GetFileContent returns the on-disk content of a ref.
func (ts *testLogSetup) GetFileContent(index int) []byte {
	return []byte(ts.commitIDs[index] + "\n")
}

func setupLogManager(t *testing.T, ctx context.Context, consumer LogConsumer) *LogManager {
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

		setup := setupTestRepo(t, ctx, 2)
		entry1 := MultiplerefChangesLogEntry(setup.relativePath, []RefChange{
			{Ref: "branch-1", Oid: setup.commitIDs[0]},
		})
		entry2 := MultiplerefChangesLogEntry(setup.relativePath, []RefChange{
			{Ref: "branch-2", Oid: setup.commitIDs[1]},
		})

		injectLogEntry(t, ctx, logManager, 1, entry1, setup.GetFileMappingForEntry(0))
		injectLogEntry(t, ctx, logManager, 2, entry2, setup.GetFileMappingForEntry(1))

		require.NoError(t, logManager.Initialize(ctx, 0))
		require.Equal(t, storage.LSN(1), logManager.oldestLSN)
		require.Equal(t, storage.LSN(2), logManager.appendedLSN)
		require.Equal(t, storage.LSN(1), logManager.lowWaterMark())

		testhelper.RequireDirectoryState(t, logManager.stateDirectory, "", testhelper.DirectoryState{
			"/":                           {Mode: mode.Directory},
			"/wal":                        {Mode: mode.Directory},
			"/wal/0000000000001":          {Mode: mode.Directory},
			"/wal/0000000000001/MANIFEST": manifestDirectoryEntry(entry1),
			"/wal/0000000000001/1":        {Mode: mode.File, Content: setup.GetFileContent(0)},
			"/wal/0000000000002":          {Mode: mode.Directory},
			"/wal/0000000000002/MANIFEST": manifestDirectoryEntry(entry2),
			"/wal/0000000000002/1":        {Mode: mode.File, Content: setup.GetFileContent(1)},
		})
	})

	t.Run("existing WAL entries with appliedLSN in-between", func(t *testing.T) {
		t.Parallel()
		ctx := testhelper.Context(t)
		stateDir := testhelper.TempDir(t)

		logManager := NewLogManager("test-storage", 1, testhelper.TempDir(t), stateDir, nil)

		setup := setupTestRepo(t, ctx, 3)
		var entries []*gitalypb.LogEntry
		for i, commitID := range setup.commitIDs {
			entry := MultiplerefChangesLogEntry(setup.relativePath, []RefChange{
				{Ref: fmt.Sprintf("branch-%d", i+1), Oid: commitID},
			})
			injectLogEntry(t, ctx, logManager, storage.LSN(i+1), entry, setup.GetFileMappingForEntry(i))
			entries = append(entries, entry)
		}

		require.NoError(t, logManager.Initialize(ctx, 2))

		require.Equal(t, storage.LSN(1), logManager.oldestLSN)
		require.Equal(t, storage.LSN(3), logManager.appendedLSN)
		require.Equal(t, storage.LSN(3), logManager.lowWaterMark())

		testhelper.RequireDirectoryState(t, logManager.stateDirectory, "", testhelper.DirectoryState{
			"/":                           {Mode: mode.Directory},
			"/wal":                        {Mode: mode.Directory},
			"/wal/0000000000001":          {Mode: mode.Directory},
			"/wal/0000000000001/MANIFEST": manifestDirectoryEntry(entries[0]),
			"/wal/0000000000001/1":        {Mode: mode.File, Content: setup.GetFileContent(0)},
			"/wal/0000000000002":          {Mode: mode.Directory},
			"/wal/0000000000002/MANIFEST": manifestDirectoryEntry(entries[1]),
			"/wal/0000000000002/1":        {Mode: mode.File, Content: setup.GetFileContent(1)},
			"/wal/0000000000003":          {Mode: mode.Directory},
			"/wal/0000000000003/MANIFEST": manifestDirectoryEntry(entries[2]),
			"/wal/0000000000003/1":        {Mode: mode.File, Content: setup.GetFileContent(2)},
		})
	})

	t.Run("existing WAL entries with up-to-date appliedLSN", func(t *testing.T) {
		t.Parallel()
		ctx := testhelper.Context(t)
		stateDir := testhelper.TempDir(t)
		logManager := NewLogManager("test-storage", 1, testhelper.TempDir(t), stateDir, nil)

		setup := setupTestRepo(t, ctx, 0)
		var entries []*gitalypb.LogEntry
		for i := 0; i < 3; i++ {
			entry := MultipleKVLogEntry(setup.relativePath, []KVOperation{
				{Key: []byte(fmt.Sprintf("Key-%d", i+1)), Value: []byte("content")},
			})
			injectLogEntry(t, ctx, logManager, storage.LSN(i+1), entry, nil)
			entries = append(entries, entry)
		}

		require.NoError(t, logManager.Initialize(ctx, 3))

		require.Equal(t, storage.LSN(1), logManager.oldestLSN)
		require.Equal(t, storage.LSN(3), logManager.appendedLSN)
		require.Equal(t, storage.LSN(4), logManager.lowWaterMark())

		testhelper.RequireDirectoryState(t, logManager.stateDirectory, "", testhelper.DirectoryState{
			"/":                           {Mode: mode.Directory},
			"/wal":                        {Mode: mode.Directory},
			"/wal/0000000000001":          {Mode: mode.Directory},
			"/wal/0000000000001/MANIFEST": manifestDirectoryEntry(entries[0]),
			"/wal/0000000000002":          {Mode: mode.Directory},
			"/wal/0000000000002/MANIFEST": manifestDirectoryEntry(entries[1]),
			"/wal/0000000000003":          {Mode: mode.Directory},
			"/wal/0000000000003/MANIFEST": manifestDirectoryEntry(entries[2]),
		})
	})
}

func TestLogManager_PruneLogEntries(t *testing.T) {
	t.Parallel()

	t.Run("no entries to remove", func(t *testing.T) {
		t.Parallel()
		ctx := testhelper.Context(t)
		logManager := setupLogManager(t, ctx, nil)

		removedLSN, err := logManager.PruneLogEntries(ctx)

		// Expect no entries to be removed
		require.NoError(t, err)
		require.Equal(t, storage.LSN(0), removedLSN)
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
		setup := setupTestRepo(t, ctx, 1)
		entry := MultiplerefChangesLogEntry(setup.relativePath, []RefChange{
			{Ref: "branch-1", Oid: setup.commitIDs[0]},
		})
		appendLogEntry(t, ctx, logManager, entry, setup.GetFileMappingForEntry(0))

		// Set this entry as applied
		logManager.AcknowledgeAppliedPos(1)
		logManager.AcknowledgeReferencedPos(1)

		// Attempt to remove applied log entries
		removedLSN, err := logManager.PruneLogEntries(ctx)

		// Expect the first entry to be removed
		require.NoError(t, err)
		require.Equal(t, storage.LSN(1), removedLSN)
		require.Equal(t, storage.LSN(2), logManager.oldestLSN)

		// Assert on-disk state after removal
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
		setup := setupTestRepo(t, ctx, 3)
		entry := func(i int) *gitalypb.LogEntry {
			return MultiplerefChangesLogEntry(setup.relativePath, []RefChange{
				{Ref: fmt.Sprintf("branch-%d", i+1), Oid: setup.commitIDs[i]},
			})
		}
		for i := 0; i < 3; i++ {
			appendLogEntry(t, ctx, logManager, entry(i), setup.GetFileMappingForEntry(i))
		}

		// Set the applied LSN to 2
		logManager.AcknowledgeAppliedPos(2)
		logManager.AcknowledgeReferencedPos(2)
		// Manually set the consumer's position to the first entry, forcing low-water mark to retain it
		logManager.AcknowledgeConsumerPos(1)

		removedLSN, err := logManager.PruneLogEntries(ctx)

		// No entries should be removed because of the low-water mark
		require.NoError(t, err)
		require.Equal(t, storage.LSN(1), removedLSN)
		require.Equal(t, storage.LSN(2), logManager.oldestLSN)

		// Assert on-disk state to ensure no entries were removed
		testhelper.RequireDirectoryState(t, logManager.stateDirectory, "", testhelper.DirectoryState{
			"/":                           {Mode: mode.Directory},
			"/wal":                        {Mode: mode.Directory},
			"/wal/0000000000002":          {Mode: mode.Directory},
			"/wal/0000000000002/MANIFEST": manifestDirectoryEntry(entry(1)),
			"/wal/0000000000002/1":        {Mode: mode.File, Content: setup.GetFileContent(1)},
			"/wal/0000000000003":          {Mode: mode.Directory},
			"/wal/0000000000003/MANIFEST": manifestDirectoryEntry(entry(2)),
			"/wal/0000000000003/1":        {Mode: mode.File, Content: setup.GetFileContent(2)},
		})
	})

	t.Run("remove multiple applied entries", func(t *testing.T) {
		t.Parallel()
		ctx := testhelper.Context(t)
		logManager := setupLogManager(t, ctx, nil)

		// Inject multiple log entries
		setup := setupTestRepo(t, ctx, 5)
		entry := func(i int) *gitalypb.LogEntry {
			return MultiplerefChangesLogEntry(setup.relativePath, []RefChange{
				{Ref: fmt.Sprintf("branch-%d", i+1), Oid: setup.commitIDs[i]},
			})
		}
		for i := 0; i < 5; i++ {
			appendLogEntry(t, ctx, logManager, entry(i), setup.GetFileMappingForEntry(i))
		}

		// Set the applied LSN to 3, allowing the first three entries to be pruned
		logManager.AcknowledgeAppliedPos(3)
		logManager.AcknowledgeReferencedPos(3)

		for i := 0; i < 3; i++ {
			removedLSN, err := logManager.PruneLogEntries(ctx)

			// Expect each entry to be removed in order
			require.NoError(t, err)
			require.Equal(t, storage.LSN(i+1), removedLSN)
		}

		// Ensure only entries starting from LSN 4 are retained
		require.Equal(t, storage.LSN(4), logManager.oldestLSN)

		// Assert on-disk state after removals
		testhelper.RequireDirectoryState(t, logManager.stateDirectory, "", testhelper.DirectoryState{
			"/":                           {Mode: mode.Directory},
			"/wal":                        {Mode: mode.Directory},
			"/wal/0000000000004":          {Mode: mode.Directory},
			"/wal/0000000000004/MANIFEST": manifestDirectoryEntry(entry(3)),
			"/wal/0000000000004/1":        {Mode: mode.File, Content: setup.GetFileContent(3)},
			"/wal/0000000000005":          {Mode: mode.Directory},
			"/wal/0000000000005/MANIFEST": manifestDirectoryEntry(entry(4)),
			"/wal/0000000000005/1":        {Mode: mode.File, Content: setup.GetFileContent(4)},
		})
	})
}

func TestLogManager_AppendLogEntry(t *testing.T) {
	t.Parallel()

	t.Run("append a log entry with a single file", func(t *testing.T) {
		t.Parallel()

		ctx := testhelper.Context(t)
		logManager := setupLogManager(t, ctx, nil)
		setup := setupTestRepo(t, ctx, 1)

		require.Equal(t, logManager.appendedLSN, storage.LSN(0))

		refEntry := MultiplerefChangesLogEntry(setup.relativePath, []RefChange{{Ref: "branch-1", Oid: setup.commitIDs[0]}})
		appendLogEntry(t, ctx, logManager, refEntry, setup.GetFileMappingForEntry(0))

		require.Equal(t, logManager.appendedLSN, storage.LSN(1))
		testhelper.RequireDirectoryState(t, logManager.stateDirectory, "", testhelper.DirectoryState{
			"/":                           {Mode: mode.Directory},
			"/wal":                        {Mode: mode.Directory},
			"/wal/0000000000001":          {Mode: mode.Directory},
			"/wal/0000000000001/MANIFEST": manifestDirectoryEntry(refEntry),
			"/wal/0000000000001/1":        {Mode: mode.File, Content: setup.GetFileContent(0)},
		})
	})

	t.Run("append a log entry with multiple files", func(t *testing.T) {
		t.Parallel()

		ctx := testhelper.Context(t)
		logManager := setupLogManager(t, ctx, nil)
		setup := setupTestRepo(t, ctx, 3)

		require.Equal(t, logManager.appendedLSN, storage.LSN(0))

		refEntry := MultiplerefChangesLogEntry(setup.relativePath, []RefChange{
			{Ref: "branch-1", Oid: setup.commitIDs[0]},
			{Ref: "branch-2", Oid: setup.commitIDs[1]},
			{Ref: "branch-3", Oid: setup.commitIDs[2]},
		})
		appendLogEntry(t, ctx, logManager, refEntry, setup.GetFileMappingForEntry(0, 1, 2))

		require.Equal(t, logManager.appendedLSN, storage.LSN(1))
		testhelper.RequireDirectoryState(t, logManager.stateDirectory, "", testhelper.DirectoryState{
			"/":                           {Mode: mode.Directory},
			"/wal":                        {Mode: mode.Directory},
			"/wal/0000000000001":          {Mode: mode.Directory},
			"/wal/0000000000001/MANIFEST": manifestDirectoryEntry(refEntry),
			"/wal/0000000000001/1":        {Mode: mode.File, Content: setup.GetFileContent(0)},
			"/wal/0000000000001/2":        {Mode: mode.File, Content: setup.GetFileContent(1)},
			"/wal/0000000000001/3":        {Mode: mode.File, Content: setup.GetFileContent(2)},
		})
	})

	t.Run("append multiple entries", func(t *testing.T) {
		t.Parallel()

		ctx := testhelper.Context(t)
		logManager := setupLogManager(t, ctx, nil)
		setup := setupTestRepo(t, ctx, 3)

		require.Equal(t, logManager.appendedLSN, storage.LSN(0))

		refEntry1 := MultiplerefChangesLogEntry(setup.relativePath, []RefChange{
			{Ref: "branch-1", Oid: setup.commitIDs[0]},
		})
		appendLogEntry(t, ctx, logManager, refEntry1, setup.GetFileMappingForEntry(0))

		refEntry2 := MultiplerefChangesLogEntry(setup.relativePath, []RefChange{
			{Ref: "branch-2", Oid: setup.commitIDs[1]},
			{Ref: "branch-3", Oid: setup.commitIDs[2]},
		})
		appendLogEntry(t, ctx, logManager, refEntry2, setup.GetFileMappingForEntry(1, 2))

		kvEntry1 := MultipleKVLogEntry(setup.relativePath, []KVOperation{
			{Key: []byte("key-1"), Value: []byte("value-1")},
			{Key: []byte("key-2"), Value: []byte("value-2")},
		})
		appendLogEntry(t, ctx, logManager, kvEntry1, nil)

		require.Equal(t, logManager.appendedLSN, storage.LSN(3))
		testhelper.RequireDirectoryState(t, logManager.stateDirectory, "", testhelper.DirectoryState{
			"/":                           {Mode: mode.Directory},
			"/wal":                        {Mode: mode.Directory},
			"/wal/0000000000001":          {Mode: mode.Directory},
			"/wal/0000000000001/MANIFEST": manifestDirectoryEntry(refEntry1),
			"/wal/0000000000001/1":        {Mode: mode.File, Content: setup.GetFileContent(0)},
			"/wal/0000000000002":          {Mode: mode.Directory},
			"/wal/0000000000002/MANIFEST": manifestDirectoryEntry(refEntry2),
			"/wal/0000000000002/1":        {Mode: mode.File, Content: setup.GetFileContent(1)},
			"/wal/0000000000002/2":        {Mode: mode.File, Content: setup.GetFileContent(2)},
			"/wal/0000000000003":          {Mode: mode.Directory},
			"/wal/0000000000003/MANIFEST": manifestDirectoryEntry(kvEntry1),
		})
	})

	t.Run("crashed before appending log entries", func(t *testing.T) {
		t.Parallel()

		ctx := testhelper.Context(t)
		logManager := setupLogManager(t, ctx, nil)
		setup := setupTestRepo(t, ctx, 3)

		logManager.testHooks.beforeAppendLogEntry = func(lsn storage.LSN) {
			if lsn == 2 {
				panic("crash please")
			}
		}

		refEntry1 := MultiplerefChangesLogEntry(setup.relativePath, []RefChange{
			{Ref: "branch-1", Oid: setup.commitIDs[0]},
		})
		kvEntry1 := MultipleKVLogEntry(setup.relativePath, []KVOperation{
			{Key: []byte("key-1"), Value: []byte("value-1")},
			{Key: []byte("key-2"), Value: []byte("value-2")},
		})
		refEntry2 := MultiplerefChangesLogEntry(setup.relativePath, []RefChange{
			{Ref: "branch-2", Oid: setup.commitIDs[1]},
			{Ref: "branch-3", Oid: setup.commitIDs[2]},
		})

		require.Equal(t, logManager.appendedLSN, storage.LSN(0))
		func() {
			defer func() {
				p := recover()
				if p == nil {
					require.FailNow(t, "there should be a crash")
				}
			}()
			appendLogEntry(t, ctx, logManager, refEntry1, setup.GetFileMappingForEntry(0))
			// There should be a crash here.
			appendLogEntry(t, ctx, logManager, kvEntry1, nil)
			// The following proposal is not reached, so it should not be recorded
			appendLogEntry(t, ctx, logManager, refEntry2, setup.GetFileMappingForEntry(1, 2))
		}()

		require.Equal(t, logManager.appendedLSN, storage.LSN(1))
		testhelper.RequireDirectoryState(t, logManager.stateDirectory, "", testhelper.DirectoryState{
			"/":                           {Mode: mode.Directory},
			"/wal":                        {Mode: mode.Directory},
			"/wal/0000000000001":          {Mode: mode.Directory},
			"/wal/0000000000001/MANIFEST": manifestDirectoryEntry(refEntry1),
			"/wal/0000000000001/1":        {Mode: mode.File, Content: setup.GetFileContent(0)},
		})

		// Remove the hook and retry the last proposal.
		logManager.testHooks.beforeAppendLogEntry = func(lsn storage.LSN) {}
		appendLogEntry(t, ctx, logManager, refEntry2, setup.GetFileMappingForEntry(1, 2))

		require.Equal(t, logManager.appendedLSN, storage.LSN(2))
		testhelper.RequireDirectoryState(t, logManager.stateDirectory, "", testhelper.DirectoryState{
			"/":                           {Mode: mode.Directory},
			"/wal":                        {Mode: mode.Directory},
			"/wal/0000000000001":          {Mode: mode.Directory},
			"/wal/0000000000001/MANIFEST": manifestDirectoryEntry(refEntry1),
			"/wal/0000000000001/1":        {Mode: mode.File, Content: setup.GetFileContent(0)},
			"/wal/0000000000002":          {Mode: mode.Directory},
			"/wal/0000000000002/MANIFEST": manifestDirectoryEntry(refEntry2),
			"/wal/0000000000002/1":        {Mode: mode.File, Content: setup.GetFileContent(1)},
			"/wal/0000000000002/2":        {Mode: mode.File, Content: setup.GetFileContent(2)},
		})
	})
}

type mockLogConsumer struct {
	mu        sync.Mutex
	positions [][]storage.LSN
	busy      atomic.Bool
}

func (c *mockLogConsumer) NotifyNewTransactions(storageName string, partitionID storage.PartitionID, oldestLSN, appendedLSN storage.LSN) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.busy.Load() {
		return
	}
	c.positions = append(c.positions, []storage.LSN{oldestLSN, appendedLSN})
}

func TestLogManager_Positions(t *testing.T) {
	ctx := testhelper.Context(t)

	simulatePositions := func(t *testing.T, logManager *LogManager, consumed storage.LSN, applied storage.LSN, referenced storage.LSN) {
		logManager.AcknowledgeConsumerPos(consumed)
		logManager.AcknowledgeAppliedPos(applied)
		logManager.AcknowledgeReferencedPos(referenced)
		for {
			removedLSN, err := logManager.PruneLogEntries(ctx)
			require.NoError(t, err)
			if removedLSN == 0 {
				break
			}
		}
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
		setup := setupTestRepo(t, ctx, 4)

		entry1 := MultiplerefChangesLogEntry(setup.relativePath, []RefChange{{Ref: "refs/heads/main", Oid: setup.commitIDs[0]}})
		appendLogEntry(t, ctx, logManager, entry1, setup.GetFileMappingForEntry(0))

		entry2 := MultiplerefChangesLogEntry(setup.relativePath, []RefChange{{Ref: "refs/heads/main", Oid: setup.commitIDs[1]}})
		appendLogEntry(t, ctx, logManager, entry2, setup.GetFileMappingForEntry(1))

		// Apply to 3 but consume to 1
		simulatePositions(t, logManager, 1, 2, 1)
		require.Equal(t, [][]storage.LSN{{1, 1}, {1, 2}}, mockConsumer.positions)
		require.Equal(t, storage.LSN(2), logManager.lowWaterMark())

		// Inject 3, 4
		entry3 := MultiplerefChangesLogEntry(setup.relativePath, []RefChange{{Ref: "refs/heads/main", Oid: setup.commitIDs[2]}})
		injectLogEntry(t, ctx, logManager, 3, entry3, setup.GetFileMappingForEntry(2))
		entry4 := MultiplerefChangesLogEntry(setup.relativePath, []RefChange{{Ref: "refs/heads/main", Oid: setup.commitIDs[3]}})
		injectLogEntry(t, ctx, logManager, 4, entry4, setup.GetFileMappingForEntry(3))

		testhelper.RequireDirectoryState(t, logManager.stateDirectory, "", testhelper.DirectoryState{
			"/":                           {Mode: mode.Directory},
			"/wal":                        {Mode: mode.Directory},
			"/wal/0000000000002":          {Mode: mode.Directory},
			"/wal/0000000000002/MANIFEST": manifestDirectoryEntry(entry2),
			"/wal/0000000000002/1":        {Mode: mode.File, Content: setup.GetFileContent(1)},
			"/wal/0000000000003":          {Mode: mode.Directory},
			"/wal/0000000000003/MANIFEST": manifestDirectoryEntry(entry3),
			"/wal/0000000000003/1":        {Mode: mode.File, Content: setup.GetFileContent(2)},
			"/wal/0000000000004":          {Mode: mode.Directory},
			"/wal/0000000000004/MANIFEST": manifestDirectoryEntry(entry4),
			"/wal/0000000000004/1":        {Mode: mode.File, Content: setup.GetFileContent(3)},
		})

		// Restart the log consumer.
		mockConsumer = &mockLogConsumer{}
		logManager = NewLogManager("test-storage", 1, testhelper.TempDir(t), stateDir, mockConsumer)
		require.NoError(t, logManager.Initialize(ctx, 2))

		// Notify consumer to consume from 2 -> 4
		require.Equal(t, [][]storage.LSN{{2, 4}}, mockConsumer.positions)

		// Both consumer and applier catch up.
		simulatePositions(t, logManager, 4, 4, 4)

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
		setup := setupTestRepo(t, ctx, 2)

		entry1 := MultiplerefChangesLogEntry(setup.relativePath, []RefChange{{Ref: "refs/heads/main", Oid: setup.commitIDs[0]}})
		appendLogEntry(t, ctx, logManager, entry1, setup.GetFileMappingForEntry(0))

		entry2 := MultiplerefChangesLogEntry(setup.relativePath, []RefChange{{Ref: "refs/heads/main", Oid: setup.commitIDs[1]}})
		appendLogEntry(t, ctx, logManager, entry2, setup.GetFileMappingForEntry(1))

		simulatePositions(t, logManager, 0, 2, 1)

		require.Equal(t, [][]storage.LSN{{1, 1}, {1, 2}}, mockConsumer.positions)
		require.Equal(t, storage.LSN(1), logManager.lowWaterMark())

		testhelper.RequireDirectoryState(t, logManager.stateDirectory, "", testhelper.DirectoryState{
			"/":                           {Mode: mode.Directory},
			"/wal":                        {Mode: mode.Directory},
			"/wal/0000000000001":          {Mode: mode.Directory},
			"/wal/0000000000001/MANIFEST": manifestDirectoryEntry(entry1),
			"/wal/0000000000001/1":        {Mode: mode.File, Content: setup.GetFileContent(0)},
			"/wal/0000000000002":          {Mode: mode.Directory},
			"/wal/0000000000002/MANIFEST": manifestDirectoryEntry(entry2),
			"/wal/0000000000002/1":        {Mode: mode.File, Content: setup.GetFileContent(1)},
		})
	})

	t.Run("acknowledged entries got pruned", func(t *testing.T) {
		mockConsumer := &mockLogConsumer{}
		logManager := setupLogManager(t, ctx, mockConsumer)
		setup := setupTestRepo(t, ctx, 2)

		entry1 := MultiplerefChangesLogEntry(setup.relativePath, []RefChange{{Ref: "refs/heads/main", Oid: setup.commitIDs[0]}})
		appendLogEntry(t, ctx, logManager, entry1, setup.GetFileMappingForEntry(0))

		entry2 := MultiplerefChangesLogEntry(setup.relativePath, []RefChange{{Ref: "refs/heads/main", Oid: setup.commitIDs[1]}})
		appendLogEntry(t, ctx, logManager, entry2, setup.GetFileMappingForEntry(1))

		simulatePositions(t, logManager, 1, 2, 2)

		require.Equal(t, [][]storage.LSN{{1, 1}, {1, 2}}, mockConsumer.positions)
		require.Equal(t, storage.LSN(2), logManager.lowWaterMark())

		testhelper.RequireDirectoryState(t, logManager.stateDirectory, "", testhelper.DirectoryState{
			"/":                           {Mode: mode.Directory},
			"/wal":                        {Mode: mode.Directory},
			"/wal/0000000000002":          {Mode: mode.Directory},
			"/wal/0000000000002/MANIFEST": manifestDirectoryEntry(entry2),
			"/wal/0000000000002/1":        {Mode: mode.File, Content: setup.GetFileContent(1)},
		})
	})

	t.Run("entries consumed faster than applied", func(t *testing.T) {
		mockConsumer := &mockLogConsumer{}
		logManager := setupLogManager(t, ctx, mockConsumer)
		setup := setupTestRepo(t, ctx, 2)

		entry1 := MultiplerefChangesLogEntry(setup.relativePath, []RefChange{{Ref: "refs/heads/main", Oid: setup.commitIDs[0]}})
		appendLogEntry(t, ctx, logManager, entry1, setup.GetFileMappingForEntry(0))

		entry2 := MultiplerefChangesLogEntry(setup.relativePath, []RefChange{{Ref: "refs/heads/main", Oid: setup.commitIDs[1]}})
		appendLogEntry(t, ctx, logManager, entry2, setup.GetFileMappingForEntry(1))

		simulatePositions(t, logManager, 2, 0, 0)

		require.Equal(t, [][]storage.LSN{{1, 1}, {1, 2}}, mockConsumer.positions)
		require.Equal(t, storage.LSN(1), logManager.lowWaterMark())

		testhelper.RequireDirectoryState(t, logManager.stateDirectory, "", testhelper.DirectoryState{
			"/":                           {Mode: mode.Directory},
			"/wal":                        {Mode: mode.Directory},
			"/wal/0000000000001":          {Mode: mode.Directory},
			"/wal/0000000000001/MANIFEST": manifestDirectoryEntry(entry2),
			"/wal/0000000000001/1":        {Mode: mode.File, Content: setup.GetFileContent(1)},
			"/wal/0000000000002":          {Mode: mode.Directory},
			"/wal/0000000000002/MANIFEST": manifestDirectoryEntry(entry2),
			"/wal/0000000000002/1":        {Mode: mode.File, Content: setup.GetFileContent(1)},
		})
	})

	t.Run("entries consumed faster than referenced", func(t *testing.T) {
		mockConsumer := &mockLogConsumer{}
		logManager := setupLogManager(t, ctx, mockConsumer)
		setup := setupTestRepo(t, ctx, 2)

		entry1 := MultiplerefChangesLogEntry(setup.relativePath, []RefChange{{Ref: "refs/heads/main", Oid: setup.commitIDs[0]}})
		appendLogEntry(t, ctx, logManager, entry1, setup.GetFileMappingForEntry(0))

		entry2 := MultiplerefChangesLogEntry(setup.relativePath, []RefChange{{Ref: "refs/heads/main", Oid: setup.commitIDs[1]}})
		appendLogEntry(t, ctx, logManager, entry2, setup.GetFileMappingForEntry(1))

		simulatePositions(t, logManager, 2, 2, 0)

		require.Equal(t, [][]storage.LSN{{1, 1}, {1, 2}}, mockConsumer.positions)
		require.Equal(t, storage.LSN(1), logManager.lowWaterMark())

		testhelper.RequireDirectoryState(t, logManager.stateDirectory, "", testhelper.DirectoryState{
			"/":                           {Mode: mode.Directory},
			"/wal":                        {Mode: mode.Directory},
			"/wal/0000000000001":          {Mode: mode.Directory},
			"/wal/0000000000001/MANIFEST": manifestDirectoryEntry(entry2),
			"/wal/0000000000001/1":        {Mode: mode.File, Content: setup.GetFileContent(1)},
			"/wal/0000000000002":          {Mode: mode.Directory},
			"/wal/0000000000002/MANIFEST": manifestDirectoryEntry(entry2),
			"/wal/0000000000002/1":        {Mode: mode.File, Content: setup.GetFileContent(1)},
		})
	})

	t.Run("acknowledge entries one by one", func(t *testing.T) {
		mockConsumer := &mockLogConsumer{}
		logManager := setupLogManager(t, ctx, mockConsumer)
		setup := setupTestRepo(t, ctx, 2)

		entry1 := MultiplerefChangesLogEntry(setup.relativePath, []RefChange{{Ref: "refs/heads/main", Oid: setup.commitIDs[0]}})
		appendLogEntry(t, ctx, logManager, entry1, setup.GetFileMappingForEntry(0))
		simulatePositions(t, logManager, 1, 1, 1)

		entry2 := MultiplerefChangesLogEntry(setup.relativePath, []RefChange{{Ref: "refs/heads/main", Oid: setup.commitIDs[1]}})
		appendLogEntry(t, ctx, logManager, entry2, setup.GetFileMappingForEntry(1))
		simulatePositions(t, logManager, 2, 2, 2)

		// The oldest LSN changes after each acknowledgement
		require.Equal(t, [][]storage.LSN{{1, 1}, {2, 2}}, mockConsumer.positions)
		require.Equal(t, storage.LSN(3), logManager.lowWaterMark())

		testhelper.RequireDirectoryState(t, logManager.stateDirectory, "", testhelper.DirectoryState{
			"/":    {Mode: mode.Directory},
			"/wal": {Mode: mode.Directory},
		})
	})

	t.Run("notify when consumer is busy", func(t *testing.T) {
		mockConsumer := &mockLogConsumer{}
		logManager := setupLogManager(t, ctx, mockConsumer)
		setup := setupTestRepo(t, ctx, 3)

		mockConsumer.busy.Store(true)
		entry1 := MultiplerefChangesLogEntry(setup.relativePath, []RefChange{{Ref: "refs/heads/main", Oid: setup.commitIDs[0]}})
		appendLogEntry(t, ctx, logManager, entry1, setup.GetFileMappingForEntry(0))
		simulatePositions(t, logManager, 0, 1, 1)

		entry2 := MultiplerefChangesLogEntry(setup.relativePath, []RefChange{{Ref: "refs/heads/main", Oid: setup.commitIDs[1]}})
		appendLogEntry(t, ctx, logManager, entry2, setup.GetFileMappingForEntry(1))
		simulatePositions(t, logManager, 0, 2, 2)

		mockConsumer.busy.Store(false)
		entry3 := MultiplerefChangesLogEntry(setup.relativePath, []RefChange{{Ref: "refs/heads/main", Oid: setup.commitIDs[2]}})
		appendLogEntry(t, ctx, logManager, entry3, setup.GetFileMappingForEntry(2))
		simulatePositions(t, logManager, 3, 3, 3)

		// Consumer records the consumable range.
		require.Equal(t, [][]storage.LSN{{1, 3}}, mockConsumer.positions)
		require.Equal(t, storage.LSN(4), logManager.lowWaterMark())

		testhelper.RequireDirectoryState(t, logManager.stateDirectory, "", testhelper.DirectoryState{
			"/":    {Mode: mode.Directory},
			"/wal": {Mode: mode.Directory},
		})
	})

	t.Run("acknowledged entries not pruned if still referenced", func(t *testing.T) {
		mockConsumer := &mockLogConsumer{}
		logManager := setupLogManager(t, ctx, mockConsumer)
		setup := setupTestRepo(t, ctx, 3)

		mockConsumer.busy.Store(true)
		entry1 := MultiplerefChangesLogEntry(setup.relativePath, []RefChange{{Ref: "refs/heads/main", Oid: setup.commitIDs[0]}})
		appendLogEntry(t, ctx, logManager, entry1, setup.GetFileMappingForEntry(0))

		entry2 := MultiplerefChangesLogEntry(setup.relativePath, []RefChange{{Ref: "refs/heads/main", Oid: setup.commitIDs[1]}})
		appendLogEntry(t, ctx, logManager, entry2, setup.GetFileMappingForEntry(1))

		entry3 := MultiplerefChangesLogEntry(setup.relativePath, []RefChange{{Ref: "refs/heads/main", Oid: setup.commitIDs[1]}})
		appendLogEntry(t, ctx, logManager, entry3, setup.GetFileMappingForEntry(1))

		// 2 is stilled referenced; hence 2 and 3 entries are kept intact
		simulatePositions(t, logManager, 3, 3, 1)

		require.Equal(t, storage.LSN(2), logManager.lowWaterMark())

		testhelper.RequireDirectoryState(t, logManager.stateDirectory, "", testhelper.DirectoryState{
			"/":                           {Mode: mode.Directory},
			"/wal":                        {Mode: mode.Directory},
			"/wal/0000000000002":          {Mode: mode.Directory},
			"/wal/0000000000002/MANIFEST": manifestDirectoryEntry(entry2),
			"/wal/0000000000002/1":        {Mode: mode.File, Content: setup.GetFileContent(1)},
			"/wal/0000000000003":          {Mode: mode.Directory},
			"/wal/0000000000003/MANIFEST": manifestDirectoryEntry(entry3),
			"/wal/0000000000003/1":        {Mode: mode.File, Content: setup.GetFileContent(2)},
		})

		simulatePositions(t, logManager, 3, 3, 3)
		require.Equal(t, storage.LSN(4), logManager.lowWaterMark())
		testhelper.RequireDirectoryState(t, logManager.stateDirectory, "", testhelper.DirectoryState{
			"/":    {Mode: mode.Directory},
			"/wal": {Mode: mode.Directory},
		})
	})
}
