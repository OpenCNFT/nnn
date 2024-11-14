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
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/keyvalue"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/mode"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper/testentry"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
	"google.golang.org/protobuf/proto"
)

type expectedEntryReference struct {
	lsn  storage.LSN
	refs int
}

func proposeLogEntry(t *testing.T, ctx context.Context, manager *LogManager, objectDependencies map[git.ObjectID]struct{}, logEntry *gitalypb.LogEntry, files map[string][]byte) storage.LSN {
	t.Helper()

	logEntryPath := testhelper.TempDir(t)
	for name, value := range files {
		path := filepath.Join(logEntryPath, name)
		require.NoError(t, os.WriteFile(path, value, mode.File))
	}

	nextLSN, err := manager.Propose(ctx, objectDependencies, logEntry, logEntryPath)
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

func assertEntryReferences(t *testing.T, manager *LogManager, expected []*expectedEntryReference, shouldNotify bool) {
	actualList := manager.entryReferences
	require.Equal(t, len(expected), actualList.Len())

	i := 0
	for elm := actualList.Front(); elm != nil; elm = elm.Next() {
		actual := elm.Value.(*EntryReference)
		require.Equal(t, expected[i].lsn, actual.lsn)
		require.Equal(t, expected[i].refs, actual.refs)
		i++
	}

	if shouldNotify {
		select {
		case <-manager.NotifyQueue():
		default:
			require.FailNow(t, "log manager should send signal to notify channel")
		}
	} else {
		select {
		case <-manager.NotifyQueue():
			require.FailNow(t, "log manager should not send signal to notify channel")
		default:
		}
	}
}

type testSetup struct {
	relativePath string
	commitIDs    []git.ObjectID
}

func setupTestRepo(t *testing.T, ctx context.Context, numCommits int) testSetup {
	relativePath := gittest.NewRepositoryName(t)
	cfg := testcfg.Build(t)
	_, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
		SkipCreationViaService: true,
		RelativePath:           relativePath,
	})

	setup := testSetup{relativePath: repoPath}

	for i := 0; i < numCommits; i++ {
		commitID := gittest.WriteCommit(t, cfg, repoPath, gittest.WithParents())
		setup.commitIDs = append(setup.commitIDs, commitID)
	}

	return setup
}

// GetFileMappingForEntry returns a map where each key is a file name (starting from "1")
// and each value is the corresponding file content.
func (ts *testSetup) GetFileMappingForEntry(indices ...int) map[string][]byte {
	mapping := map[string][]byte{}
	for order, index := range indices {
		mapping[fmt.Sprintf("%d", order+1)] = ts.GetFileContent(index)
	}
	return mapping
}

// GetFileContent returns the on-disk content of a ref.
func (ts *testSetup) GetFileContent(index int) []byte {
	return []byte(ts.commitIDs[index] + "\n")
}

func setupLogManager(t *testing.T, ctx context.Context, consumer LogConsumer) *LogManager {
	database, err := keyvalue.NewBadgerStore(testhelper.SharedLogger(t), t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { testhelper.MustClose(t, database) })

	logManager := NewLogManager("test-storage", 1, database, testhelper.TempDir(t), testhelper.TempDir(t), consumer)
	require.NoError(t, logManager.Initialize(ctx))

	return logManager
}

func TestLogManager_Initialize(t *testing.T) {
	t.Parallel()

	t.Run("initial state without prior log entries", func(t *testing.T) {
		t.Parallel()
		ctx := testhelper.Context(t)
		stateDir := testhelper.TempDir(t)

		database, err := keyvalue.NewBadgerStore(testhelper.SharedLogger(t), t.TempDir())
		require.NoError(t, err)
		defer testhelper.MustClose(t, database)

		logManager := NewLogManager("test-storage", 1, database, testhelper.TempDir(t), stateDir, nil)
		require.NoError(t, logManager.Initialize(ctx))

		require.Equal(t, storage.LSN(1), logManager.oldestLSN)
		require.Equal(t, storage.LSN(0), logManager.appliedLSN)
		require.Equal(t, storage.LSN(0), logManager.appendedLSN)
		testhelper.RequireDirectoryState(t, logManager.stateDirectory, "", testhelper.DirectoryState{
			"/":    {Mode: mode.Directory},
			"/wal": {Mode: mode.Directory},
		})
	})

	t.Run("existing WAL entries without existing appliedLSN", func(t *testing.T) {
		t.Parallel()
		ctx := testhelper.Context(t)
		stateDir := testhelper.TempDir(t)
		database, err := keyvalue.NewBadgerStore(testhelper.SharedLogger(t), t.TempDir())
		require.NoError(t, err)
		defer testhelper.MustClose(t, database)

		logManager := NewLogManager("test-storage", 1, database, testhelper.TempDir(t), stateDir, nil)

		setup := setupTestRepo(t, ctx, 2)
		entry1 := testentry.MultipleRefChangesLogEntry(setup.relativePath, []testentry.RefChange{
			{Ref: "branch-1", Oid: setup.commitIDs[0]},
		})
		entry2 := testentry.MultipleRefChangesLogEntry(setup.relativePath, []testentry.RefChange{
			{Ref: "branch-2", Oid: setup.commitIDs[1]},
		})

		injectLogEntry(t, ctx, logManager, 1, entry1, setup.GetFileMappingForEntry(0))
		injectLogEntry(t, ctx, logManager, 2, entry2, setup.GetFileMappingForEntry(1))

		require.NoError(t, logManager.Initialize(ctx))
		require.Equal(t, storage.LSN(1), logManager.oldestLSN)
		require.Equal(t, storage.LSN(0), logManager.appliedLSN)
		require.Equal(t, storage.LSN(2), logManager.appendedLSN)

		testhelper.RequireDirectoryState(t, logManager.stateDirectory, "", testhelper.DirectoryState{
			"/":                           {Mode: mode.Directory},
			"/wal":                        {Mode: mode.Directory},
			"/wal/0000000000001":          {Mode: mode.Directory},
			"/wal/0000000000001/MANIFEST": testentry.ManifestDirectoryEntry(entry1),
			"/wal/0000000000001/1":        {Mode: mode.File, Content: setup.GetFileContent(0)},
			"/wal/0000000000002":          {Mode: mode.Directory},
			"/wal/0000000000002/MANIFEST": testentry.ManifestDirectoryEntry(entry2),
			"/wal/0000000000002/1":        {Mode: mode.File, Content: setup.GetFileContent(1)},
		})
	})

	t.Run("existing WAL entries with appliedLSN in-between", func(t *testing.T) {
		t.Parallel()
		ctx := testhelper.Context(t)
		stateDir := testhelper.TempDir(t)
		database, err := keyvalue.NewBadgerStore(testhelper.SharedLogger(t), t.TempDir())
		require.NoError(t, err)
		defer testhelper.MustClose(t, database)

		logManager := NewLogManager("test-storage", 1, database, testhelper.TempDir(t), stateDir, nil)

		setup := setupTestRepo(t, ctx, 3)
		var entries []*gitalypb.LogEntry
		for i, commitID := range setup.commitIDs {
			entry := testentry.MultipleRefChangesLogEntry(setup.relativePath, []testentry.RefChange{
				{Ref: fmt.Sprintf("branch-%d", i+1), Oid: commitID},
			})
			injectLogEntry(t, ctx, logManager, storage.LSN(i+1), entry, setup.GetFileMappingForEntry(i))
			entries = append(entries, entry)
		}

		require.NoError(t, logManager.StoreAppliedLSN(2))
		require.NoError(t, logManager.Initialize(ctx))

		require.Equal(t, storage.LSN(1), logManager.oldestLSN)
		require.Equal(t, storage.LSN(2), logManager.appliedLSN)
		require.Equal(t, storage.LSN(3), logManager.appendedLSN)

		testhelper.RequireDirectoryState(t, logManager.stateDirectory, "", testhelper.DirectoryState{
			"/":                           {Mode: mode.Directory},
			"/wal":                        {Mode: mode.Directory},
			"/wal/0000000000001":          {Mode: mode.Directory},
			"/wal/0000000000001/MANIFEST": testentry.ManifestDirectoryEntry(entries[0]),
			"/wal/0000000000001/1":        {Mode: mode.File, Content: setup.GetFileContent(0)},
			"/wal/0000000000002":          {Mode: mode.Directory},
			"/wal/0000000000002/MANIFEST": testentry.ManifestDirectoryEntry(entries[1]),
			"/wal/0000000000002/1":        {Mode: mode.File, Content: setup.GetFileContent(1)},
			"/wal/0000000000003":          {Mode: mode.Directory},
			"/wal/0000000000003/MANIFEST": testentry.ManifestDirectoryEntry(entries[2]),
			"/wal/0000000000003/1":        {Mode: mode.File, Content: setup.GetFileContent(2)},
		})
	})

	t.Run("existing WAL entries with up-to-date appliedLSN", func(t *testing.T) {
		t.Parallel()
		ctx := testhelper.Context(t)
		stateDir := testhelper.TempDir(t)
		database, err := keyvalue.NewBadgerStore(testhelper.SharedLogger(t), t.TempDir())
		require.NoError(t, err)
		defer testhelper.MustClose(t, database)

		logManager := NewLogManager("test-storage", 1, database, testhelper.TempDir(t), stateDir, nil)

		setup := setupTestRepo(t, ctx, 0)
		var entries []*gitalypb.LogEntry
		for i := 0; i < 3; i++ {
			entry := testentry.MultipleKVLogEntry(setup.relativePath, []testentry.KVOperation{
				{Key: []byte(fmt.Sprintf("Key-%d", i+1)), Value: []byte("content")},
			})
			injectLogEntry(t, ctx, logManager, storage.LSN(i+1), entry, nil)
			entries = append(entries, entry)
		}

		require.NoError(t, logManager.StoreAppliedLSN(3))
		require.NoError(t, logManager.Initialize(ctx))

		require.Equal(t, storage.LSN(1), logManager.oldestLSN)
		require.Equal(t, storage.LSN(3), logManager.appliedLSN)
		require.Equal(t, storage.LSN(3), logManager.appendedLSN)

		testhelper.RequireDirectoryState(t, logManager.stateDirectory, "", testhelper.DirectoryState{
			"/":                           {Mode: mode.Directory},
			"/wal":                        {Mode: mode.Directory},
			"/wal/0000000000001":          {Mode: mode.Directory},
			"/wal/0000000000001/MANIFEST": testentry.ManifestDirectoryEntry(entries[0]),
			"/wal/0000000000002":          {Mode: mode.Directory},
			"/wal/0000000000002/MANIFEST": testentry.ManifestDirectoryEntry(entries[1]),
			"/wal/0000000000003":          {Mode: mode.Directory},
			"/wal/0000000000003/MANIFEST": testentry.ManifestDirectoryEntry(entries[2]),
		})
	})
}

func TestLogManager_RemoveAppliedLogEntries(t *testing.T) {
	t.Parallel()

	t.Run("no entries to remove", func(t *testing.T) {
		t.Parallel()
		ctx := testhelper.Context(t)
		logManager := setupLogManager(t, ctx, nil)

		removedLSN, err := logManager.RemoveAppliedLogEntries(ctx)

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
		entry := testentry.MultipleRefChangesLogEntry(setup.relativePath, []testentry.RefChange{
			{Ref: "branch-1", Oid: setup.commitIDs[0]},
		})
		injectLogEntry(t, ctx, logManager, 1, entry, setup.GetFileMappingForEntry(0))

		// Set this entry as applied
		require.NoError(t, logManager.StoreAppliedLSN(1))

		// Attempt to remove applied log entries
		removedLSN, err := logManager.RemoveAppliedLogEntries(ctx)

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
		logManager := setupLogManager(t, ctx, nil)

		// Inject multiple log entries
		setup := setupTestRepo(t, ctx, 3)
		entry := func(i int) *gitalypb.LogEntry {
			return testentry.MultipleRefChangesLogEntry(setup.relativePath, []testentry.RefChange{
				{Ref: fmt.Sprintf("branch-%d", i+1), Oid: setup.commitIDs[i]},
			})
		}
		for i := 0; i < 3; i++ {
			injectLogEntry(t, ctx, logManager, storage.LSN(i+1), entry(i), setup.GetFileMappingForEntry(i))
		}

		// Set the applied LSN to 2
		require.NoError(t, logManager.StoreAppliedLSN(2))

		// Manually set the consumer's position to the first entry, forcing low-water mark to retain it
		logManager.consumerPos.setPosition(1)

		removedLSN, err := logManager.RemoveAppliedLogEntries(ctx)

		// No entries should be removed because of the low-water mark
		require.NoError(t, err)
		require.Equal(t, storage.LSN(1), removedLSN)
		require.Equal(t, storage.LSN(2), logManager.oldestLSN)

		// Assert on-disk state to ensure no entries were removed
		testhelper.RequireDirectoryState(t, logManager.stateDirectory, "", testhelper.DirectoryState{
			"/":                           {Mode: mode.Directory},
			"/wal":                        {Mode: mode.Directory},
			"/wal/0000000000002":          {Mode: mode.Directory},
			"/wal/0000000000002/MANIFEST": testentry.ManifestDirectoryEntry(entry(1)),
			"/wal/0000000000002/1":        {Mode: mode.File, Content: setup.GetFileContent(1)},
			"/wal/0000000000003":          {Mode: mode.Directory},
			"/wal/0000000000003/MANIFEST": testentry.ManifestDirectoryEntry(entry(2)),
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
			return testentry.MultipleRefChangesLogEntry(setup.relativePath, []testentry.RefChange{
				{Ref: fmt.Sprintf("branch-%d", i+1), Oid: setup.commitIDs[i]},
			})
		}
		for i := 0; i < 5; i++ {
			injectLogEntry(t, ctx, logManager, storage.LSN(i+1), entry(i), setup.GetFileMappingForEntry(i))
		}

		// Set the applied LSN to 3, allowing the first three entries to be pruned
		require.NoError(t, logManager.StoreAppliedLSN(3))

		for i := 0; i < 3; i++ {
			removedLSN, err := logManager.RemoveAppliedLogEntries(ctx)

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
			"/wal/0000000000004/MANIFEST": testentry.ManifestDirectoryEntry(entry(3)),
			"/wal/0000000000004/1":        {Mode: mode.File, Content: setup.GetFileContent(3)},
			"/wal/0000000000005":          {Mode: mode.Directory},
			"/wal/0000000000005/MANIFEST": testentry.ManifestDirectoryEntry(entry(4)),
			"/wal/0000000000005/1":        {Mode: mode.File, Content: setup.GetFileContent(4)},
		})
	})
}

func TestLogManager_Propose(t *testing.T) {
	t.Parallel()

	t.Run("propose a log entry with a single file", func(t *testing.T) {
		t.Parallel()

		ctx := testhelper.Context(t)
		logManager := setupLogManager(t, ctx, nil)
		setup := setupTestRepo(t, ctx, 1)

		require.Equal(t, logManager.appendedLSN, storage.LSN(0))

		refEntry := testentry.RefChangeLogEntry(setup.relativePath, "branch-1", setup.commitIDs[0])
		proposeLogEntry(t, ctx, logManager, nil, refEntry, setup.GetFileMappingForEntry(0))

		require.Equal(t, logManager.appendedLSN, storage.LSN(1))
		testhelper.RequireDirectoryState(t, logManager.stateDirectory, "", testhelper.DirectoryState{
			"/":                           {Mode: mode.Directory},
			"/wal":                        {Mode: mode.Directory},
			"/wal/0000000000001":          {Mode: mode.Directory},
			"/wal/0000000000001/MANIFEST": testentry.ManifestDirectoryEntry(refEntry),
			"/wal/0000000000001/1":        {Mode: mode.File, Content: setup.GetFileContent(0)},
		})
	})

	t.Run("propose a log entry with multiple files", func(t *testing.T) {
		t.Parallel()

		ctx := testhelper.Context(t)
		logManager := setupLogManager(t, ctx, nil)
		setup := setupTestRepo(t, ctx, 3)

		require.Equal(t, logManager.appendedLSN, storage.LSN(0))

		refEntry := testentry.MultipleRefChangesLogEntry(setup.relativePath, []testentry.RefChange{
			{Ref: "branch-1", Oid: setup.commitIDs[0]},
			{Ref: "branch-2", Oid: setup.commitIDs[1]},
			{Ref: "branch-3", Oid: setup.commitIDs[2]},
		})
		proposeLogEntry(t, ctx, logManager, nil, refEntry, setup.GetFileMappingForEntry(0, 1, 2))

		require.Equal(t, logManager.appendedLSN, storage.LSN(1))
		testhelper.RequireDirectoryState(t, logManager.stateDirectory, "", testhelper.DirectoryState{
			"/":                           {Mode: mode.Directory},
			"/wal":                        {Mode: mode.Directory},
			"/wal/0000000000001":          {Mode: mode.Directory},
			"/wal/0000000000001/MANIFEST": testentry.ManifestDirectoryEntry(refEntry),
			"/wal/0000000000001/1":        {Mode: mode.File, Content: setup.GetFileContent(0)},
			"/wal/0000000000001/2":        {Mode: mode.File, Content: setup.GetFileContent(1)},
			"/wal/0000000000001/3":        {Mode: mode.File, Content: setup.GetFileContent(2)},
		})
	})

	t.Run("propose multiple entries", func(t *testing.T) {
		t.Parallel()

		ctx := testhelper.Context(t)
		logManager := setupLogManager(t, ctx, nil)
		setup := setupTestRepo(t, ctx, 3)

		require.Equal(t, logManager.appendedLSN, storage.LSN(0))

		refEntry1 := testentry.MultipleRefChangesLogEntry(setup.relativePath, []testentry.RefChange{
			{Ref: "branch-1", Oid: setup.commitIDs[0]},
		})
		proposeLogEntry(t, ctx, logManager, nil, refEntry1, setup.GetFileMappingForEntry(0))

		refEntry2 := testentry.MultipleRefChangesLogEntry(setup.relativePath, []testentry.RefChange{
			{Ref: "branch-2", Oid: setup.commitIDs[1]},
			{Ref: "branch-3", Oid: setup.commitIDs[2]},
		})
		proposeLogEntry(t, ctx, logManager, nil, refEntry2, setup.GetFileMappingForEntry(1, 2))

		kvEntry1 := testentry.MultipleKVLogEntry(setup.relativePath, []testentry.KVOperation{
			{Key: []byte("key-1"), Value: []byte("value-1")},
			{Key: []byte("key-2"), Value: []byte("value-2")},
		})
		proposeLogEntry(t, ctx, logManager, nil, kvEntry1, nil)

		require.Equal(t, logManager.appendedLSN, storage.LSN(3))
		testhelper.RequireDirectoryState(t, logManager.stateDirectory, "", testhelper.DirectoryState{
			"/":                           {Mode: mode.Directory},
			"/wal":                        {Mode: mode.Directory},
			"/wal/0000000000001":          {Mode: mode.Directory},
			"/wal/0000000000001/MANIFEST": testentry.ManifestDirectoryEntry(refEntry1),
			"/wal/0000000000001/1":        {Mode: mode.File, Content: setup.GetFileContent(0)},
			"/wal/0000000000002":          {Mode: mode.Directory},
			"/wal/0000000000002/MANIFEST": testentry.ManifestDirectoryEntry(refEntry2),
			"/wal/0000000000002/1":        {Mode: mode.File, Content: setup.GetFileContent(1)},
			"/wal/0000000000002/2":        {Mode: mode.File, Content: setup.GetFileContent(2)},
			"/wal/0000000000003":          {Mode: mode.Directory},
			"/wal/0000000000003/MANIFEST": testentry.ManifestDirectoryEntry(kvEntry1),
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

		refEntry1 := testentry.MultipleRefChangesLogEntry(setup.relativePath, []testentry.RefChange{
			{Ref: "branch-1", Oid: setup.commitIDs[0]},
		})
		kvEntry1 := testentry.MultipleKVLogEntry(setup.relativePath, []testentry.KVOperation{
			{Key: []byte("key-1"), Value: []byte("value-1")},
			{Key: []byte("key-2"), Value: []byte("value-2")},
		})
		refEntry2 := testentry.MultipleRefChangesLogEntry(setup.relativePath, []testentry.RefChange{
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
			proposeLogEntry(t, ctx, logManager, nil, refEntry1, setup.GetFileMappingForEntry(0))
			// There should be a crash here.
			proposeLogEntry(t, ctx, logManager, nil, kvEntry1, nil)
			// The following proposal is not reached, so it should not be recorded
			proposeLogEntry(t, ctx, logManager, nil, refEntry2, setup.GetFileMappingForEntry(1, 2))
		}()

		require.Equal(t, logManager.appendedLSN, storage.LSN(1))
		testhelper.RequireDirectoryState(t, logManager.stateDirectory, "", testhelper.DirectoryState{
			"/":                           {Mode: mode.Directory},
			"/wal":                        {Mode: mode.Directory},
			"/wal/0000000000001":          {Mode: mode.Directory},
			"/wal/0000000000001/MANIFEST": testentry.ManifestDirectoryEntry(refEntry1),
			"/wal/0000000000001/1":        {Mode: mode.File, Content: setup.GetFileContent(0)},
		})

		// Remove the hook and retry the last proposal.
		logManager.testHooks.beforeAppendLogEntry = func(lsn storage.LSN) {}
		proposeLogEntry(t, ctx, logManager, nil, refEntry2, setup.GetFileMappingForEntry(1, 2))

		require.Equal(t, logManager.appendedLSN, storage.LSN(2))
		testhelper.RequireDirectoryState(t, logManager.stateDirectory, "", testhelper.DirectoryState{
			"/":                           {Mode: mode.Directory},
			"/wal":                        {Mode: mode.Directory},
			"/wal/0000000000001":          {Mode: mode.Directory},
			"/wal/0000000000001/MANIFEST": testentry.ManifestDirectoryEntry(refEntry1),
			"/wal/0000000000001/1":        {Mode: mode.File, Content: setup.GetFileContent(0)},
			"/wal/0000000000002":          {Mode: mode.Directory},
			"/wal/0000000000002/MANIFEST": testentry.ManifestDirectoryEntry(refEntry2),
			"/wal/0000000000002/1":        {Mode: mode.File, Content: setup.GetFileContent(1)},
			"/wal/0000000000002/2":        {Mode: mode.File, Content: setup.GetFileContent(2)},
		})
	})
}

func TestLogManager_EntryReferences(t *testing.T) {
	t.Parallel()
	t.Run("empty entry references list", func(t *testing.T) {
		t.Parallel()
		ctx := testhelper.Context(t)
		logManager := setupLogManager(t, ctx, nil)
		assertEntryReferences(t, logManager, []*expectedEntryReference{}, false)
		entryRef := logManager.IncrementEntryReference(2)
		assertEntryReferences(t, logManager, []*expectedEntryReference{
			{lsn: 2, refs: 1},
		}, false)
		logManager.DecrementEntryReference(entryRef)
		assertEntryReferences(t, logManager, []*expectedEntryReference{}, true)
	})

	t.Run("reference list with one item", func(t *testing.T) {
		t.Parallel()
		ctx := testhelper.Context(t)
		logManager := setupLogManager(t, ctx, nil)
		setup := setupTestRepo(t, ctx, 1)
		proposeLogEntry(t, ctx, logManager, nil, testentry.RefChangeLogEntry(setup.relativePath, "branch-1",
			setup.commitIDs[0]), setup.GetFileMappingForEntry(0))
		assertEntryReferences(t, logManager, []*expectedEntryReference{
			{lsn: 1, refs: 0},
		}, false)
		ref1 := logManager.IncrementEntryReference(1)
		ref2 := logManager.IncrementEntryReference(1)
		assertEntryReferences(t, logManager, []*expectedEntryReference{
			{lsn: 1, refs: 2},
		}, false)
		logManager.DecrementEntryReference(ref1)
		logManager.DecrementEntryReference(ref2)
		assertEntryReferences(t, logManager, []*expectedEntryReference{}, true)
	})

	t.Run("reference list with multiple items", func(t *testing.T) {
		t.Parallel()
		ctx := testhelper.Context(t)
		logManager := setupLogManager(t, ctx, nil)
		setup := setupTestRepo(t, ctx, 4)
		proposeLogEntry(t, ctx, logManager, nil, testentry.RefChangeLogEntry(setup.relativePath, "branch-1",
			setup.commitIDs[0]), setup.GetFileMappingForEntry(0))
		ref1 := logManager.IncrementEntryReference(1)
		ref2 := logManager.IncrementEntryReference(1)
		assertEntryReferences(t, logManager, []*expectedEntryReference{
			{lsn: 1, refs: 2},
		}, false)
		proposeLogEntry(t, ctx, logManager, nil, testentry.RefChangeLogEntry(setup.relativePath, "branch-2",
			setup.commitIDs[1]), setup.GetFileMappingForEntry(1))
		ref3 := logManager.IncrementEntryReference(2)
		assertEntryReferences(t, logManager, []*expectedEntryReference{
			{lsn: 1, refs: 2},
			{lsn: 2, refs: 1},
		}, false)
		proposeLogEntry(t, ctx, logManager, nil, testentry.RefChangeLogEntry(setup.relativePath, "branch-3",
			setup.commitIDs[2]), setup.GetFileMappingForEntry(2))
		ref4 := logManager.IncrementEntryReference(3)
		assertEntryReferences(t, logManager, []*expectedEntryReference{
			{lsn: 1, refs: 2},
			{lsn: 2, refs: 1},
			{lsn: 3, refs: 1},
		}, false)
		proposeLogEntry(t, ctx, logManager, nil, testentry.RefChangeLogEntry(setup.relativePath, "branch-4",
			setup.commitIDs[3]), setup.GetFileMappingForEntry(3))
		ref5 := logManager.IncrementEntryReference(3)
		assertEntryReferences(t, logManager, []*expectedEntryReference{
			{lsn: 1, refs: 2},
			{lsn: 2, refs: 1},
			{lsn: 3, refs: 1},
			{lsn: 4, refs: 1},
		}, false)
		logManager.DecrementEntryReference(ref3)
		// De-reference an item in the middle of the list.
		assertEntryReferences(t, logManager, []*expectedEntryReference{
			{lsn: 1, refs: 2},
			{lsn: 2, refs: 0},
			{lsn: 3, refs: 1},
			{lsn: 4, refs: 1},
		}, false)
		// De-reference another item in the middle of the list.
		logManager.DecrementEntryReference(ref4)
		assertEntryReferences(t, logManager, []*expectedEntryReference{
			{lsn: 1, refs: 2},
			{lsn: 2, refs: 0},
			{lsn: 3, refs: 0},
			{lsn: 4, refs: 1},
		}, false)
		// De-reference an item in the head of the list, but one remaining reference.
		logManager.DecrementEntryReference(ref1)
		assertEntryReferences(t, logManager, []*expectedEntryReference{
			{lsn: 1, refs: 1},
			{lsn: 2, refs: 0},
			{lsn: 3, refs: 0},
			{lsn: 4, refs: 1},
		}, false)
		// De-reference the head of the list. It cleans up til reaching the first referenced entry.
		logManager.DecrementEntryReference(ref2)
		assertEntryReferences(t, logManager, []*expectedEntryReference{
			{lsn: 4, refs: 1},
		}, true)
		// Clean up the whole queue.
		logManager.DecrementEntryReference(ref5)
		assertEntryReferences(t, logManager, []*expectedEntryReference{}, true)
	})

	t.Run("reference list's head is cut off multiple times", func(t *testing.T) {
		t.Parallel()
		ctx := testhelper.Context(t)
		logManager := setupLogManager(t, ctx, nil)
		setup := setupTestRepo(t, ctx, 4)

		proposeLogEntry(t, ctx, logManager, nil, testentry.RefChangeLogEntry(setup.relativePath, "branch-1",
			setup.commitIDs[0]), setup.GetFileMappingForEntry(0))
		ref1 := logManager.IncrementEntryReference(1)
		proposeLogEntry(t, ctx, logManager, nil, testentry.RefChangeLogEntry(setup.relativePath, "branch-2",
			setup.commitIDs[1]), setup.GetFileMappingForEntry(1))
		proposeLogEntry(t, ctx, logManager, nil, testentry.RefChangeLogEntry(setup.relativePath, "branch-3",
			setup.commitIDs[2]), setup.GetFileMappingForEntry(2))
		ref2 := logManager.IncrementEntryReference(3)
		proposeLogEntry(t, ctx, logManager, nil, testentry.RefChangeLogEntry(setup.relativePath, "branch-4",
			setup.commitIDs[3]), setup.GetFileMappingForEntry(3))
		ref3 := logManager.IncrementEntryReference(4)

		assertEntryReferences(t, logManager, []*expectedEntryReference{
			{lsn: 1, refs: 1},
			{lsn: 2, refs: 0},
			{lsn: 3, refs: 1},
			{lsn: 4, refs: 1},
		}, false)
		logManager.DecrementEntryReference(ref1)
		assertEntryReferences(t, logManager, []*expectedEntryReference{
			{lsn: 3, refs: 1},
			{lsn: 4, refs: 1},
		}, true)
		logManager.DecrementEntryReference(ref2)
		assertEntryReferences(t, logManager, []*expectedEntryReference{
			{lsn: 4, refs: 1},
		}, true)
		logManager.DecrementEntryReference(ref3)
		assertEntryReferences(t, logManager, []*expectedEntryReference{}, true)
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

func TestLogManager_Consumer(t *testing.T) {
	ctx := testhelper.Context(t)

	simulatePositions := func(t *testing.T, logManager *LogManager, consumed storage.LSN, applied storage.LSN) {
		logManager.consumerPos.setPosition(consumed)

		require.NoError(t, logManager.StoreAppliedLSN(applied))
		logManager.cleanupEntryReferences()
		for {
			removedLSN, err := logManager.RemoveAppliedLogEntries(ctx)
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
		require.Equal(t, storage.LSN(0), logManager.consumerPos.getPosition())
		require.Equal(t, storage.LSN(1), logManager.lowWaterMark())

		testhelper.RequireDirectoryState(t, logManager.stateDirectory, "", testhelper.DirectoryState{
			"/":    {Mode: mode.Directory},
			"/wal": {Mode: mode.Directory},
		})
	})

	t.Run("notify consumer after restart", func(t *testing.T) {
		stateDir := testhelper.TempDir(t)
		dbDir := testhelper.TempDir(t)

		// Before restart
		mockConsumer := &mockLogConsumer{}
		database, err := keyvalue.NewBadgerStore(testhelper.SharedLogger(t), dbDir)
		require.NoError(t, err)

		logManager := NewLogManager("test-storage", 1, database, testhelper.TempDir(t), stateDir, mockConsumer)
		require.NoError(t, logManager.Initialize(ctx))
		setup := setupTestRepo(t, ctx, 4)

		entry1 := testentry.RefChangeLogEntry(setup.relativePath, "refs/heads/main", setup.commitIDs[0])
		proposeLogEntry(t, ctx, logManager, nil, entry1, setup.GetFileMappingForEntry(0))

		entry2 := testentry.RefChangeLogEntry(setup.relativePath, "refs/heads/main", setup.commitIDs[1])
		proposeLogEntry(t, ctx, logManager, nil, entry2, setup.GetFileMappingForEntry(1))

		// Apply to 3 but consume to 1
		simulatePositions(t, logManager, 1, 2)
		require.Equal(t, [][]storage.LSN{{1, 1}, {1, 2}}, mockConsumer.positions)
		require.Equal(t, storage.LSN(2), logManager.lowWaterMark())

		// Inject 3, 4
		entry3 := testentry.RefChangeLogEntry(setup.relativePath, "refs/heads/main", setup.commitIDs[2])
		injectLogEntry(t, ctx, logManager, 3, entry3, setup.GetFileMappingForEntry(2))
		entry4 := testentry.RefChangeLogEntry(setup.relativePath, "refs/heads/main", setup.commitIDs[3])
		injectLogEntry(t, ctx, logManager, 4, entry4, setup.GetFileMappingForEntry(3))

		// Close log manager
		testhelper.MustClose(t, database)

		testhelper.RequireDirectoryState(t, logManager.stateDirectory, "", testhelper.DirectoryState{
			"/":                           {Mode: mode.Directory},
			"/wal":                        {Mode: mode.Directory},
			"/wal/0000000000002":          {Mode: mode.Directory},
			"/wal/0000000000002/MANIFEST": testentry.ManifestDirectoryEntry(entry2),
			"/wal/0000000000002/1":        {Mode: mode.File, Content: setup.GetFileContent(1)},
			"/wal/0000000000003":          {Mode: mode.Directory},
			"/wal/0000000000003/MANIFEST": testentry.ManifestDirectoryEntry(entry3),
			"/wal/0000000000003/1":        {Mode: mode.File, Content: setup.GetFileContent(2)},
			"/wal/0000000000004":          {Mode: mode.Directory},
			"/wal/0000000000004/MANIFEST": testentry.ManifestDirectoryEntry(entry4),
			"/wal/0000000000004/1":        {Mode: mode.File, Content: setup.GetFileContent(3)},
		})

		// Restart the log consumer.
		mockConsumer = &mockLogConsumer{}
		database, err = keyvalue.NewBadgerStore(testhelper.SharedLogger(t), dbDir)
		require.NoError(t, err)
		defer testhelper.MustClose(t, database)

		logManager = NewLogManager("test-storage", 1, database, testhelper.TempDir(t), stateDir, mockConsumer)
		require.NoError(t, logManager.Initialize(ctx))

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
		setup := setupTestRepo(t, ctx, 2)

		entry1 := testentry.RefChangeLogEntry(setup.relativePath, "refs/heads/main", setup.commitIDs[0])
		proposeLogEntry(t, ctx, logManager, nil, entry1, setup.GetFileMappingForEntry(0))

		entry2 := testentry.RefChangeLogEntry(setup.relativePath, "refs/heads/main", setup.commitIDs[1])
		proposeLogEntry(t, ctx, logManager, nil, entry2, setup.GetFileMappingForEntry(1))

		simulatePositions(t, logManager, 0, 2)

		require.Equal(t, [][]storage.LSN{{1, 1}, {1, 2}}, mockConsumer.positions)
		require.Equal(t, storage.LSN(1), logManager.lowWaterMark())

		testhelper.RequireDirectoryState(t, logManager.stateDirectory, "", testhelper.DirectoryState{
			"/":                           {Mode: mode.Directory},
			"/wal":                        {Mode: mode.Directory},
			"/wal/0000000000001":          {Mode: mode.Directory},
			"/wal/0000000000001/MANIFEST": testentry.ManifestDirectoryEntry(entry1),
			"/wal/0000000000001/1":        {Mode: mode.File, Content: setup.GetFileContent(0)},
			"/wal/0000000000002":          {Mode: mode.Directory},
			"/wal/0000000000002/MANIFEST": testentry.ManifestDirectoryEntry(entry2),
			"/wal/0000000000002/1":        {Mode: mode.File, Content: setup.GetFileContent(1)},
		})
	})

	t.Run("acknowledged entries got pruned", func(t *testing.T) {
		mockConsumer := &mockLogConsumer{}
		logManager := setupLogManager(t, ctx, mockConsumer)
		setup := setupTestRepo(t, ctx, 2)

		entry1 := testentry.RefChangeLogEntry(setup.relativePath, "refs/heads/main", setup.commitIDs[0])
		proposeLogEntry(t, ctx, logManager, nil, entry1, setup.GetFileMappingForEntry(0))

		entry2 := testentry.RefChangeLogEntry(setup.relativePath, "refs/heads/main", setup.commitIDs[1])
		proposeLogEntry(t, ctx, logManager, nil, entry2, setup.GetFileMappingForEntry(1))

		simulatePositions(t, logManager, 1, 2)

		require.Equal(t, [][]storage.LSN{{1, 1}, {1, 2}}, mockConsumer.positions)
		require.Equal(t, storage.LSN(2), logManager.lowWaterMark())

		testhelper.RequireDirectoryState(t, logManager.stateDirectory, "", testhelper.DirectoryState{
			"/":                           {Mode: mode.Directory},
			"/wal":                        {Mode: mode.Directory},
			"/wal/0000000000002":          {Mode: mode.Directory},
			"/wal/0000000000002/MANIFEST": testentry.ManifestDirectoryEntry(entry2),
			"/wal/0000000000002/1":        {Mode: mode.File, Content: setup.GetFileContent(1)},
		})
	})

	t.Run("entries consumed faster than applied", func(t *testing.T) {
		mockConsumer := &mockLogConsumer{}
		logManager := setupLogManager(t, ctx, mockConsumer)
		setup := setupTestRepo(t, ctx, 2)

		entry1 := testentry.RefChangeLogEntry(setup.relativePath, "refs/heads/main", setup.commitIDs[0])
		proposeLogEntry(t, ctx, logManager, nil, entry1, setup.GetFileMappingForEntry(0))

		entry2 := testentry.RefChangeLogEntry(setup.relativePath, "refs/heads/main", setup.commitIDs[1])
		proposeLogEntry(t, ctx, logManager, nil, entry2, setup.GetFileMappingForEntry(1))

		simulatePositions(t, logManager, 2, 0)

		require.Equal(t, [][]storage.LSN{{1, 1}, {1, 2}}, mockConsumer.positions)
		require.Equal(t, storage.LSN(1), logManager.lowWaterMark())

		testhelper.RequireDirectoryState(t, logManager.stateDirectory, "", testhelper.DirectoryState{
			"/":                           {Mode: mode.Directory},
			"/wal":                        {Mode: mode.Directory},
			"/wal/0000000000001":          {Mode: mode.Directory},
			"/wal/0000000000001/MANIFEST": testentry.ManifestDirectoryEntry(entry2),
			"/wal/0000000000001/1":        {Mode: mode.File, Content: setup.GetFileContent(1)},
			"/wal/0000000000002":          {Mode: mode.Directory},
			"/wal/0000000000002/MANIFEST": testentry.ManifestDirectoryEntry(entry2),
			"/wal/0000000000002/1":        {Mode: mode.File, Content: setup.GetFileContent(1)},
		})
	})

	t.Run("acknowledge entries one by one", func(t *testing.T) {
		mockConsumer := &mockLogConsumer{}
		logManager := setupLogManager(t, ctx, mockConsumer)
		setup := setupTestRepo(t, ctx, 2)

		entry1 := testentry.RefChangeLogEntry(setup.relativePath, "refs/heads/main", setup.commitIDs[0])
		proposeLogEntry(t, ctx, logManager, nil, entry1, setup.GetFileMappingForEntry(0))
		simulatePositions(t, logManager, 1, 1)

		entry2 := testentry.RefChangeLogEntry(setup.relativePath, "refs/heads/main", setup.commitIDs[1])
		proposeLogEntry(t, ctx, logManager, nil, entry2, setup.GetFileMappingForEntry(1))
		simulatePositions(t, logManager, 2, 2)

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
		entry1 := testentry.RefChangeLogEntry(setup.relativePath, "refs/heads/main", setup.commitIDs[0])
		proposeLogEntry(t, ctx, logManager, nil, entry1, setup.GetFileMappingForEntry(0))
		simulatePositions(t, logManager, 0, 1)

		entry2 := testentry.RefChangeLogEntry(setup.relativePath, "refs/heads/main", setup.commitIDs[1])
		proposeLogEntry(t, ctx, logManager, nil, entry2, setup.GetFileMappingForEntry(1))
		simulatePositions(t, logManager, 0, 2)

		mockConsumer.busy.Store(false)
		entry3 := testentry.RefChangeLogEntry(setup.relativePath, "refs/heads/main", setup.commitIDs[2])
		proposeLogEntry(t, ctx, logManager, nil, entry3, setup.GetFileMappingForEntry(2))
		simulatePositions(t, logManager, 3, 3)

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

		entry1 := testentry.RefChangeLogEntry(setup.relativePath, "refs/heads/main", setup.commitIDs[0])
		proposeLogEntry(t, ctx, logManager, nil, entry1, setup.GetFileMappingForEntry(0))

		entry2 := testentry.RefChangeLogEntry(setup.relativePath, "refs/heads/main", setup.commitIDs[1])
		proposeLogEntry(t, ctx, logManager, nil, entry2, setup.GetFileMappingForEntry(1))

		ref := logManager.IncrementEntryReference(2)
		simulatePositions(t, logManager, 2, 2)

		entry3 := testentry.RefChangeLogEntry(setup.relativePath, "refs/heads/main", setup.commitIDs[2])
		proposeLogEntry(t, ctx, logManager, nil, entry3, setup.GetFileMappingForEntry(1))
		simulatePositions(t, logManager, 3, 3)

		require.Equal(t, [][]storage.LSN{{1, 1}, {1, 2}, {2, 3}}, mockConsumer.positions)
		require.Equal(t, storage.LSN(2), logManager.lowWaterMark())

		testhelper.RequireDirectoryState(t, logManager.stateDirectory, "", testhelper.DirectoryState{
			"/":                           {Mode: mode.Directory},
			"/wal":                        {Mode: mode.Directory},
			"/wal/0000000000002":          {Mode: mode.Directory},
			"/wal/0000000000002/MANIFEST": testentry.ManifestDirectoryEntry(entry2),
			"/wal/0000000000002/1":        {Mode: mode.File, Content: setup.GetFileContent(1)},
			"/wal/0000000000003":          {Mode: mode.Directory},
			"/wal/0000000000003/MANIFEST": testentry.ManifestDirectoryEntry(entry3),
			"/wal/0000000000003/1":        {Mode: mode.File, Content: setup.GetFileContent(2)},
		})

		logManager.DecrementEntryReference(ref)
		simulatePositions(t, logManager, 3, 3)
		require.Equal(t, storage.LSN(4), logManager.lowWaterMark())
		testhelper.RequireDirectoryState(t, logManager.stateDirectory, "", testhelper.DirectoryState{
			"/":    {Mode: mode.Directory},
			"/wal": {Mode: mode.Directory},
		})
	})
}
