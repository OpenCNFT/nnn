package partition

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
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

func setupLogManager(t *testing.T, ctx context.Context) *LogManager {
	database, err := keyvalue.NewBadgerStore(testhelper.SharedLogger(t), t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { testhelper.MustClose(t, database) })

	logManager := NewLogManager("test-storage", 1, database, testhelper.TempDir(t), testhelper.TempDir(t), nil)
	require.NoError(t, logManager.Initialize(ctx))

	return logManager
}

func TestLogManager_EntryReferences(t *testing.T) {
	t.Parallel()
	t.Run("empty entry references list", func(t *testing.T) {
		t.Parallel()
		ctx := testhelper.Context(t)
		logManager := setupLogManager(t, ctx)
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
		logManager := setupLogManager(t, ctx)
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
		logManager := setupLogManager(t, ctx)
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
		logManager := setupLogManager(t, ctx)
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
