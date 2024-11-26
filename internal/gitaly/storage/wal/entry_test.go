package wal

import (
	"io/fs"
	"os"
	"path/filepath"
	"testing"
	"testing/fstest"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/mode"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper"
)

func setupTestDirectory(t *testing.T, path string) {
	require.NoError(t, os.MkdirAll(path, mode.Directory))
	require.NoError(t, os.WriteFile(filepath.Join(path, "file-1"), []byte("file-1"), mode.Executable))
	privateSubDir := filepath.Join(filepath.Join(path, "subdir-private"))
	require.NoError(t, os.Mkdir(privateSubDir, mode.Directory))
	require.NoError(t, os.WriteFile(filepath.Join(privateSubDir, "file-2"), []byte("file-2"), mode.File))
	sharedSubDir := filepath.Join(path, "subdir-shared")
	require.NoError(t, os.Mkdir(sharedSubDir, mode.Directory))
	require.NoError(t, os.WriteFile(filepath.Join(sharedSubDir, "file-3"), []byte("file-3"), mode.File))
}

func TestEntry_Directory(t *testing.T) {
	t.Parallel()

	stateDir := t.TempDir()
	require.Equal(t, stateDir, NewEntry(stateDir).Directory())
}

func TestEntry(t *testing.T) {
	t.Parallel()

	storageRoot := t.TempDir()

	firstLevelDir := "test-dir"
	secondLevelDir := "second-level/test-dir"
	require.NoError(t, os.WriteFile(filepath.Join(storageRoot, "root-file"), []byte("root file"), mode.File))
	setupTestDirectory(t, filepath.Join(storageRoot, firstLevelDir))
	setupTestDirectory(t, filepath.Join(storageRoot, secondLevelDir))

	symlinkPath := filepath.Join(storageRoot, "symlink-to-file")
	require.NoError(t, os.Symlink(
		filepath.Join(storageRoot, "root-file"),
		symlinkPath,
	))

	rootDirPerm := testhelper.Umask().Mask(fs.ModePerm)

	for _, tc := range []struct {
		desc               string
		run                func(*testing.T, *Entry)
		expectedOperations operations
		expectedFiles      testhelper.DirectoryState
	}{
		{
			desc: "stage non-regular file",
			run: func(t *testing.T, entry *Entry) {
				_, err := entry.stageFile(symlinkPath)
				require.Equal(t, newIrregularFileStagedError(fs.ModeSymlink), err)
			},
			expectedOperations: func() operations {
				var ops operations
				return ops
			}(),
			expectedFiles: testhelper.DirectoryState{
				"/": {Mode: fs.ModeDir | rootDirPerm},
			},
		},
		{
			desc: "RecordFileCreation",
			run: func(t *testing.T, entry *Entry) {
				require.NoError(t, entry.RecordFileCreation(
					filepath.Join(storageRoot, "root-file"),
					"test-dir/file-1",
				))
			},
			expectedOperations: func() operations {
				var ops operations
				ops.createHardLink("1", "test-dir/file-1", false)
				return ops
			}(),
			expectedFiles: testhelper.DirectoryState{
				"/":  {Mode: fs.ModeDir | rootDirPerm},
				"/1": {Mode: mode.File, Content: []byte("root file")},
			},
		},
		{
			desc: "RecordDirectoryEntryRemoval",
			run: func(t *testing.T, entry *Entry) {
				entry.RecordDirectoryEntryRemoval("test-dir/file-1")
			},
			expectedOperations: func() operations {
				var ops operations
				ops.removeDirectoryEntry("test-dir/file-1")
				return ops
			}(),
			expectedFiles: testhelper.DirectoryState{
				"/": {Mode: fs.ModeDir | rootDirPerm},
			},
		},
		{
			desc: "RecordFileUpdate on root level file",
			run: func(t *testing.T, entry *Entry) {
				require.NoError(t, entry.RecordFileUpdate(storageRoot, "root-file"))
			},
			expectedOperations: func() operations {
				var ops operations
				ops.removeDirectoryEntry("root-file")
				ops.createHardLink("1", "root-file", false)
				return ops
			}(),
			expectedFiles: testhelper.DirectoryState{
				"/":  {Mode: fs.ModeDir | rootDirPerm},
				"/1": {Mode: mode.File, Content: []byte("root file")},
			},
		},
		{
			desc: "RecordFileUpdate on first level file",
			run: func(t *testing.T, entry *Entry) {
				require.NoError(t, entry.RecordFileUpdate(storageRoot, filepath.Join(firstLevelDir, "file-1")))
			},
			expectedOperations: func() operations {
				var ops operations
				ops.removeDirectoryEntry("test-dir/file-1")
				ops.createHardLink("1", "test-dir/file-1", false)
				return ops
			}(),
			expectedFiles: testhelper.DirectoryState{
				"/":  {Mode: fs.ModeDir | rootDirPerm},
				"/1": {Mode: mode.Executable, Content: []byte("file-1")},
			},
		},
		{
			desc: "RecordDirectoryCreation on first level directory",
			run: func(t *testing.T, entry *Entry) {
				require.NoError(t, entry.RecordDirectoryCreation(storageRoot, firstLevelDir))
			},
			expectedOperations: func() operations {
				var ops operations
				ops.createDirectory("test-dir")
				ops.createHardLink("1", "test-dir/file-1", false)
				ops.createDirectory("test-dir/subdir-private")
				ops.createHardLink("2", "test-dir/subdir-private/file-2", false)
				ops.createDirectory("test-dir/subdir-shared")
				ops.createHardLink("3", "test-dir/subdir-shared/file-3", false)
				return ops
			}(),
			expectedFiles: testhelper.DirectoryState{
				"/":  {Mode: fs.ModeDir | rootDirPerm},
				"/1": {Mode: mode.Executable, Content: []byte("file-1")},
				"/2": {Mode: mode.File, Content: []byte("file-2")},
				"/3": {Mode: mode.File, Content: []byte("file-3")},
			},
		},
		{
			desc: "RecordDirectoryCreation on second level directory",
			run: func(t *testing.T, entry *Entry) {
				require.NoError(t, entry.RecordDirectoryCreation(storageRoot, secondLevelDir))
			},
			expectedOperations: func() operations {
				var ops operations
				ops.createDirectory("second-level/test-dir")
				ops.createHardLink("1", "second-level/test-dir/file-1", false)
				ops.createDirectory("second-level/test-dir/subdir-private")
				ops.createHardLink("2", "second-level/test-dir/subdir-private/file-2", false)
				ops.createDirectory("second-level/test-dir/subdir-shared")
				ops.createHardLink("3", "second-level/test-dir/subdir-shared/file-3", false)
				return ops
			}(),
			expectedFiles: testhelper.DirectoryState{
				"/":  {Mode: fs.ModeDir | rootDirPerm},
				"/1": {Mode: mode.Executable, Content: []byte("file-1")},
				"/2": {Mode: mode.File, Content: []byte("file-2")},
				"/3": {Mode: mode.File, Content: []byte("file-3")},
			},
		},
		{
			desc: "RecordDirectoryRemoval on first level directory",
			run: func(t *testing.T, entry *Entry) {
				require.NoError(t, entry.RecordDirectoryRemoval(storageRoot, firstLevelDir))
			},
			expectedOperations: func() operations {
				var ops operations
				ops.removeDirectoryEntry("test-dir/file-1")
				ops.removeDirectoryEntry("test-dir/subdir-private/file-2")
				ops.removeDirectoryEntry("test-dir/subdir-private")
				ops.removeDirectoryEntry("test-dir/subdir-shared/file-3")
				ops.removeDirectoryEntry("test-dir/subdir-shared")
				ops.removeDirectoryEntry("test-dir")
				return ops
			}(),
			expectedFiles: testhelper.DirectoryState{
				"/": {Mode: fs.ModeDir | rootDirPerm},
			},
		},
		{
			desc: "RecordDirectoryRemoval on second level directory",
			run: func(t *testing.T, entry *Entry) {
				require.NoError(t, entry.RecordDirectoryRemoval(storageRoot, secondLevelDir))
			},
			expectedOperations: func() operations {
				var ops operations
				ops.removeDirectoryEntry("second-level/test-dir/file-1")
				ops.removeDirectoryEntry("second-level/test-dir/subdir-private/file-2")
				ops.removeDirectoryEntry("second-level/test-dir/subdir-private")
				ops.removeDirectoryEntry("second-level/test-dir/subdir-shared/file-3")
				ops.removeDirectoryEntry("second-level/test-dir/subdir-shared")
				ops.removeDirectoryEntry("second-level/test-dir")
				return ops
			}(),
			expectedFiles: testhelper.DirectoryState{
				"/": {Mode: fs.ModeDir | rootDirPerm},
			},
		},
		{
			desc: "key value operations",
			run: func(t *testing.T, entry *Entry) {
				entry.SetKey([]byte("set-key"), []byte("value"))
				entry.DeleteKey([]byte("deleted-key"))
			},
			expectedOperations: func() operations {
				var ops operations
				ops.setKey([]byte("set-key"), []byte("value"))
				ops.deleteKey([]byte("deleted-key"))
				return ops
			}(),
			expectedFiles: testhelper.DirectoryState{
				"/": {Mode: fs.ModeDir | rootDirPerm},
			},
		},
		{
			desc: "Mkdir",
			run: func(t *testing.T, entry *Entry) {
				entry.RecordMkdir("parent/target")
			},
			expectedOperations: func() operations {
				var ops operations
				ops.createDirectory("parent/target")
				return ops
			}(),
			expectedFiles: testhelper.DirectoryState{
				"/": {Mode: fs.ModeDir | rootDirPerm},
			},
		},
		{
			desc: "CreateLink",
			run: func(t *testing.T, entry *Entry) {
				entry.CreateLink("parent/source", "target")
			},
			expectedOperations: func() operations {
				var ops operations
				ops.createHardLink("parent/source", "target", true)
				return ops
			}(),
			expectedFiles: testhelper.DirectoryState{
				"/": {Mode: fs.ModeDir | rootDirPerm},
			},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			stateDir := t.TempDir()
			entry := NewEntry(stateDir)

			tc.run(t, entry)

			testhelper.ProtoEqual(t, tc.expectedOperations, entry.operations)
			testhelper.RequireDirectoryState(t, stateDir, "", tc.expectedFiles)
		})
	}
}

func TestRecordAlternateUnlink(t *testing.T) {
	t.Parallel()

	createSourceHierarchy := func(tb testing.TB, path string) {
		testhelper.CreateFS(tb, path, fstest.MapFS{
			".":                      {Mode: mode.Directory},
			"objects":                {Mode: mode.Directory},
			"objects/info":           {Mode: mode.Directory},
			"objects/3f":             {Mode: mode.Directory},
			"objects/3f/1":           {Mode: mode.File},
			"objects/3f/2":           {Mode: mode.File},
			"objects/4f":             {Mode: mode.Directory},
			"objects/4f/3":           {Mode: mode.File},
			"objects/pack":           {Mode: mode.Directory},
			"objects/pack/pack.pack": {Mode: mode.File},
			"objects/pack/pack.idx":  {Mode: mode.File},
		})
	}

	for _, tc := range []struct {
		desc               string
		createTarget       func(tb testing.TB, path string)
		expectedOperations operations
	}{
		{
			desc: "empty target",
			createTarget: func(tb testing.TB, path string) {
				require.NoError(tb, os.Mkdir(path, mode.Directory))
				require.NoError(tb, os.Mkdir(filepath.Join(path, "objects"), mode.Directory))
				require.NoError(tb, os.Mkdir(filepath.Join(path, "objects/pack"), mode.Directory))
			},
			expectedOperations: func() operations {
				var ops operations
				ops.createDirectory("target/objects/3f")
				ops.createHardLink("source/objects/3f/1", "target/objects/3f/1", true)
				ops.createHardLink("source/objects/3f/2", "target/objects/3f/2", true)
				ops.createDirectory("target/objects/4f")
				ops.createHardLink("source/objects/4f/3", "target/objects/4f/3", true)
				ops.createHardLink("source/objects/pack/pack.idx", "target/objects/pack/pack.idx", true)
				ops.createHardLink("source/objects/pack/pack.pack", "target/objects/pack/pack.pack", true)
				ops.removeDirectoryEntry("target/objects/info/alternates")
				return ops
			}(),
		},
		{
			desc: "target with some existing state",
			createTarget: func(tb testing.TB, path string) {
				testhelper.CreateFS(tb, path, fstest.MapFS{
					".":                     {Mode: mode.Directory},
					"objects":               {Mode: mode.Directory},
					"objects/3f":            {Mode: mode.Directory},
					"objects/3f/1":          {Mode: mode.File},
					"objects/4f":            {Mode: mode.Directory},
					"objects/4f/3":          {Mode: mode.File},
					"objects/pack":          {Mode: mode.Directory},
					"objects/pack/pack.idx": {Mode: mode.File},
				})
			},
			expectedOperations: func() operations {
				var ops operations
				ops.createHardLink("source/objects/3f/2", "target/objects/3f/2", true)
				ops.createHardLink("source/objects/pack/pack.pack", "target/objects/pack/pack.pack", true)
				ops.removeDirectoryEntry("target/objects/info/alternates")
				return ops
			}(),
		},
		{
			desc:         "target with fully matching object state",
			createTarget: createSourceHierarchy,
			expectedOperations: func() operations {
				var ops operations
				ops.removeDirectoryEntry("target/objects/info/alternates")
				return ops
			}(),
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			storageRoot := t.TempDir()
			createSourceHierarchy(t, filepath.Join(storageRoot, "source"))

			tc.createTarget(t, filepath.Join(storageRoot, "target"))

			stateDirectory := t.TempDir()
			entry := NewEntry(stateDirectory)
			require.NoError(t, entry.RecordAlternateUnlink(storageRoot, "target", "../../source/objects"))

			testhelper.ProtoEqual(t, tc.expectedOperations, entry.operations)
		})
	}
}
