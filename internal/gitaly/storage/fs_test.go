package storage

import (
	"os"
	"path/filepath"
	"testing"
	"testing/fstest"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/mode"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper"
)

type operations []any

type mockFS struct {
	root       string
	operations operations
}

func newMockFS(root string) *mockFS { return &mockFS{root: root} }

func (m *mockFS) append(op any) {
	m.operations = append(m.operations, op)
}

func (m *mockFS) Root() string { return m.root }

type recordRead struct{ path string }

func (m *mockFS) RecordRead(path string) error {
	m.append(recordRead{path: path})
	return nil
}

type recordRemoval struct{ path string }

func (m *mockFS) RecordRemoval(path string) error {
	m.append(recordRemoval{path: path})
	return nil
}

type recordFile struct{ path string }

func (m *mockFS) RecordFile(path string) error {
	m.append(recordFile{path: path})
	return nil
}

type recordLink struct{ sourcePath, destinationPath string }

func (m *mockFS) RecordLink(sourcePath, destinationPath string) error {
	m.append(recordLink{sourcePath: sourcePath, destinationPath: destinationPath})
	return nil
}

type recordDirectory struct{ path string }

func (m *mockFS) RecordDirectory(path string) error {
	m.append(recordDirectory{path: path})
	return nil
}

func TestFS_recordingHelpers(t *testing.T) {
	t.Parallel()

	storageRoot := t.TempDir()

	setupTestDirectory := func(t *testing.T, path string) {
		require.NoError(t, os.MkdirAll(path, mode.Directory))
		require.NoError(t, os.WriteFile(filepath.Join(path, "file-1"), []byte("file-1"), mode.Executable))
		privateSubDir := filepath.Join(filepath.Join(path, "subdir-private"))
		require.NoError(t, os.Mkdir(privateSubDir, mode.Directory))
		require.NoError(t, os.WriteFile(filepath.Join(privateSubDir, "file-2"), []byte("file-2"), mode.File))
		sharedSubDir := filepath.Join(path, "subdir-shared")
		require.NoError(t, os.Mkdir(sharedSubDir, mode.Directory))
		require.NoError(t, os.WriteFile(filepath.Join(sharedSubDir, "file-3"), []byte("file-3"), mode.File))
	}

	firstLevelDir := "test-dir"
	secondLevelDir := "second-level/test-dir"
	require.NoError(t, os.WriteFile(filepath.Join(storageRoot, "root-file"), []byte("root file"), mode.File))
	setupTestDirectory(t, filepath.Join(storageRoot, firstLevelDir))
	setupTestDirectory(t, filepath.Join(storageRoot, secondLevelDir))

	for _, tc := range []struct {
		desc               string
		run                func(*testing.T, FS)
		expectedOperations operations
		expectedFiles      testhelper.DirectoryState
	}{
		{
			desc: "RecordDirectoryCreation on first level directory",
			run: func(t *testing.T, fs FS) {
				require.NoError(t, RecordDirectoryCreation(fs, firstLevelDir))
			},
			expectedOperations: operations{
				recordDirectory{path: "test-dir"},
				recordFile{path: "test-dir/file-1"},
				recordDirectory{path: "test-dir/subdir-private"},
				recordFile{path: "test-dir/subdir-private/file-2"},
				recordDirectory{path: "test-dir/subdir-shared"},
				recordFile{path: "test-dir/subdir-shared/file-3"},
			},
		},
		{
			desc: "RecordDirectoryCreation on second level directory",
			run: func(t *testing.T, fs FS) {
				require.NoError(t, RecordDirectoryCreation(fs, secondLevelDir))
			},
			expectedOperations: operations{
				recordDirectory{path: "second-level/test-dir"},
				recordFile{path: "second-level/test-dir/file-1"},
				recordDirectory{path: "second-level/test-dir/subdir-private"},
				recordFile{path: "second-level/test-dir/subdir-private/file-2"},
				recordDirectory{path: "second-level/test-dir/subdir-shared"},
				recordFile{path: "second-level/test-dir/subdir-shared/file-3"},
			},
		},
		{
			desc: "RecordDirectoryRemoval on first level directory",
			run: func(t *testing.T, fs FS) {
				require.NoError(t, RecordDirectoryRemoval(fs, fs.Root(), firstLevelDir))
			},
			expectedOperations: operations{
				recordRemoval{path: "test-dir/file-1"},
				recordRemoval{path: "test-dir/subdir-private/file-2"},
				recordRemoval{path: "test-dir/subdir-private"},
				recordRemoval{path: "test-dir/subdir-shared/file-3"},
				recordRemoval{path: "test-dir/subdir-shared"},
				recordRemoval{path: "test-dir"},
			},
		},
		{
			desc: "RecordDirectoryRemoval on second level directory",
			run: func(t *testing.T, fs FS) {
				require.NoError(t, RecordDirectoryRemoval(fs, fs.Root(), secondLevelDir))
			},
			expectedOperations: operations{
				recordRemoval{path: "second-level/test-dir/file-1"},
				recordRemoval{path: "second-level/test-dir/subdir-private/file-2"},
				recordRemoval{path: "second-level/test-dir/subdir-private"},
				recordRemoval{path: "second-level/test-dir/subdir-shared/file-3"},
				recordRemoval{path: "second-level/test-dir/subdir-shared"},
				recordRemoval{path: "second-level/test-dir"},
			},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			f := newMockFS(storageRoot)
			tc.run(t, f)
			require.Equal(t, tc.expectedOperations, f.operations)
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
			expectedOperations: operations{
				recordDirectory{path: "target/objects/3f"},
				recordLink{sourcePath: "source/objects/3f/1", destinationPath: "target/objects/3f/1"},
				recordLink{sourcePath: "source/objects/3f/2", destinationPath: "target/objects/3f/2"},
				recordDirectory{path: "target/objects/4f"},
				recordLink{sourcePath: "source/objects/4f/3", destinationPath: "target/objects/4f/3"},
				recordLink{sourcePath: "source/objects/pack/pack.idx", destinationPath: "target/objects/pack/pack.idx"},
				recordLink{sourcePath: "source/objects/pack/pack.pack", destinationPath: "target/objects/pack/pack.pack"},
				recordRemoval{path: "target/objects/info/alternates"},
			},
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
			expectedOperations: operations{
				recordLink{sourcePath: "source/objects/3f/2", destinationPath: "target/objects/3f/2"},
				recordLink{sourcePath: "source/objects/pack/pack.pack", destinationPath: "target/objects/pack/pack.pack"},
				recordRemoval{path: "target/objects/info/alternates"},
			},
		},
		{
			desc:         "target with fully matching object state",
			createTarget: createSourceHierarchy,
			expectedOperations: operations{
				recordRemoval{path: "target/objects/info/alternates"},
			},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			storageRoot := t.TempDir()
			createSourceHierarchy(t, filepath.Join(storageRoot, "source"))

			tc.createTarget(t, filepath.Join(storageRoot, "target"))

			fs := newMockFS(storageRoot)
			require.NoError(t, RecordAlternateUnlink(fs, fs.Root(), "target", "../../source/objects"))
			require.Equal(t, tc.expectedOperations, fs.operations)
		})
	}
}
