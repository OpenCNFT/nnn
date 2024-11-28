package fsrecorder

import (
	"io/fs"
	"os"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/mode"
)

type recordingWALBuilder struct {
	operations []any
}

type createDirectory struct{ path string }

func (r *recordingWALBuilder) CreateDirectory(path string) {
	r.append(createDirectory{path: path})
}

type removeDirectoryEntry struct{ path string }

func (r *recordingWALBuilder) RemoveDirectoryEntry(path string) {
	r.append(removeDirectoryEntry{path: path})
}

type createFile struct{ sourceAbsolutePath, path string }

func (r *recordingWALBuilder) CreateFile(sourceAbsolutePath, path string) error {
	r.append(createFile{sourceAbsolutePath: sourceAbsolutePath, path: path})
	return nil
}

type createLink struct{ sourcePath, destinationPath string }

func (r *recordingWALBuilder) CreateLink(sourcePath, destinationPath string) {
	r.append(createLink{sourcePath: sourcePath, destinationPath: destinationPath})
}

func (r *recordingWALBuilder) append(op any) {
	r.operations = append(r.operations, op)
}

func TestFS(t *testing.T) {
	t.Run("Root", func(t *testing.T) {
		root := t.TempDir()
		require.Equal(t, root, NewFS(root, &recordingWALBuilder{}).Root())
	})

	testPathValidation := func(t *testing.T, run func(FS, string) error) {
		t.Run("path validation", func(t *testing.T) {
			f := NewFS(t.TempDir(), &recordingWALBuilder{})

			t.Run("targeting root fails", func(t *testing.T) {
				require.ErrorIs(t, run(f, ""), newPathEscapesRootError(""))
			})

			t.Run("escaping root fails", func(t *testing.T) {
				const path = "../non-root-path"
				require.ErrorIs(t, run(f, path), newPathEscapesRootError(path))
			})

			require.Equal(t, &recordingWALBuilder{}, f.wal)
		})
	}

	t.Run("Mkdir", func(t *testing.T) {
		testPathValidation(t, func(f FS, path string) error { return f.Mkdir(path) })

		t.Run("fails if parent does not exist", func(t *testing.T) {
			f := NewFS(t.TempDir(), &recordingWALBuilder{})

			require.ErrorIs(t, f.Mkdir("non-existent/target"), fs.ErrNotExist)
			require.Equal(t, &recordingWALBuilder{}, f.wal)
		})

		t.Run("fails if target exists", func(t *testing.T) {
			f := NewFS(t.TempDir(), &recordingWALBuilder{})

			require.NoError(t, os.Mkdir(filepath.Join(f.root, "target"), mode.Directory))

			require.ErrorIs(t, f.Mkdir("target"), fs.ErrExist)
			require.Equal(t, &recordingWALBuilder{}, f.wal)
		})

		t.Run("successfully creates directories", func(t *testing.T) {
			f := NewFS(t.TempDir(), &recordingWALBuilder{})

			require.NoError(t, f.Mkdir("parent"))
			require.NoError(t, f.Mkdir("parent/child"))
			require.Equal(t,
				&recordingWALBuilder{operations: []any{
					createDirectory{path: "parent"},
					createDirectory{path: "parent/child"},
				}},
				f.wal,
			)
		})
	})

	t.Run("MkdirAll", func(t *testing.T) {
		testPathValidation(t, func(f FS, path string) error { return f.MkdirAll(path) })

		t.Run("target under a file", func(t *testing.T) {
			f := NewFS(t.TempDir(), &recordingWALBuilder{})

			require.NoError(t, os.WriteFile(filepath.Join(f.root, "file"), nil, mode.File))

			require.ErrorIs(t, f.MkdirAll("file/target"), syscall.ENOTDIR)
			require.Equal(t, &recordingWALBuilder{}, f.wal)
		})

		t.Run("target is a file", func(t *testing.T) {
			f := NewFS(t.TempDir(), &recordingWALBuilder{})

			require.NoError(t, os.WriteFile(filepath.Join(f.root, "file"), nil, mode.File))

			require.Equal(t, newTargetIsFileError("file"), f.MkdirAll("file"))
			require.Equal(t, &recordingWALBuilder{}, f.wal)
		})

		t.Run("target exists", func(t *testing.T) {
			f := NewFS(t.TempDir(), &recordingWALBuilder{})

			require.NoError(t, os.MkdirAll(filepath.Join(f.root, "parent/target"), mode.Directory))

			require.NoError(t, f.MkdirAll("parent/target"))
			require.Equal(t, &recordingWALBuilder{}, f.wal)
		})

		t.Run("successfully creates missing directories", func(t *testing.T) {
			f := NewFS(t.TempDir(), &recordingWALBuilder{})

			require.NoError(t, os.MkdirAll(filepath.Join(f.root, "parent"), mode.Directory))

			require.NoError(t, f.MkdirAll("parent/child/target"))
			require.Equal(t,
				&recordingWALBuilder{operations: []any{
					createDirectory{path: "parent/child"},
					createDirectory{path: "parent/child/target"},
				}},
				f.wal,
			)
		})
	})

	t.Run("RecordRead", func(t *testing.T) {
		testPathValidation(t, func(f FS, path string) error { return f.RecordRead(path) })

		f := NewFS(t.TempDir(), &recordingWALBuilder{})

		require.Equal(t, ReadSet{}, f.ReadSet())

		require.NoError(t, f.RecordRead("parent"))
		require.NoError(t, f.RecordRead("parent/not-read/child"))

		require.Equal(t, ReadSet{
			"parent":                {},
			"parent/not-read/child": {},
		}, f.ReadSet())
	})

	t.Run("RecordRemoval", func(t *testing.T) {
		testPathValidation(t, func(f FS, path string) error { return f.RecordRemoval(path) })

		f := NewFS(t.TempDir(), &recordingWALBuilder{})

		require.NoError(t, f.RecordRemoval("parent/target"))
		require.Equal(t,
			&recordingWALBuilder{operations: []any{
				removeDirectoryEntry{path: "parent/target"},
			}},
			f.wal,
		)
	})

	t.Run("RecordFile", func(t *testing.T) {
		testPathValidation(t, func(f FS, path string) error { return f.RecordFile(path) })

		f := NewFS(t.TempDir(), &recordingWALBuilder{})

		require.NoError(t, f.RecordFile("parent/target"))
		require.Equal(t,
			&recordingWALBuilder{operations: []any{
				createFile{sourceAbsolutePath: filepath.Join(f.Root(), "parent/target"), path: "parent/target"},
			}},
			f.wal,
		)
	})

	t.Run("RecordLink", func(t *testing.T) {
		t.Run("source path", func(t *testing.T) {
			testPathValidation(t, func(f FS, path string) error { return f.RecordLink(path, "valid") })
		})

		t.Run("destination path", func(t *testing.T) {
			testPathValidation(t, func(f FS, path string) error { return f.RecordLink("valid", path) })
		})

		f := NewFS(t.TempDir(), &recordingWALBuilder{})

		require.NoError(t, f.RecordLink("source", "parent/target"))
		require.Equal(t,
			&recordingWALBuilder{operations: []any{
				createLink{sourcePath: "source", destinationPath: "parent/target"},
			}},
			f.wal,
		)
	})

	t.Run("RecordDirectory", func(t *testing.T) {
		testPathValidation(t, func(f FS, path string) error { return f.RecordDirectory(path) })

		f := NewFS(t.TempDir(), &recordingWALBuilder{})

		require.NoError(t, f.RecordDirectory("parent/target"))
		require.Equal(t,
			&recordingWALBuilder{operations: []any{
				createDirectory{path: "parent/target"},
			}},
			f.wal,
		)
	})
}
