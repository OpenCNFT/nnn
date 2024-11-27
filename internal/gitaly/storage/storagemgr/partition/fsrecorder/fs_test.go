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

type recordMkdir struct{ path string }

func (r *recordingWALBuilder) RecordMkdir(path string) {
	r.append(recordMkdir{path: path})
}

func (r *recordingWALBuilder) append(op any) {
	r.operations = append(r.operations, op)
}

func TestFS(t *testing.T) {
	t.Run("Root", func(t *testing.T) {
		root := t.TempDir()
		require.Equal(t, root, NewFS(root, &recordingWALBuilder{}).Root())
	})

	t.Run("Mkdir", func(t *testing.T) {
		t.Run("targeting root fails", func(t *testing.T) {
			f := NewFS(t.TempDir(), &recordingWALBuilder{})

			require.Equal(t, newPathEscapesRootError(""), f.Mkdir(""))
			require.Equal(t, &recordingWALBuilder{}, f.wal)
		})

		t.Run("escaping root fails", func(t *testing.T) {
			f := NewFS(t.TempDir(), &recordingWALBuilder{})

			const path = "../non-root-path"
			require.Equal(t, newPathEscapesRootError(path), f.Mkdir(path))
			require.Equal(t, &recordingWALBuilder{}, f.wal)
		})

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
					recordMkdir{path: "parent"},
					recordMkdir{path: "parent/child"},
				}},
				f.wal,
			)
		})
	})

	t.Run("MkdirAll", func(t *testing.T) {
		t.Run("targeting root fails", func(t *testing.T) {
			f := NewFS(t.TempDir(), &recordingWALBuilder{})

			require.Equal(t, newPathEscapesRootError(""), f.MkdirAll(""))
			require.Equal(t, &recordingWALBuilder{}, f.wal)
		})

		t.Run("escaping root fails", func(t *testing.T) {
			f := NewFS(t.TempDir(), &recordingWALBuilder{})

			const path = "../non-root-path"
			require.Equal(t, newPathEscapesRootError(path), f.Mkdir(path))
			require.Equal(t, &recordingWALBuilder{}, f.wal)
		})

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
					recordMkdir{path: "parent/child"},
					recordMkdir{path: "parent/child/target"},
				}},
				f.wal,
			)
		})
	})
}
