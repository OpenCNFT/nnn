package fsrecorder

import (
	"io/fs"
	"os"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/mode"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/wal"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
)

func TestFS(t *testing.T) {
	t.Run("Root", func(t *testing.T) {
		root := t.TempDir()
		require.Equal(t, root, NewFS(root, wal.NewEntry(t.TempDir())).Root())
	})

	t.Run("Mkdir", func(t *testing.T) {
		t.Run("targeting root fails", func(t *testing.T) {
			f := NewFS(t.TempDir(), wal.NewEntry(t.TempDir()))

			require.Equal(t, newPathEscapesRootError(""), f.Mkdir(""))
			require.Nil(t, f.entry.Operations())
		})

		t.Run("escaping root fails", func(t *testing.T) {
			f := NewFS(t.TempDir(), wal.NewEntry(t.TempDir()))

			const path = "../non-root-path"
			require.Equal(t, newPathEscapesRootError(path), f.Mkdir(path))
			require.Nil(t, f.entry.Operations())
		})

		t.Run("fails if parent does not exist", func(t *testing.T) {
			f := NewFS(t.TempDir(), wal.NewEntry(t.TempDir()))

			require.ErrorIs(t, f.Mkdir("non-existent/target"), fs.ErrNotExist)
			require.Nil(t, f.entry.Operations())
		})

		t.Run("fails if target exists", func(t *testing.T) {
			f := NewFS(t.TempDir(), wal.NewEntry(t.TempDir()))

			require.NoError(t, os.Mkdir(filepath.Join(f.root, "target"), mode.Directory))

			require.ErrorIs(t, f.Mkdir("target"), fs.ErrExist)
			require.Nil(t, f.entry.Operations())
		})

		t.Run("successfully creates directories", func(t *testing.T) {
			f := NewFS(t.TempDir(), wal.NewEntry(t.TempDir()))

			require.NoError(t, f.Mkdir("parent"))
			require.NoError(t, f.Mkdir("parent/child"))
			require.Equal(t,
				[]*gitalypb.LogEntry_Operation{
					{
						Operation: &gitalypb.LogEntry_Operation_CreateDirectory_{
							CreateDirectory: &gitalypb.LogEntry_Operation_CreateDirectory{
								Path: []byte("parent"),
								Mode: uint32(mode.Directory),
							},
						},
					},
					{
						Operation: &gitalypb.LogEntry_Operation_CreateDirectory_{
							CreateDirectory: &gitalypb.LogEntry_Operation_CreateDirectory{
								Path: []byte("parent/child"),
								Mode: uint32(mode.Directory),
							},
						},
					},
				},
				f.entry.Operations(),
			)
		})
	})

	t.Run("MkdirAll", func(t *testing.T) {
		t.Run("targeting root fails", func(t *testing.T) {
			f := NewFS(t.TempDir(), wal.NewEntry(t.TempDir()))

			require.Equal(t, newPathEscapesRootError(""), f.MkdirAll(""))
			require.Nil(t, f.entry.Operations())
		})

		t.Run("escaping root fails", func(t *testing.T) {
			f := NewFS(t.TempDir(), wal.NewEntry(t.TempDir()))

			const path = "../non-root-path"
			require.Equal(t, newPathEscapesRootError(path), f.Mkdir(path))
			require.Nil(t, f.entry.Operations())
		})

		t.Run("target under a file", func(t *testing.T) {
			f := NewFS(t.TempDir(), wal.NewEntry(t.TempDir()))

			require.NoError(t, os.WriteFile(filepath.Join(f.root, "file"), nil, mode.File))

			require.ErrorIs(t, f.MkdirAll("file/target"), syscall.ENOTDIR)
			require.Nil(t, f.entry.Operations())
		})

		t.Run("target is a file", func(t *testing.T) {
			f := NewFS(t.TempDir(), wal.NewEntry(t.TempDir()))

			require.NoError(t, os.WriteFile(filepath.Join(f.root, "file"), nil, mode.File))

			require.Equal(t, newTargetIsFileError("file"), f.MkdirAll("file"))
			require.Nil(t, f.entry.Operations())
		})

		t.Run("target exists", func(t *testing.T) {
			f := NewFS(t.TempDir(), wal.NewEntry(t.TempDir()))

			require.NoError(t, os.MkdirAll(filepath.Join(f.root, "parent/target"), mode.Directory))

			require.NoError(t, f.MkdirAll("parent/target"))
			require.Nil(t, f.entry.Operations())
		})

		t.Run("successfully creates missing directories", func(t *testing.T) {
			f := NewFS(t.TempDir(), wal.NewEntry(t.TempDir()))

			require.NoError(t, os.MkdirAll(filepath.Join(f.root, "parent"), mode.Directory))

			require.NoError(t, f.MkdirAll("parent/child/target"))
			require.Equal(t,
				[]*gitalypb.LogEntry_Operation{
					{
						Operation: &gitalypb.LogEntry_Operation_CreateDirectory_{
							CreateDirectory: &gitalypb.LogEntry_Operation_CreateDirectory{
								Path: []byte("parent/child"),
								Mode: uint32(mode.Directory),
							},
						},
					},
					{
						Operation: &gitalypb.LogEntry_Operation_CreateDirectory_{
							CreateDirectory: &gitalypb.LogEntry_Operation_CreateDirectory{
								Path: []byte("parent/child/target"),
								Mode: uint32(mode.Directory),
							},
						},
					},
				},
				f.entry.Operations(),
			)
		})
	})
}
