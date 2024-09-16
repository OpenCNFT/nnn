package partition

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/mode"
)

func TestGetInode(t *testing.T) {
	t.Parallel()

	tempDir := t.TempDir()

	file1Path := filepath.Join(tempDir, "file-1")
	file2Path := filepath.Join(tempDir, "file-2")
	linkPath := filepath.Join(tempDir, "link-1")
	require.NoError(t, os.WriteFile(file1Path, nil, mode.Directory))
	require.NoError(t, os.WriteFile(file2Path, nil, mode.Directory))
	require.NoError(t, os.Link(file1Path, linkPath))

	file1Inode, err := getInode(file1Path)
	require.NoError(t, err)
	file2Inode, err := getInode(file2Path)
	require.NoError(t, err)
	linkInode, err := getInode(linkPath)
	require.NoError(t, err)

	require.Equal(t, file1Inode, linkInode)
	require.NotEqual(t, file1Inode, file2Inode)

	nonExistentInode, err := getInode(filepath.Join(tempDir, "non-existent"))
	require.NoError(t, err)
	require.EqualValues(t, nonExistentInode, 0)
}
