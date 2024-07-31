package storagemgr

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/keyvalue"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/mode"
)

func TestMayHavePendingWAL(t *testing.T) {
	storage1 := t.TempDir()
	storage2 := t.TempDir()

	mayHaveWAL, err := MayHavePendingWAL([]string{storage1, storage2})
	require.NoError(t, err)
	require.False(t, mayHaveWAL)

	require.NoError(t, os.MkdirAll(keyvalue.DatabaseDirectoryPath(storage2), mode.Directory))

	mayHaveWAL, err = MayHavePendingWAL([]string{storage1, storage2})
	require.NoError(t, err)
	require.True(t, mayHaveWAL)
}
