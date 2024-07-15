package permission

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestOwnerRead(t *testing.T) {
	require.Equal(t, "-r--------", OwnerRead.String())
}

func TestOwnerWrite(t *testing.T) {
	require.Equal(t, "--w-------", OwnerWrite.String())
}

func TestOwnerExecute(t *testing.T) {
	require.Equal(t, "---x------", OwnerExecute.String())
}

func TestGroupRead(t *testing.T) {
	require.Equal(t, "----r-----", GroupRead.String())
}

func TestGroupWrite(t *testing.T) {
	require.Equal(t, "-----w----", GroupWrite.String())
}

func TestGroupExecute(t *testing.T) {
	require.Equal(t, "------x---", GroupExecute.String())
}

func TestOthersRead(t *testing.T) {
	require.Equal(t, "-------r--", OthersRead.String())
}

func TestOthersWrite(t *testing.T) {
	require.Equal(t, "--------w-", OthersWrite.String())
}

func TestOthersExecute(t *testing.T) {
	require.Equal(t, "---------x", OthersExecute.String())
}
