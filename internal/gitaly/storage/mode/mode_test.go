package mode

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDirectory(t *testing.T) {
	require.Equal(t, "drwx------", Directory.String())
}

func TestExecutable(t *testing.T) {
	require.Equal(t, "-r-x------", Executable.String())
}

func TestFile(t *testing.T) {
	require.Equal(t, "-r--------", File.String())
}
