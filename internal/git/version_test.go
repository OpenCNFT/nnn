package git

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestVersion_LessThan(t *testing.T) {
	for _, tc := range []struct {
		smaller, larger string
	}{
		{"0.0.0", "0.0.0"},
		{"0.0.0", "0.0.1"},
		{"0.0.0", "0.1.0"},
		{"0.0.0", "0.1.1"},
		{"0.0.0", "1.0.0"},
		{"0.0.0", "1.0.1"},
		{"0.0.0", "1.1.0"},
		{"0.0.0", "1.1.1"},

		{"0.0.1", "0.0.1"},
		{"0.0.1", "0.1.0"},
		{"0.0.1", "0.1.1"},
		{"0.0.1", "1.0.0"},
		{"0.0.1", "1.0.1"},
		{"0.0.1", "1.1.0"},
		{"0.0.1", "1.1.1"},

		{"0.1.0", "0.1.0"},
		{"0.1.0", "0.1.1"},
		{"0.1.0", "1.0.0"},
		{"0.1.0", "1.0.1"},
		{"0.1.0", "1.1.0"},
		{"0.1.0", "1.1.1"},

		{"0.1.1", "0.1.1"},
		{"0.1.1", "1.0.0"},
		{"0.1.1", "1.0.1"},
		{"0.1.1", "1.1.0"},
		{"0.1.1", "1.1.1"},

		{"1.0.0", "1.0.0"},
		{"1.0.0", "1.0.1"},
		{"1.0.0", "1.1.0"},
		{"1.0.0", "1.1.1"},

		{"1.0.1", "1.0.1"},
		{"1.0.1", "1.1.0"},
		{"1.0.1", "1.1.1"},

		{"1.1.0", "1.1.0"},
		{"1.1.0", "1.1.1"},

		{"1.1.1", "1.1.1"},

		{"1.1.1.rc0", "1.1.1.rc0"},
		{"1.1.1.rc0", "1.1.1"},
		{"1.1.0", "1.1.1.rc0"},

		{"1.1.GIT", "1.1.1"},
		{"1.0.0", "1.1.GIT"},

		{"1.1.1", "1.1.1.gl1"},
		{"1.1.1.gl0", "1.1.1.gl1"},
		{"1.1.1.gl1", "1.1.1.gl2"},
		{"1.1.1.gl1", "1.1.2"},
	} {
		t.Run(fmt.Sprintf("%s < %s", tc.smaller, tc.larger), func(t *testing.T) {
			smaller, err := parseVersion(tc.smaller)
			require.NoError(t, err)

			larger, err := parseVersion(tc.larger)
			require.NoError(t, err)

			if tc.smaller == tc.larger {
				require.False(t, smaller.LessThan(larger))
				require.False(t, larger.LessThan(smaller))
			} else {
				require.True(t, smaller.LessThan(larger))
				require.False(t, larger.LessThan(smaller))
			}
		})
	}

	t.Run("1.1.GIT == 1.1.0", func(t *testing.T) {
		first, err := parseVersion("1.1.GIT")
		require.NoError(t, err)

		second, err := parseVersion("1.1.0")
		require.NoError(t, err)

		// This is a special case: "GIT" is treated the same as "0".
		require.False(t, first.LessThan(second))
		require.False(t, second.LessThan(first))
	})
}

func TestVersion_IsSupported(t *testing.T) {
	for _, tc := range []struct {
		version string
		expect  bool
	}{
		{"2.20.0", false},
		{"2.24.0-rc0", false},
		{"2.24.0", false},
		{"2.25.0", false},
		{"2.32.0", false},
		{"2.38.0-rc0", false},
		{"2.39.0", false},
		{"2.39.0.gl0", false},
		{"2.39.0.gl3", false},
		{"2.40.0", false},
		{"2.40.0.gl1", false},
		{"2.40.1", false},
		{"2.41.0", true},
		{"3.0.0", true},
		{"3.0.0.gl5", true},
	} {
		t.Run(tc.version, func(t *testing.T) {
			version, err := parseVersion(tc.version)
			require.NoError(t, err)
			require.Equal(t, tc.expect, version.IsSupported())
		})
	}
}

func TestVersion_GeometricRepackingSupportsAlternates(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		version string
		expect  bool
	}{
		{"1.0.0", false},
		{"2.39.2", false},
		{"2.40.0", false},
		{"2.40.1", false},
		{"2.40.0.gl1", true},
		{"2.40.0.gl2", true},
		{"2.40.1.gl1", true},
		{"2.40.1.gl2", true},
		{"2.41.0", true},
		{"3.0.0", true},
	} {
		t.Run(tc.version, func(t *testing.T) {
			version, err := parseVersion(tc.version)
			require.NoError(t, err)
			require.Equal(t, tc.expect,
				version.GeometricRepackingSupportsAlternates())
		})
	}
}

func TestVersion_CatfileSupportsNulTerminatedOutput(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		version string
		expect  bool
	}{
		{"1.0.0", false},
		{"2.40.2", false},
		{"2.41.0", false},
		{"2.41.1", false},
		{"2.41.0.gl1", true},
		{"2.41.0.gl2", true},
		{"2.41.1.gl1", true},
		{"2.41.1.gl2", true},
		{"2.42.0", true},
		{"3.0.0", true},
	} {
		t.Run(tc.version, func(t *testing.T) {
			version, err := parseVersion(tc.version)
			require.NoError(t, err)
			require.Equal(t, tc.expect,
				version.CatfileSupportsNulTerminatedOutput())
		})
	}
}
