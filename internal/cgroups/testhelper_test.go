//go:build linux

package cgroups

import (
	"fmt"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/config/cgroups"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper"
)

// cmdArgs are Command arguments used processes to be added to a cgroup.
var cmdArgs = []string{"ls", "-hal", "."}

func TestMain(m *testing.M) {
	testhelper.Run(m)
}

// getRepoNumber finds the groupId from the given path
func getRepoNumber(t *testing.T, path string) string {
	// Find the last occurrence of "repos-" in the path
	lastIndex := strings.LastIndex(path, "repos-")
	assert.NotEqual(t, lastIndex, -1, fmt.Sprintf("no 'repos-' found in path %s", path))

	// Extract the part after "repos-" using the last index
	repoNumber := path[lastIndex+len("repos-"):]

	return repoNumber
}

// setupMockCgroup configures some defaults for the mocks
func setupMockCgroup(t *testing.T, version int) (mockCgroup, cgroups.Config) {
	mock := newMock(t, version)
	config := defaultCgroupsConfig()
	config.Repositories.Count = 10
	config.Repositories.MemoryBytes = 1024
	config.Repositories.CPUShares = 16
	config.Mountpoint = mock.rootPath()
	return mock, config
}

// pidExistsInCgroupProcs validates that an expected process id has been added to the cgroup.procs file
func pidExistsInCgroupProcs(t *testing.T, mock *mockCgroup, pid int, expectedPID int, shard uint, shardFromSetupCmd uint) {
	for _, path := range (*mock).repoPaths(pid, shard) {
		procsPath := filepath.Join(path, "cgroup.procs")
		content := string(readCgroupFile(t, procsPath))

		// Verify the default cmd triggered from Setup() also got added
		if shard == shardFromSetupCmd {
			require.NotEmpty(t, content, fmt.Sprintf("Path to procs: %s had empty contents", procsPath))
		} else {
			if content != "" {
				actualPID, err := strconv.Atoi(content)
				require.NoError(t, err, "Could not parse content's PID to an int")

				require.Equal(t, expectedPID, actualPID,
					fmt.Sprintf("Process id %d should be added to shard %d's cgroup.procs. Content has procs: %d", expectedPID, shard, actualPID))
			}
		}
	}
}
