//go:build linux

package cgroups

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/config/cgroups"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper"
)

// cmdArgs are Command arguments used processes to be added to a cgroup.
var cmdArgs = []string{"ls", "-hal", "."}

func TestMain(m *testing.M) {
	testhelper.Run(m)
}

// Contains is a generic function that helps check if an element exists in an array of the same type
func Contains[T comparable](s []T, e T) bool {
	for _, v := range s {
		if v == e {
			return true
		}
	}
	return false
}

// getRepoNumber finds the groupId from the given path
func getRepoNumber(path string) (string, error) {
	// Find the last occurrence of "repos-" in the path
	lastIndex := strings.LastIndex(path, "repos-")
	if lastIndex == -1 {
		return "", fmt.Errorf("no 'repos-' found in path %s", path)
	}

	// Extract the part after "repos-" using the last index
	repoNumber := path[lastIndex+len("repos-"):]

	return repoNumber, nil
}

// findDirMatchingRegex filters for directories that match the input regex
func findDirMatchingRegex(re *regexp.Regexp, dir string) ([]string, error) {
	files := []string{}

	walk := func(fn string, fi os.FileInfo, err error) error {
		if re.MatchString(fn) == false {
			return nil
		}
		if fi.IsDir() {
			fmt.Println(fn + string(os.PathSeparator))
			files = append(files, fn)
		}
		return nil
	}
	if err := filepath.Walk(dir, walk); err != nil {
		return nil, fmt.Errorf("regex dir walk: %w", err)
	}
	fmt.Printf("Found %[1]d files.\n", len(files))
	return files, nil
}

// setupMockCgroup configures some defaults for the mocks
func setupMockCgroup(t *testing.T, version int) (mockCgroup, cgroups.Config) {
	mock := newMock(t, version)
	config := defaultCgroupsConfig()
	config.Repositories.Count = 10
	config.Repositories.MemoryBytes = 1024
	config.Repositories.CPUShares = 16
	config.HierarchyRoot = "gitaly"
	config.Mountpoint = mock.rootPath()
	return mock, config
}

// pidExistsInCgroupProcs validates that an expected process id has been added to the cgroup.procs file
func pidExistsInCgroupProcs(t *testing.T, mock mockCgroup, pid int, cmdProcessID int, groupID []uint) {
	for _, shard := range groupID {
		for _, path := range mock.repoPaths(pid, shard) {
			procsPath := filepath.Join(path, "cgroup.procs")
			content := string(readCgroupFile(t, procsPath))

			// Verify the default cmd triggered from Setup() also got added
			if shard == 0 {
				require.NotEmpty(t, content, fmt.Sprintf("Path to procs: %s had empty contents", procsPath))
			} else {
				if content != "" {
					cmdPid, err := strconv.Atoi(content)
					require.NoError(t, err, "Could not parse content's PID to an int")

					require.Equal(t, cmdProcessID, cmdPid,
						fmt.Sprintf("Process id %d should be added to shard %d's cgroup.procs. Content has procs: %d", cmdProcessID, shard, cmdPid))
				}
			}
		}
	}
}
