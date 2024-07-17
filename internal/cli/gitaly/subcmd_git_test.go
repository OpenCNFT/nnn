package gitaly

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/service/setup"
	"gitlab.com/gitlab-org/gitaly/v16/internal/helper/perm"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper/testserver"
)

func TestGitalyGitCommand(t *testing.T) {
	ctx := testhelper.Context(t)
	cfg := testcfg.Build(t)
	dir := testhelper.TempDir(t)
	seedDirWithExecutables := func(t *testing.T, executableNames ...string) {
		for _, executableName := range executableNames {
			require.NoError(t, os.WriteFile(filepath.Join(dir, executableName), nil, perm.PrivateExecutable))
		}
	}

	// Git environments supported by the Git command factory.
	seedDirWithExecutables(t, "gitaly-git-v2.45", "gitaly-git-remote-http-v2.45", "gitaly-git-http-backend-v2.45",
		"gitaly-git-v2.44", "gitaly-git-remote-http-v2.44", "gitaly-git-http-backend-v2.44")
	t.Setenv("GITALY_TESTING_BUNDLED_GIT_PATH", dir)

	// Ensure we're using bundled binaries
	cfg.Git.UseBundledBinaries = true
	cfg.BinDir = dir
	testcfg.BuildGitaly(t, cfg)
	cfg.SocketPath = testserver.RunGitalyServer(t, cfg, setup.RegisterAll)

	repo, repoPath := gittest.CreateRepository(t, ctx, cfg)
	gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("main"))

	cfg.SocketPath = testhelper.GetTemporaryGitalySocketFileName(t)

	cfg.Storages = []config.Storage{
		{
			Name: repo.StorageName,
			Path: filepath.Dir(repoPath),
		},
	}

	configPath := testcfg.WriteTemporaryGitalyConfigFile(t, cfg)

	tests := []struct {
		name           string
		args           []string
		expectedOutput string
		expectedError  string
	}{
		{
			name:           "git status",
			args:           []string{"status"},
			expectedOutput: "On branch main",
		},
		{
			name:          "invalid git command",
			args:          []string{"invalid-command"},
			expectedError: "exit status 1",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cmd := exec.Command(cfg.BinaryPath("gitaly"),
				"git",
				"-c", configPath,
			)
			cmd.Args = append(cmd.Args, tt.args...)
			cmd.Dir = repoPath // Set the working directory to the repository path

			var stdout, stderr bytes.Buffer
			cmd.Stdout = &stdout
			cmd.Stderr = &stderr

			t.Logf("Running command: %v in directory: %s", cmd.Args, cmd.Dir)

			err := cmd.Run()

			t.Logf("Stdout: %s", stdout.String())
			t.Logf("Stderr: %s", stderr.String())

			if tt.expectedError != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tt.expectedError)
			} else {
				require.NoError(t, err)
				require.Contains(t, stdout.String(), tt.expectedOutput)
			}
		})
	}
}
