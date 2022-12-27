//go:build !gitaly_test_sha256

package main

import (
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/updateref"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v15/proto/go/gitalypb"
	"google.golang.org/protobuf/encoding/protojson"
)

const keepAroundNamespace = "refs/keep-around"

func TestVisibilityOfHiddenRefs(t *testing.T) {
	ctx := testhelper.Context(t)

	cfg, repo, repoPath := testcfg.BuildWithRepo(t)
	testcfg.BuildGitalySSH(t, cfg)
	testcfg.BuildGitalyHooks(t, cfg)

	socketPath := testhelper.GetTemporaryGitalySocketFileName(t)

	_, clean := runServer(t, false, cfg, "unix", socketPath)
	defer clean()

	_, clean = runServer(t, false, cfg, "unix", cfg.InternalSocketPath())
	defer clean()

	// Create a keep-around ref
	existingSha := git.ObjectID("1e292f8fedd741b75372e19097c76d327140c312")
	keepAroundRef := fmt.Sprintf("%s/%s", keepAroundNamespace, existingSha)

	localRepo := localrepo.NewTestRepo(t, cfg, repo)
	updater, err := updateref.New(ctx, localRepo)
	defer testhelper.MustClose(t, updater)

	require.NoError(t, err)
	require.NoError(t, updater.Start())
	require.NoError(t, updater.Create(git.ReferenceName(keepAroundRef), existingSha))
	require.NoError(t, updater.Commit())

	git.Exec(t, cfg, "-C", repoPath, "config", "transfer.hideRefs", keepAroundNamespace)

	output := git.Exec(t, cfg, "ls-remote", repoPath, keepAroundNamespace)
	require.Empty(t, output, "there should be no keep-around refs in normal ls-remote output")

	wd, err := os.Getwd()
	require.NoError(t, err)

	tests := []struct {
		name             string
		GitConfigOptions []string
		HiddenRefFound   bool
	}{
		{
			name:             "With no custom GitConfigOptions passed",
			GitConfigOptions: []string{},
			HiddenRefFound:   true,
		},
		{
			name:             "With custom GitConfigOptions passed",
			GitConfigOptions: []string{fmt.Sprintf("transfer.hideRefs=%s", keepAroundRef)},
			HiddenRefFound:   false,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			payload, err := protojson.Marshal(&gitalypb.SSHUploadPackRequest{
				Repository:       repo,
				GitConfigOptions: test.GitConfigOptions,
			})

			require.NoError(t, err)

			stdout := git.ExecOpts(t, cfg, git.ExecConfig{
				Env: []string{
					fmt.Sprintf("GITALY_PAYLOAD=%s", payload),
					fmt.Sprintf("GITALY_ADDRESS=unix:%s", socketPath),
					fmt.Sprintf("GITALY_WD=%s", wd),
					fmt.Sprintf("PATH=.:%s", os.Getenv("PATH")),
					fmt.Sprintf("GIT_SSH_COMMAND=%s upload-pack", cfg.BinaryPath("gitaly-ssh")),
				},
			}, "ls-remote", fmt.Sprintf("%s:%s", "git@localhost", repoPath), keepAroundRef)

			if test.HiddenRefFound {
				require.Equal(t, fmt.Sprintf("%s\t%s\n", existingSha, keepAroundRef), string(stdout))
			} else {
				require.NotEqual(t, fmt.Sprintf("%s\t%s\n", existingSha, keepAroundRef), string(stdout))
			}
		})
	}
}
