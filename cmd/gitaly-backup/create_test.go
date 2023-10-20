package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"io"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v16/internal/backup"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/service/setup"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper/testserver"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
)

func TestCreateSubcommand(t *testing.T) {
	tests := []struct {
		Name               string
		Flags              func(backupRoot string) []string
		ServerOpts         func(ctx context.Context, backupRoot string) []testserver.GitalyServerOpt
		ExpectedErrMessage string
	}{
		{
			Name: "when a local backup is created",
			Flags: func(backupRoot string) []string {
				return []string{"-path", backupRoot, "-id", "the-new-backup"}
			},
			ServerOpts: func(ctx context.Context, backupRoot string) []testserver.GitalyServerOpt {
				return nil
			},
			ExpectedErrMessage: "create: pipeline: 1 failures encountered:\n - invalid: manager: could not dial source: invalid connection string: \"invalid\"\n",
		},
		{
			Name: "when a server-side backup is created",
			Flags: func(path string) []string {
				return []string{"-server-side", "-id", "the-new-backup"}
			},
			ServerOpts: func(ctx context.Context, backupRoot string) []testserver.GitalyServerOpt {
				backupSink, err := backup.ResolveSink(ctx, backupRoot)
				require.NoError(t, err)

				backupLocator, err := backup.ResolveLocator("pointer", backupSink)
				require.NoError(t, err)

				return []testserver.GitalyServerOpt{
					testserver.WithBackupSink(backupSink),
					testserver.WithBackupLocator(backupLocator),
				}
			},
			ExpectedErrMessage: "create: pipeline: 1 failures encountered:\n - invalid: server-side create: could not dial source: invalid connection string: \"invalid\"\n",
		},
	}

	for _, tc := range tests {
		t.Run(tc.Name, func(t *testing.T) {
			ctx := testhelper.Context(t)
			path := testhelper.TempDir(t)

			cfg := testcfg.Build(t)
			cfg.SocketPath = testserver.RunGitalyServer(t, cfg, setup.RegisterAll, tc.ServerOpts(ctx, path)...)

			var repos []*gitalypb.Repository
			for i := 0; i < 5; i++ {
				repo, repoPath := gittest.CreateRepository(t, ctx, cfg)
				gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch(git.DefaultBranch))
				repos = append(repos, repo)
			}

			var stdin bytes.Buffer
			encoder := json.NewEncoder(&stdin)

			for _, repo := range repos {
				require.NoError(t, encoder.Encode(map[string]string{
					"address":         cfg.SocketPath,
					"token":           cfg.Auth.Token,
					"storage_name":    repo.StorageName,
					"relative_path":   repo.RelativePath,
					"gl_project_path": repo.GlProjectPath,
				}))
			}

			require.NoError(t, encoder.Encode(map[string]string{
				"address":       "invalid",
				"token":         "invalid",
				"relative_path": "invalid",
			}))

			cmd := createSubcommand{}
			fs := flag.NewFlagSet("create", flag.ContinueOnError)
			cmd.Flags(fs)
			require.NoError(t, fs.Parse(tc.Flags(path)))

			require.EqualError(t,
				cmd.Run(ctx, testhelper.SharedLogger(t), &stdin, io.Discard),
				tc.ExpectedErrMessage)

			for _, repo := range repos {
				bundlePath := filepath.Join(path, strings.TrimSuffix(repo.RelativePath, ".git"), "the-new-backup", "001.bundle")
				require.FileExists(t, bundlePath)
			}
		})
	}
}
