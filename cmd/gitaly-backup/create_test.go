package main

import (
	"bytes"
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
	cfg := testcfg.Build(t)

	cfg.SocketPath = testserver.RunGitalyServer(t, cfg, setup.RegisterAll)

	ctx := testhelper.Context(t)
	path := testhelper.TempDir(t)

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

	cmd := createSubcommand{backupPath: path}

	fs := flag.NewFlagSet("create", flag.ContinueOnError)
	cmd.Flags(fs)

	require.NoError(t, fs.Parse([]string{"-path", path, "-id", "the-new-backup"}))
	require.EqualError(t,
		cmd.Run(ctx, testhelper.NewDiscardingLogger(t), &stdin, io.Discard),
		"create: pipeline: 1 failures encountered:\n - invalid: manager: could not dial source: invalid connection string: \"invalid\"\n")

	for _, repo := range repos {
		bundlePath := filepath.Join(path, strings.TrimSuffix(repo.RelativePath, ".git"), "the-new-backup", "001.bundle")
		require.FileExists(t, bundlePath)
	}
}

func TestCreateSubcommand_serverSide(t *testing.T) {
	ctx := testhelper.Context(t)

	backupRoot := testhelper.TempDir(t)
	backupSink, err := backup.ResolveSink(ctx, backupRoot)
	require.NoError(t, err)

	backupLocator, err := backup.ResolveLocator("pointer", backupSink)
	require.NoError(t, err)

	cfg := testcfg.Build(t)
	cfg.SocketPath = testserver.RunGitalyServer(t, cfg, setup.RegisterAll,
		testserver.WithBackupSink(backupSink),
		testserver.WithBackupLocator(backupLocator),
	)

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

	require.NoError(t, fs.Parse([]string{"-server-side", "-id", "the-new-backup"}))
	require.EqualError(t,
		cmd.Run(ctx, testhelper.NewDiscardingLogger(t), &stdin, io.Discard),
		"create: pipeline: 1 failures encountered:\n - invalid: server-side create: could not dial source: invalid connection string: \"invalid\"\n")

	for _, repo := range repos {
		bundlePath := filepath.Join(backupRoot, strings.TrimSuffix(repo.RelativePath, ".git"), "the-new-backup", "001.bundle")
		require.FileExists(t, bundlePath)
	}
}
