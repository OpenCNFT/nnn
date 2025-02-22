package localrepo_test

import (
	"bytes"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/service"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/service/hook"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/service/repository"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/service/ssh"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper/testserver"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
	"google.golang.org/grpc"
)

func TestRepo_FetchInternal(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg := testcfg.Build(t)
	protocolDetectingFactory := gittest.NewProtocolDetectingCommandFactory(t, ctx, cfg)

	cfg.SocketPath = testserver.RunGitalyServer(t, cfg, func(srv *grpc.Server, deps *service.Dependencies) {
		gitalypb.RegisterSSHServiceServer(srv, ssh.NewServer(deps))
		gitalypb.RegisterHookServiceServer(srv, hook.NewServer(deps))
		gitalypb.RegisterRepositoryServiceServer(srv, repository.NewServer(deps))
	}, testserver.WithGitCommandFactory(protocolDetectingFactory))

	testcfg.BuildGitalySSH(t, cfg)
	testcfg.BuildGitalyHooks(t, cfg)

	remoteRepoProto, remoteRepoPath := gittest.CreateRepository(t, ctx, cfg)
	remoteOID := gittest.WriteCommit(t, cfg, remoteRepoPath, gittest.WithBranch("master"))
	tagV100OID := gittest.WriteTag(t, cfg, remoteRepoPath, "v1.0.0", remoteOID.Revision(), gittest.WriteTagConfig{
		Message: "v1.0.0",
	})
	tagV110OID := gittest.WriteTag(t, cfg, remoteRepoPath, "v1.1.0", remoteOID.Revision(), gittest.WriteTagConfig{
		Message: "v1.1.0",
	})

	t.Run("refspec with tag", func(t *testing.T) {
		ctx := testhelper.MergeIncomingMetadata(ctx, testcfg.GitalyServersMetadataFromCfg(t, cfg))

		repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg)
		repo := localrepo.NewTestRepo(t, cfg, repoProto)

		gittest.Exec(t, cfg, "-C", repoPath, "config", "fetch.writeCommitGraph", "true")

		require.NoError(t, repo.FetchInternal(
			ctx, remoteRepoProto, []string{"refs/heads/master:refs/heads/master"},
			localrepo.FetchOpts{},
		))

		refs, err := repo.GetReferences(ctx)
		require.NoError(t, err)
		require.Equal(t, []git.Reference{
			{Name: "refs/heads/master", Target: remoteOID.String()},
			{Name: "refs/tags/v1.0.0", Target: tagV100OID.String()},
			{Name: "refs/tags/v1.1.0", Target: tagV110OID.String()},
		}, refs)

		// Even if the gitconfig says we should write a commit graph, Gitaly should refuse
		// to do so.
		require.NoFileExists(t, filepath.Join(repoPath, "objects/info/commit-graph"))
		require.NoDirExists(t, filepath.Join(repoPath, "objects/info/commit-graphs"))

		// Assert that we're using the expected Git protocol version, which is protocol v2.
		require.Equal(t, "GIT_PROTOCOL=version=2\n", protocolDetectingFactory.ReadProtocol(t))

		require.NoFileExists(t, filepath.Join(repoPath, "FETCH_HEAD"))
	})

	t.Run("refspec without tags", func(t *testing.T) {
		ctx := testhelper.MergeIncomingMetadata(ctx, testcfg.GitalyServersMetadataFromCfg(t, cfg))

		repoProto, _ := gittest.CreateRepository(t, ctx, cfg)
		repo := localrepo.NewTestRepo(t, cfg, repoProto)

		require.NoError(t, repo.FetchInternal(
			ctx, remoteRepoProto, []string{"refs/heads/master:refs/heads/master"},
			localrepo.FetchOpts{Tags: localrepo.FetchOptsTagsNone},
		))

		refs, err := repo.GetReferences(ctx)
		require.NoError(t, err)
		require.Equal(t, []git.Reference{
			{Name: "refs/heads/master", Target: remoteOID.String()},
		}, refs)
	})

	t.Run("object ID", func(t *testing.T) {
		ctx := testhelper.MergeIncomingMetadata(ctx, testcfg.GitalyServersMetadataFromCfg(t, cfg))

		repoProto, _ := gittest.CreateRepository(t, ctx, cfg)
		repo := localrepo.NewTestRepo(t, cfg, repoProto)

		require.NoError(t, repo.FetchInternal(
			ctx, remoteRepoProto, []string{remoteOID.String()},
			localrepo.FetchOpts{},
		))

		exists, err := repo.HasRevision(ctx, remoteOID.Revision()+"^{commit}")
		require.NoError(t, err, "the object from remote should exists in local after fetch done")
		require.True(t, exists)
	})

	t.Run("nonexistent revision", func(t *testing.T) {
		ctx := testhelper.MergeIncomingMetadata(ctx, testcfg.GitalyServersMetadataFromCfg(t, cfg))

		repoProto, _ := gittest.CreateRepository(t, ctx, cfg)
		repo := localrepo.NewTestRepo(t, cfg, repoProto)

		var stderr bytes.Buffer
		err := repo.FetchInternal(
			ctx, remoteRepoProto, []string{"refs/does/not/exist"},
			localrepo.FetchOpts{Stderr: &stderr},
		)
		require.EqualError(t, err, "exit status 128")
		require.IsType(t, err, localrepo.FetchFailedError{})

		// When git-fetch(1) is spawned, it is configured to use the gitaly-ssh binary. Instead of
		// an SSH connection, a sidechannel connection is created to transmit git data by using a
		// multiplexed yamux session. In scenarios where the server hangs up a yamux connection and
		// a client continues to write to the closed connection, an info log is written to stderr.
		// This means there is a race where both git and yamux can write to stderr. Because this
		// race is inconsequential, the test assertion only validates that stderr contains the
		// expected git error.
		require.Contains(t, stderr.String(), "fatal: couldn't find remote ref refs/does/not/exist\n")
	})

	t.Run("with env", func(t *testing.T) {
		ctx := testhelper.MergeIncomingMetadata(ctx, testcfg.GitalyServersMetadataFromCfg(t, cfg))

		repoProto, _ := gittest.CreateRepository(t, ctx, cfg)
		repo := localrepo.NewTestRepo(t, cfg, repoProto)

		var stderr bytes.Buffer
		err := repo.FetchInternal(
			ctx, remoteRepoProto, []string{"refs/heads/master"},
			localrepo.FetchOpts{Stderr: &stderr, Env: []string{"GIT_TRACE=1"}},
		)
		require.NoError(t, err)
		require.Contains(t, stderr.String(), "trace: built-in: git fetch --no-write-fetch-head --quiet --atomic --end-of-options")
	})

	t.Run("with disabled transactions", func(t *testing.T) {
		ctx := testhelper.MergeIncomingMetadata(ctx, testcfg.GitalyServersMetadataFromCfg(t, cfg))

		repoProto, _ := gittest.CreateRepository(t, ctx, cfg)
		repo := localrepo.NewTestRepo(t, cfg, repoProto)

		var stderr bytes.Buffer
		err := repo.FetchInternal(
			ctx, remoteRepoProto, []string{"refs/heads/master"},
			localrepo.FetchOpts{Stderr: &stderr, Env: []string{"GIT_TRACE=1"}, DisableTransactions: true},
		)
		require.NoError(t, err)
		require.Contains(t, stderr.String(), "trace: built-in: git fetch --no-write-fetch-head --quiet --end-of-options")
	})

	t.Run("invalid remote repo", func(t *testing.T) {
		ctx := testhelper.MergeIncomingMetadata(ctx, testcfg.GitalyServersMetadataFromCfg(t, cfg))

		repoProto, _ := gittest.CreateRepository(t, ctx, cfg)
		repo := localrepo.NewTestRepo(t, cfg, repoProto)

		err := repo.FetchInternal(ctx, &gitalypb.Repository{
			RelativePath: "does/not/exist",
			StorageName:  cfg.Storages[0].Name,
		}, []string{"refs/does/not/exist"}, localrepo.FetchOpts{})
		require.Error(t, err)
		require.IsType(t, err, localrepo.FetchFailedError{})
		require.Contains(t, err.Error(), storage.ErrRepositoryNotFound.Error())
	})

	t.Run("pruning", func(t *testing.T) {
		ctx := testhelper.MergeIncomingMetadata(ctx, testcfg.GitalyServersMetadataFromCfg(t, cfg))

		repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg)
		repo := localrepo.NewTestRepo(t, cfg, repoProto)

		// Create a local reference. Given that it doesn't exist on the remote side, it
		// would get pruned if we pass `--prune`.
		gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("prune-me"))

		// By default, refs are not pruned.
		require.NoError(t, repo.FetchInternal(
			ctx, remoteRepoProto, []string{"refs/*:refs/*"}, localrepo.FetchOpts{},
		))

		exists, err := repo.HasRevision(ctx, "refs/heads/prune-me")
		require.NoError(t, err)
		require.True(t, exists)

		// But they are pruned if we pass the `WithPrune()` option.
		require.NoError(t, repo.FetchInternal(
			ctx, remoteRepoProto, []string{"refs/*:refs/*"}, localrepo.FetchOpts{Prune: true},
		))

		exists, err = repo.HasRevision(ctx, "refs/heads/prune-me")
		require.NoError(t, err)
		require.False(t, exists)
	})
}
