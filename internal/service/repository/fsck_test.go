package repository

import (
	"os"
	"path"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	pb "gitlab.com/gitlab-org/gitaly-proto/go"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
)

func TestFsckSuccess(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	server, serverSocketPath := runRepoServer(t)
	defer server.Stop()

	client, conn := newRepositoryClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, _, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	c, err := client.Fsck(ctx, &pb.FsckRequest{Repository: testRepo})
	assert.NoError(t, err)
	assert.NotNil(t, c)
	assert.Empty(t, c.GetError())
}

func TestFsckFailureSeverlyBrokenRepo(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	server, serverSocketPath := runRepoServer(t)
	defer server.Stop()

	client, conn := newRepositoryClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, testRepoPath, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	// This makes the repo severly broken so that `git` does not identify it as a
	// proper repo.
	require.NoError(t, os.RemoveAll(path.Join(testRepoPath, "objects")))
	fd, err := os.Create(path.Join(testRepoPath, "objects"))
	require.NoError(t, err)
	require.NoError(t, fd.Close())

	c, err := client.Fsck(ctx, &pb.FsckRequest{Repository: testRepo})
	assert.NoError(t, err)
	assert.NotNil(t, c)
	assert.Contains(t, string(c.GetError()), "could not find repository")
}

func TestFsckFailureSlightlyBrokenRepo(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	server, serverSocketPath := runRepoServer(t)
	defer server.Stop()

	client, conn := newRepositoryClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, testRepoPath, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	// This makes the repo slighly broken so that `git` still identify it as a
	// proper repo, but `fsck` complains about broken refs...
	require.NoError(t, os.RemoveAll(path.Join(testRepoPath, "objects", "pack")))

	c, err := client.Fsck(ctx, &pb.FsckRequest{Repository: testRepo})
	assert.NoError(t, err)
	assert.NotNil(t, c)
	assert.NotEmpty(t, string(c.GetError()))
	assert.Contains(t, string(c.GetError()), "Could not fsck repository")
}
