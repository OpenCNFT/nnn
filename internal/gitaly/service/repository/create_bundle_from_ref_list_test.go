package repository

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/housekeeping"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/mode"
	"gitlab.com/gitlab-org/gitaly/v16/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/v16/streamio"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestCreateBundleFromRefList_success(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg, client := setupRepositoryService(t)

	// Create a work tree with a HEAD pointing to a commit that is missing. CreateBundle should
	// clean this up before creating the bundle.
	repo, repoPath := gittest.CreateRepository(t, ctx, cfg)
	masterOID := gittest.WriteCommit(t, cfg, repoPath, gittest.WithMessage("master"), gittest.WithBranch("master"))
	sha := gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("branch"))

	require.NoError(t, os.MkdirAll(filepath.Join(repoPath, housekeeping.GitlabWorktreePrefix), mode.Directory))

	gittest.Exec(t, cfg, "-C", repoPath, "worktree", "add", filepath.Join(housekeeping.GitlabWorktreePrefix, "worktree1"), sha.String())
	require.NoError(t, os.Chtimes(filepath.Join(repoPath, housekeeping.GitlabWorktreePrefix, "worktree1"), time.Now().Add(-7*time.Hour), time.Now().Add(-7*time.Hour)))

	gittest.Exec(t, cfg, "-C", repoPath, "branch", "-D", "branch")
	require.NoError(t, os.Remove(filepath.Join(repoPath, "objects", sha.String()[0:2], sha.String()[2:])))

	c, err := client.CreateBundleFromRefList(ctx)
	require.NoError(t, err)

	// Send only the repo first and get the acknowledgement
	require.NoError(t, c.Send(&gitalypb.CreateBundleFromRefListRequest{
		Repository: repo,
	}))
	response, err := c.Recv()
	require.NoError(t, err)
	require.Empty(t, response.Data) // First response is only an acknowledgement without any data

	require.NoError(t, c.Send(&gitalypb.CreateBundleFromRefListRequest{
		Patterns: [][]byte{
			[]byte("master"),
			[]byte("^master~1"),
		},
	}))
	require.NoError(t, c.CloseSend())

	reader := streamio.NewReader(func() ([]byte, error) {
		response, err := c.Recv()
		return response.GetData(), err
	})

	bundle, err := os.Create(filepath.Join(testhelper.TempDir(t), "bundle"))
	require.NoError(t, err)

	_, err = io.Copy(bundle, reader)
	require.NoError(t, err)

	require.NoError(t, bundle.Close())

	output := gittest.Exec(t, cfg, "-C", repoPath, "bundle", "verify", bundle.Name())

	require.Contains(t, string(output), fmt.Sprintf("The bundle contains this ref:\n%s refs/heads/master", masterOID))
}

func TestCreateBundleFromRefList_missing_ref(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg, client := setupRepositoryService(t)

	repo, repoPath := gittest.CreateRepository(t, ctx, cfg)
	commitOID := gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("master"))

	c, err := client.CreateBundleFromRefList(ctx)
	require.NoError(t, err)

	// Send only the repo first and get the acknowledgement
	require.NoError(t, c.Send(&gitalypb.CreateBundleFromRefListRequest{
		Repository: repo,
	}))
	response, err := c.Recv()
	require.NoError(t, err)
	require.Empty(t, response.Data) // First response is only an acknowledgement without any data

	require.NoError(t, c.Send(&gitalypb.CreateBundleFromRefListRequest{
		Patterns: [][]byte{
			[]byte("refs/heads/master"),
			[]byte("refs/heads/totally_missing"),
		},
	}))
	require.NoError(t, c.CloseSend())

	reader := streamio.NewReader(func() ([]byte, error) {
		response, err := c.Recv()
		return response.GetData(), err
	})

	bundle, err := os.Create(filepath.Join(testhelper.TempDir(t), "bundle"))
	require.NoError(t, err)

	_, err = io.Copy(bundle, reader)
	require.NoError(t, err)

	require.NoError(t, bundle.Close())

	output := gittest.Exec(t, cfg, "-C", repoPath, "bundle", "verify", bundle.Name())

	require.Contains(t, string(output), fmt.Sprintf("The bundle contains this ref:\n%s refs/heads/master", commitOID))
}

func TestCreateBundleFromRefList_PatternsValidation(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg, client := setupRepositoryService(t)
	repo, _ := gittest.CreateRepository(t, ctx, cfg)

	stream, err := client.CreateBundleFromRefList(ctx)
	require.NoError(t, err)

	require.NoError(t, stream.Send(&gitalypb.CreateBundleFromRefListRequest{
		Repository: repo,
		Patterns:   [][]byte{[]byte("master")},
	}))
	require.NoError(t, stream.CloseSend())
	_, err = stream.Recv()
	testhelper.RequireGrpcError(t, structerr.NewInvalidArgument("patterns should not be set for the first request in the stream"), err)
}

func TestCreateBundleFromRefList_RepositoryValidation(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	_, client := setupRepositoryService(t)

	stream, err := client.CreateBundleFromRefList(ctx)
	require.NoError(t, err)

	require.NoError(t, stream.Send(&gitalypb.CreateBundleFromRefListRequest{}))
	require.NoError(t, stream.CloseSend())
	_, err = stream.Recv()
	testhelper.RequireGrpcError(t, structerr.NewInvalidArgument("%w", storage.ErrRepositoryNotSet), err)
}

func TestCreateBundleFromRefList_BundleValidation(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg, client := setupRepositoryService(t)
	repo, _ := gittest.CreateRepository(t, ctx, cfg)

	stream, err := client.CreateBundleFromRefList(ctx)
	require.NoError(t, err)

	// Send only the repo first and get the acknowledgement
	require.NoError(t, stream.Send(&gitalypb.CreateBundleFromRefListRequest{
		Repository: repo,
	}))
	response, err := stream.Recv()
	require.NoError(t, err)
	require.Empty(t, response.Data) // First response is only an acknowledgement without any data

	require.NoError(t, stream.Send(&gitalypb.CreateBundleFromRefListRequest{
		Patterns: [][]byte{[]byte("master"), []byte("^master")},
	}))

	require.NoError(t, stream.CloseSend())
	for _, err = stream.Recv(); err == nil; _, err = stream.Recv() {
	}
	testhelper.RequireGrpcError(t, status.Error(codes.FailedPrecondition, "create bundle: refusing to create empty bundle"), err)
}
