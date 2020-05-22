package repository

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/git/stats"
	"gitlab.com/gitlab-org/gitaly/internal/git/updateref"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
)

func getNewestPackfileModtime(t *testing.T, repoPath string) time.Time {
	packFiles, err := filepath.Glob(filepath.Join(repoPath, "objects", "pack", "*.pack"))
	require.NoError(t, err)
	if len(packFiles) == 0 {
		t.Error("no packfiles exist")
	}

	var newestPackfileModtime time.Time

	for _, packFile := range packFiles {
		info, err := os.Stat(packFile)
		require.NoError(t, err)
		if info.ModTime().After(newestPackfileModtime) {
			newestPackfileModtime = info.ModTime()
		}
	}

	return newestPackfileModtime
}

func TestOptimizeRepository(t *testing.T) {
	testRepo, testRepoPath, cleanup := testhelper.NewTestRepo(t)
	defer cleanup()

	testhelper.MustRunCommand(t, nil, "git", "-C", testRepoPath, "repack", "-A", "-b")

	ctx, cancel := testhelper.Context()
	defer cancel()

	hasBitmap, err := stats.HasBitmap(testRepo)
	require.NoError(t, err)
	require.True(t, hasBitmap, "expect a bitmap since we just repacked with -b")

	// get timestamp of latest packfile
	newestsPackfileTime := getNewestPackfileModtime(t, testRepoPath)

	testhelper.CreateCommit(t, testRepoPath, "master", nil)

	serverSocketPath, stop := runRepoServer(t)
	defer stop()

	repoClient, conn := NewRepositoryClient(t, serverSocketPath)
	defer conn.Close()

	_, err = repoClient.OptimizeRepository(ctx, &gitalypb.OptimizeRepositoryRequest{Repository: testRepo})
	require.NoError(t, err)

	require.Equal(t, getNewestPackfileModtime(t, testRepoPath), newestsPackfileTime, "there should not have been a new packfile created")

	testRepo, testRepoPath, cleanupBare := testhelper.InitBareRepo(t)
	defer cleanupBare()

	blobs := 10
	blobIDs := testhelper.WriteBlobs(t, testRepoPath, blobs)

	updater, err := updateref.New(ctx, testRepo)
	require.NoError(t, err)

	for _, blobID := range blobIDs {
		commitID := testhelper.CommitBlobWithName(t, testRepoPath, blobID, blobID, "adding another blob....")
		require.NoError(t, updater.Create("refs/heads/"+blobID, commitID))
	}

	require.NoError(t, updater.Wait())

	bitmaps, err := filepath.Glob(filepath.Join(testRepoPath, "objects", "pack", "*.bitmap"))
	require.NoError(t, err)
	require.Empty(t, bitmaps)

	mrRefs := filepath.Join(testRepoPath, "refs/merge-requests")
	emptyRef := filepath.Join(mrRefs, "1")
	require.NoError(t, os.MkdirAll(emptyRef, 0755))
	require.DirExists(t, emptyRef, "sanity check for empty ref dir existence")

	// optimize repository on a repository without a bitmap should call repack full
	_, err = repoClient.OptimizeRepository(ctx, &gitalypb.OptimizeRepositoryRequest{Repository: testRepo})
	require.NoError(t, err)

	bitmaps, err = filepath.Glob(filepath.Join(testRepoPath, "objects", "pack", "*.bitmap"))
	require.NoError(t, err)
	require.NotEmpty(t, bitmaps)

	// All empty directories should be removed
	testhelper.AssertPathNotExists(t, emptyRef)
	testhelper.AssertPathNotExists(t, mrRefs)
	require.FileExists(t,
		filepath.Join(testRepoPath, "refs/heads", blobIDs[0]),
		"unpacked refs should never be removed",
	)
}

func TestOptimizeRepositoryValidation(t *testing.T) {
	testRepo, _, cleanup := testhelper.NewTestRepo(t)
	defer cleanup()

	testCases := []struct {
		desc              string
		repo              *gitalypb.Repository
		expectedErrorCode codes.Code
	}{
		{
			desc:              "empty repository",
			repo:              nil,
			expectedErrorCode: codes.InvalidArgument,
		},
		{
			desc:              "invalid repository storage",
			repo:              &gitalypb.Repository{StorageName: "non-existent", RelativePath: testRepo.GetRelativePath()},
			expectedErrorCode: codes.InvalidArgument,
		},
		{
			desc:              "invalid repository path",
			repo:              &gitalypb.Repository{StorageName: testRepo.GetStorageName(), RelativePath: "/path/not/exist"},
			expectedErrorCode: codes.NotFound,
		},
	}

	serverSocketPath, stop := runRepoServer(t)
	defer stop()

	repoClient, conn := NewRepositoryClient(t, serverSocketPath)
	defer conn.Close()

	ctx, cancel := testhelper.Context()
	defer cancel()

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			_, err := repoClient.OptimizeRepository(ctx, &gitalypb.OptimizeRepositoryRequest{Repository: tc.repo})
			require.Error(t, err)
			testhelper.RequireGrpcError(t, err, tc.expectedErrorCode)
		})
	}

	_, err := repoClient.OptimizeRepository(ctx, &gitalypb.OptimizeRepositoryRequest{Repository: testRepo})
	require.NoError(t, err)
}
