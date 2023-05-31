package ref

import (
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestSuccessfulFindBranchRequest(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg, client := setupRefServiceWithoutRepo(t)

	repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg)
	repo := localrepo.NewTestRepo(t, cfg, repoProto)

	branchesByName := make(map[git.ReferenceName]*gitalypb.Branch)
	for _, branchName := range []string{
		"refs/heads/branch",
		"refs/heads/heads/branch",
		"refs/heads/refs/heads/branch",
	} {
		oid := gittest.WriteCommit(t, cfg, repoPath, gittest.WithReference(branchName), gittest.WithMessage(branchName))

		commit, err := repo.ReadCommit(ctx, oid.Revision())
		require.NoError(t, err)

		branchesByName[git.ReferenceName(branchName)] = &gitalypb.Branch{
			Name:         []byte(branchName[len("refs/heads/"):]),
			TargetCommit: commit,
		}
	}

	testCases := []struct {
		desc           string
		branchName     string
		expectedBranch *gitalypb.Branch
	}{
		{
			desc:           "regular branch name",
			branchName:     "branch",
			expectedBranch: branchesByName["refs/heads/branch"],
		},
		{
			desc:           "absolute reference path",
			branchName:     "heads/branch",
			expectedBranch: branchesByName["refs/heads/heads/branch"],
		},
		{
			desc:           "heads path",
			branchName:     "refs/heads/branch",
			expectedBranch: branchesByName["refs/heads/refs/heads/branch"],
		},
		{
			desc:       "non-existent branch",
			branchName: "i-do-not-exist-on-this-repo",
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.desc, func(t *testing.T) {
			request := &gitalypb.FindBranchRequest{
				Repository: repoProto,
				Name:       []byte(testCase.branchName),
			}

			response, err := client.FindBranch(ctx, request)

			require.NoError(t, err)
			require.Equal(t, testCase.expectedBranch, response.Branch, "mismatched branches")
		})
	}
}

func TestFailedFindBranchRequest(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg, client := setupRefServiceWithoutRepo(t)
	repo, _ := gittest.CreateRepository(t, ctx, cfg)

	testCases := []struct {
		desc        string
		repo        *gitalypb.Repository
		branchName  string
		expectedErr error
	}{
		{
			desc: "no repository provided",
			repo: nil,
			expectedErr: status.Error(codes.InvalidArgument, testhelper.GitalyOrPraefect(
				"empty Repository",
				"repo scoped: empty Repository",
			)),
		},
		{
			desc:        "empty branch name",
			repo:        repo,
			branchName:  "",
			expectedErr: status.Error(codes.InvalidArgument, "Branch name cannot be empty"),
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.desc, func(t *testing.T) {
			request := &gitalypb.FindBranchRequest{
				Repository: testCase.repo,
				Name:       []byte(testCase.branchName),
			}

			_, err := client.FindBranch(ctx, request)
			testhelper.RequireGrpcError(t, testCase.expectedErr, err)
		})
	}
}
