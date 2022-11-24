//go:build !gitaly_test_sha256

package repository

import (
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v15/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v15/proto/go/gitalypb"
)

func TestHasLocalBranches_successful(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg, repo, _, client := setupRepositoryService(t, ctx)

	emptyRepo, _ := gittest.CreateRepository(t, ctx, cfg)

	for _, tc := range []struct {
		desc             string
		request          *gitalypb.HasLocalBranchesRequest
		expectedResponse *gitalypb.HasLocalBranchesResponse
	}{
		{
			desc: "repository has branches",
			request: &gitalypb.HasLocalBranchesRequest{
				Repository: repo,
			},
			expectedResponse: &gitalypb.HasLocalBranchesResponse{
				Value: true,
			},
		},
		{
			desc: "repository doesn't have branches",
			request: &gitalypb.HasLocalBranchesRequest{
				Repository: emptyRepo,
			},
			expectedResponse: &gitalypb.HasLocalBranchesResponse{
				Value: false,
			},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			response, err := client.HasLocalBranches(ctx, tc.request)
			require.NoError(t, err)
			testhelper.ProtoEqual(t, tc.expectedResponse, response)
		})
	}
}

func TestHasLocalBranches_failure(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	_, client := setupRepositoryServiceWithoutRepo(t)

	for _, tc := range []struct {
		desc        string
		repository  *gitalypb.Repository
		expectedErr error
	}{
		{
			desc:       "repository nil",
			repository: nil,
			expectedErr: helper.ErrInvalidArgumentf(testhelper.GitalyOrPraefectMessage(
				"empty Repository",
				"repo scoped: empty Repository",
			)),
		},
		{
			desc: "repository doesn't exist",
			repository: &gitalypb.Repository{
				StorageName:  "fake",
				RelativePath: "path",
			},
			expectedErr: helper.ErrInvalidArgumentf(testhelper.GitalyOrPraefectMessage(
				`GetStorageByName: no such storage: "fake"`,
				"repo scoped: invalid Repository",
			)),
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			request := &gitalypb.HasLocalBranchesRequest{Repository: tc.repository}
			_, err := client.HasLocalBranches(ctx, request)
			testhelper.RequireGrpcError(t, tc.expectedErr, err)
		})
	}
}
