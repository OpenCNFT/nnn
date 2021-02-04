package operations

import (
	"context"
	"crypto/sha1"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/git/log"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/internal/metadata/featureflag"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	updateBranchName = "feature"
	newrev           = []byte("1a35b5a77cf6af7edf6703f88e82f6aff613666f")
	oldrev           = []byte("0b4bc9a49b562e85de7cc9e834518ea6828729b9")
)

func TestSuccessfulUserUpdateBranchRequest(t *testing.T) {
	testhelper.NewFeatureSets([]featureflag.FeatureFlag{
		featureflag.ReferenceTransactions,
		featureflag.GoUserUpdateBranch,
	}).Run(t, testSuccessfulUserUpdateBranchRequest)
}

func testSuccessfulUserUpdateBranchRequest(t *testing.T, ctx context.Context) {
	testRepo, testRepoPath, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	serverSocketPath, stop := runOperationServiceServer(t)
	defer stop()

	client, conn := newOperationClient(t, serverSocketPath)
	defer conn.Close()

	testCases := []struct {
		desc             string
		updateBranchName string
		oldRev           []byte
		newRev           []byte
	}{
		{
			desc:             "short name fast-forward update",
			updateBranchName: updateBranchName,
			oldRev:           []byte("0b4bc9a49b562e85de7cc9e834518ea6828729b9"),
			newRev:           []byte("1a35b5a77cf6af7edf6703f88e82f6aff613666f"),
		},
		{
			desc:             "short name non-fast-forward update",
			updateBranchName: "fix",
			oldRev:           []byte("48f0be4bd10c1decee6fae52f9ae6d10f77b60f4"),
			newRev:           []byte("12d65c8dd2b2676fa3ac47d955accc085a37a9c1"),
		},
		{
			desc:             "short name branch creation",
			updateBranchName: "a-new-branch",
			oldRev:           []byte(git.ZeroOID.String()),
			newRev:           []byte("845009f4d7bdc9e0d8f26b1c6fb6e108aaff9314"),
		},
		// We create refs/heads/heads/BRANCH and
		// refs/heads/refs/heads/BRANCH here. See a similar
		// test for UserCreateBranch in
		// TestSuccessfulCreateBranchRequest()
		{
			desc:             "heads/* branch creation",
			updateBranchName: "heads/a-new-branch",
			oldRev:           []byte(git.ZeroOID.String()),
			newRev:           []byte("845009f4d7bdc9e0d8f26b1c6fb6e108aaff9314"),
		},
		{
			desc:             "refs/heads/* branch creation",
			updateBranchName: "refs/heads/a-new-branch",
			oldRev:           []byte(git.ZeroOID.String()),
			newRev:           []byte("845009f4d7bdc9e0d8f26b1c6fb6e108aaff9314"),
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.desc, func(t *testing.T) {
			responseOk := &gitalypb.UserUpdateBranchResponse{}
			request := &gitalypb.UserUpdateBranchRequest{
				Repository: testRepo,
				BranchName: []byte(testCase.updateBranchName),
				Newrev:     testCase.newRev,
				Oldrev:     testCase.oldRev,
				User:       testhelper.TestUser,
			}
			response, err := client.UserUpdateBranch(ctx, request)
			require.NoError(t, err)
			require.Equal(t, responseOk, response)

			branchCommit, err := log.GetCommit(ctx, git.NewExecCommandFactory(config.Config), testRepo, git.Revision(testCase.updateBranchName))

			require.NoError(t, err)
			require.Equal(t, string(testCase.newRev), branchCommit.Id)

			branches := testhelper.MustRunCommand(t, nil, "git", "-C", testRepoPath, "for-each-ref", "--", "refs/heads/"+branchName)
			require.Contains(t, string(branches), "refs/heads/"+branchName)
		})
	}
}

func TestSuccessfulUserUpdateBranchRequestToDelete(t *testing.T) {
	testWithFeature(t, featureflag.GoUserUpdateBranch, testSuccessfulUserUpdateBranchRequestToDelete)
}

func testSuccessfulUserUpdateBranchRequestToDelete(t *testing.T, ctx context.Context) {
	serverSocketPath, stop := runOperationServiceServer(t)
	defer stop()

	client, conn := newOperationClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, testRepoPath, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	testCases := []struct {
		desc             string
		updateBranchName string
		oldRev           []byte
		newRev           []byte
		err              error
		createBranch     bool
	}{
		{
			desc:             "short name branch deletion",
			updateBranchName: "csv",
			oldRev:           []byte("3dd08961455abf80ef9115f4afdc1c6f968b503c"),
			newRev:           []byte(git.ZeroOID.String()),
			err:              status.Error(codes.InvalidArgument, "object not found"),
		},
		// We test for the failed heads/* and refs/heads/* cases below in TestFailedUserUpdateBranchRequest
		{
			desc:             "heads/* name branch deletion",
			updateBranchName: "heads/my-test-branch",
			createBranch:     true,
			oldRev:           []byte("689600b91aabec706e657e38ea706ece1ee8268f"),
			newRev:           []byte(git.ZeroOID.String()),
			err:              status.Error(codes.InvalidArgument, "object not found"),
		},
		{
			desc:             "refs/heads/* name branch deletion",
			updateBranchName: "refs/heads/my-other-test-branch",
			createBranch:     true,
			oldRev:           []byte("db46a1c5a5e474aa169b6cdb7a522d891bc4c5f9"),
			newRev:           []byte(git.ZeroOID.String()),
			err:              status.Error(codes.InvalidArgument, "object not found"),
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.desc, func(t *testing.T) {
			if testCase.createBranch {
				testhelper.MustRunCommand(t, nil, "git", "-C", testRepoPath, "branch", "--", testCase.updateBranchName, string(testCase.oldRev))
			}

			responseOk := &gitalypb.UserUpdateBranchResponse{}
			request := &gitalypb.UserUpdateBranchRequest{
				Repository: testRepo,
				BranchName: []byte(testCase.updateBranchName),
				Newrev:     testCase.newRev,
				Oldrev:     testCase.oldRev,
				User:       testhelper.TestUser,
			}
			response, err := client.UserUpdateBranch(ctx, request)
			require.Nil(t, err)
			require.Equal(t, responseOk, response)

			_, err = log.GetCommit(ctx, git.NewExecCommandFactory(config.Config), testRepo, git.Revision(testCase.updateBranchName))
			require.True(t, log.IsNotFound(err), "expected 'not found' error got %v", err)

			refs := testhelper.MustRunCommand(t, nil, "git", "-C", testRepoPath, "for-each-ref", "--", "refs/heads/"+testCase.updateBranchName)
			require.NotContains(t, string(refs), testCase.oldRev, "branch deleted from refs")
		})
	}
}

func TestSuccessfulGitHooksForUserUpdateBranchRequest(t *testing.T) {
	testWithFeature(t, featureflag.GoUserUpdateBranch, testSuccessfulGitHooksForUserUpdateBranchRequest)
}

func testSuccessfulGitHooksForUserUpdateBranchRequest(t *testing.T, ctx context.Context) {
	serverSocketPath, stop := runOperationServiceServer(t)
	defer stop()

	client, conn := newOperationClient(t, serverSocketPath)
	defer conn.Close()

	for _, hookName := range GitlabHooks {
		t.Run(hookName, func(t *testing.T) {
			testRepo, testRepoPath, cleanupFn := testhelper.NewTestRepo(t)
			defer cleanupFn()

			hookOutputTempPath, cleanup := testhelper.WriteEnvToCustomHook(t, testRepoPath, hookName)
			defer cleanup()

			request := &gitalypb.UserUpdateBranchRequest{
				Repository: testRepo,
				BranchName: []byte(updateBranchName),
				Newrev:     newrev,
				Oldrev:     oldrev,
				User:       testhelper.TestUser,
			}

			responseOk := &gitalypb.UserUpdateBranchResponse{}
			response, err := client.UserUpdateBranch(ctx, request)
			require.NoError(t, err)
			require.Empty(t, response.PreReceiveError)

			require.Equal(t, responseOk, response)
			output := string(testhelper.MustReadFile(t, hookOutputTempPath))
			require.Contains(t, output, "GL_USERNAME="+testhelper.TestUser.GlUsername)
		})
	}
}

func TestFailedUserUpdateBranchDueToHooks(t *testing.T) {
	testWithFeature(t, featureflag.GoUserUpdateBranch, testFailedUserUpdateBranchDueToHooks)
}

func testFailedUserUpdateBranchDueToHooks(t *testing.T, ctx context.Context) {
	testRepo, testRepoPath, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	// macOS' default tmp directory is under /var/... but /var itself is a symlink
	// to /private/var. The repository path in the environment variables has its symlinked
	// evaluated, causing the comparison to fail, thus we evaluate them here already so the
	// assertion later works.
	testRepoPath, err := filepath.EvalSymlinks(testRepoPath)
	require.NoError(t, err)

	serverSocketPath, stop := runOperationServiceServer(t)
	defer stop()

	client, conn := newOperationClient(t, serverSocketPath)
	defer conn.Close()

	request := &gitalypb.UserUpdateBranchRequest{
		Repository: testRepo,
		BranchName: []byte(updateBranchName),
		Newrev:     newrev,
		Oldrev:     oldrev,
		User:       testhelper.TestUser,
	}
	// Write a hook that will fail with the environment as the error message
	// so we can check that string for our env variables.
	hookContent := []byte("#!/bin/sh\nprintenv | paste -sd ' ' - >&2\nexit 1")

	for _, hookName := range gitlabPreHooks {
		remove := testhelper.WriteCustomHook(t, testRepoPath, hookName, hookContent)
		defer remove()

		response, err := client.UserUpdateBranch(ctx, request)
		require.Nil(t, err)
		require.Contains(t, response.PreReceiveError, "GL_USERNAME="+testhelper.TestUser.GlUsername)
		require.Contains(t, response.PreReceiveError, "PWD="+testRepoPath)

		responseOk := &gitalypb.UserUpdateBranchResponse{
			PreReceiveError: response.PreReceiveError,
		}
		require.Equal(t, responseOk, response)
	}
}

func TestFailedUserUpdateBranchRequest(t *testing.T) {
	testWithFeature(t, featureflag.GoUserUpdateBranch, testFailedUserUpdateBranchRequest)
}

func testFailedUserUpdateBranchRequest(t *testing.T, ctx context.Context) {
	serverSocketPath, stop := runOperationServiceServer(t)
	defer stop()

	client, conn := newOperationClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, _, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	revDoesntExist := fmt.Sprintf("%x", sha1.Sum([]byte("we need a non existent sha")))

	testCases := []struct {
		desc                string
		branchName          string
		newrev              []byte
		oldrev              []byte
		gotrev              []byte
		expectNotFoundError bool
		user                *gitalypb.User
		response            *gitalypb.UserUpdateBranchResponse
		err                 error
	}{
		{
			desc:                "empty branch name",
			branchName:          "",
			newrev:              newrev,
			oldrev:              oldrev,
			expectNotFoundError: true,
			user:                testhelper.TestUser,
			err:                 status.Error(codes.InvalidArgument, "empty branch name"),
		},
		{
			desc:       "empty newrev",
			branchName: updateBranchName,
			newrev:     nil,
			oldrev:     oldrev,
			user:       testhelper.TestUser,
			err:        status.Error(codes.InvalidArgument, "empty newrev"),
		},
		{
			desc:       "empty oldrev",
			branchName: updateBranchName,
			newrev:     newrev,
			oldrev:     nil,
			gotrev:     oldrev,
			user:       testhelper.TestUser,
			err:        status.Error(codes.InvalidArgument, "empty oldrev"),
		},
		{
			desc:       "empty user",
			branchName: updateBranchName,
			newrev:     newrev,
			oldrev:     oldrev,
			user:       nil,
			err:        status.Error(codes.InvalidArgument, "empty user"),
		},
		{
			desc:                "non-existing branch",
			branchName:          "i-dont-exist",
			newrev:              newrev,
			oldrev:              oldrev,
			expectNotFoundError: true,
			user:                testhelper.TestUser,
			err:                 status.Errorf(codes.FailedPrecondition, "Could not update %v. Please refresh and try again.", "i-dont-exist"),
		},
		{
			desc:       "existing branch failed deletion attempt",
			branchName: "csv",
			newrev:     []byte(git.ZeroOID.String()),
			oldrev:     oldrev,
			gotrev:     []byte("3dd08961455abf80ef9115f4afdc1c6f968b503c"),
			user:       testhelper.TestUser,
			err:        status.Errorf(codes.FailedPrecondition, "Could not update %v. Please refresh and try again.", "csv"),
		},
		{
			desc:       "non-existing newrev",
			branchName: updateBranchName,
			newrev:     []byte(revDoesntExist),
			oldrev:     oldrev,
			user:       testhelper.TestUser,
			err:        status.Errorf(codes.FailedPrecondition, "Could not update %v. Please refresh and try again.", updateBranchName),
		},
		{
			desc:       "non-existing oldrev",
			branchName: updateBranchName,
			newrev:     newrev,
			oldrev:     []byte(revDoesntExist),
			gotrev:     oldrev,
			user:       testhelper.TestUser,
			err:        status.Errorf(codes.FailedPrecondition, "Could not update %v. Please refresh and try again.", updateBranchName),
		},
		{
			desc:       "existing branch, but unsupported heads/* name",
			branchName: "heads/feature",
			newrev:     []byte("1a35b5a77cf6af7edf6703f88e82f6aff613666f"),
			oldrev:     []byte("0b4bc9a49b562e85de7cc9e834518ea6828729b9"),
			user:       testhelper.TestUser,
			err:        status.Errorf(codes.FailedPrecondition, "Could not update %v. Please refresh and try again.", "heads/feature"),
		},
		{
			desc:       "delete existing branch, but unsupported refs/heads/* name",
			branchName: "refs/heads/crlf-diff",
			newrev:     []byte(git.ZeroOID.String()),
			oldrev:     []byte("593890758a6f845c600f38ffa05be2749211caee"),
			user:       testhelper.TestUser,
			err:        status.Errorf(codes.FailedPrecondition, "Could not update %v. Please refresh and try again.", "refs/heads/crlf-diff"),
		},
		{
			desc:                "short name branch deletion",
			branchName:          "csv",
			oldrev:              []byte("3dd08961455abf80ef9115f4afdc1c6f968b503c"),
			newrev:              []byte(git.ZeroOID.String()),
			expectNotFoundError: true,
			user:                testhelper.TestUser,
			err:                 nil,
			response:            &gitalypb.UserUpdateBranchResponse{},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.desc, func(t *testing.T) {
			request := &gitalypb.UserUpdateBranchRequest{
				Repository: testRepo,
				BranchName: []byte(testCase.branchName),
				Newrev:     testCase.newrev,
				Oldrev:     testCase.oldrev,
				User:       testCase.user,
			}

			response, err := client.UserUpdateBranch(ctx, request)
			require.Equal(t, testCase.response, response)
			require.Equal(t, testCase.err, err)

			branchCommit, err := log.GetCommit(ctx, git.NewExecCommandFactory(config.Config), testRepo, git.Revision(testCase.branchName))
			if testCase.expectNotFoundError {
				require.True(t, log.IsNotFound(err), "expected 'not found' error got %v", err)
				return
			}
			require.NoError(t, err)

			if len(testCase.gotrev) == 0 {
				// The common case is the update didn't succeed
				testCase.gotrev = testCase.oldrev
			}
			require.Equal(t, string(testCase.gotrev), branchCommit.Id)
		})
	}
}
