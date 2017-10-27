package diff

import (
	"fmt"
	"os"
	"path"
	"testing"

	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/streamio"

	pb "gitlab.com/gitlab-org/gitaly-proto/go"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
)

func TestSuccessfulRawDiffRequest(t *testing.T) {
	server, serverSocketPath := runDiffServer(t)
	defer server.Stop()

	client, conn := newDiffClient(t, serverSocketPath)
	defer conn.Close()

	ctx, cancel := testhelper.Context()
	defer cancel()

	rightCommit := "e395f646b1499e8e0279445fc99a0596a65fab7e"
	leftCommit := "8a0f2ee90d940bfb0ba1e14e8214b0649056e4ab"
	rpcRequest := &pb.RawDiffRequest{Repository: testRepo, RightCommitId: rightCommit, LeftCommitId: leftCommit}

	c, err := client.RawDiff(ctx, rpcRequest)
	require.NoError(t, err)

	reader := streamio.NewReader(func() ([]byte, error) {
		response, err := c.Recv()
		return response.GetData(), err
	})

	committerName := "Scrooge McDuck"
	committerEmail := "scrooge@mcduck.com"
	storagePath := testhelper.GitlabTestStoragePath()
	sandboxRepoPath := path.Join(storagePath, "raw-diff-sandbox")

	testhelper.MustRunCommand(t, nil, "git", "clone", testRepoPath, sandboxRepoPath)
	testhelper.MustRunCommand(t, nil, "git", "-C", sandboxRepoPath, "reset", "--hard", leftCommit)
	defer os.RemoveAll(sandboxRepoPath)

	testhelper.MustRunCommand(t, reader, "git", "-C", sandboxRepoPath, "apply")
	testhelper.MustRunCommand(t, reader, "git", "-C", sandboxRepoPath, "add", ".")
	testhelper.MustRunCommand(t, nil, "git", "-C", sandboxRepoPath,
		"-c", fmt.Sprintf("user.name=%s", committerName),
		"-c", fmt.Sprintf("user.email=%s", committerEmail),
		"commit", "-m", "Applying received raw diff")

	expectedTreeStructure := testhelper.MustRunCommand(t, nil, "git", "-C", testRepoPath, "ls-tree", "-r", rightCommit)
	actualTreeStructure := testhelper.MustRunCommand(t, nil, "git", "-C", sandboxRepoPath, "ls-tree", "-r", "HEAD")
	require.Equal(t, expectedTreeStructure, actualTreeStructure)
}

func TestFailedRawDiffRequestDueToValidations(t *testing.T) {
	server, serverSocketPath := runDiffServer(t)
	defer server.Stop()

	client, conn := newDiffClient(t, serverSocketPath)
	defer conn.Close()

	testCases := []struct {
		desc    string
		request *pb.RawDiffRequest
		code    codes.Code
	}{
		{
			desc: "empty left commit",
			request: &pb.RawDiffRequest{
				Repository:    testRepo,
				LeftCommitId:  "",
				RightCommitId: "e395f646b1499e8e0279445fc99a0596a65fab7e",
			},
			code: codes.InvalidArgument,
		},
		{
			desc: "empty right commit",
			request: &pb.RawDiffRequest{
				Repository:    testRepo,
				RightCommitId: "",
				LeftCommitId:  "e395f646b1499e8e0279445fc99a0596a65fab7e",
			},
			code: codes.InvalidArgument,
		},
		{
			desc: "empty repo",
			request: &pb.RawDiffRequest{
				Repository:    nil,
				RightCommitId: "8a0f2ee90d940bfb0ba1e14e8214b0649056e4ab",
				LeftCommitId:  "e395f646b1499e8e0279445fc99a0596a65fab7e",
			},
			code: codes.InvalidArgument,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.desc, func(t *testing.T) {
			ctx, cancel := testhelper.Context()
			defer cancel()

			c, _ := client.RawDiff(ctx, testCase.request)
			testhelper.AssertGrpcError(t, drainRawDiffResponse(c), testCase.code, "")
		})
	}
}

func TestSuccessfulRawPatchRequest(t *testing.T) {
	server, serverSocketPath := runDiffServer(t)
	defer server.Stop()

	client, conn := newDiffClient(t, serverSocketPath)
	defer conn.Close()

	ctx, cancel := testhelper.Context()
	defer cancel()

	rightCommit := "e395f646b1499e8e0279445fc99a0596a65fab7e"
	leftCommit := "8a0f2ee90d940bfb0ba1e14e8214b0649056e4ab"
	rpcRequest := &pb.RawPatchRequest{Repository: testRepo, RightCommitId: rightCommit, LeftCommitId: leftCommit}

	c, err := client.RawPatch(ctx, rpcRequest)
	require.NoError(t, err)

	reader := streamio.NewReader(func() ([]byte, error) {
		response, err := c.Recv()
		return response.GetData(), err
	})

	storagePath := testhelper.GitlabTestStoragePath()
	sandboxRepoPath := path.Join(storagePath, "raw-patch-sandbox")

	testhelper.MustRunCommand(t, nil, "git", "clone", testRepoPath, sandboxRepoPath)
	testhelper.MustRunCommand(t, nil, "git", "-C", sandboxRepoPath, "reset", "--hard", leftCommit)
	defer os.RemoveAll(sandboxRepoPath)

	testhelper.MustRunCommand(t, reader, "git", "-C", sandboxRepoPath, "am")

	expectedTreeStructure := testhelper.MustRunCommand(t, nil, "git", "-C", testRepoPath, "ls-tree", "-r", rightCommit)
	actualTreeStructure := testhelper.MustRunCommand(t, nil, "git", "-C", sandboxRepoPath, "ls-tree", "-r", "HEAD")
	require.Equal(t, expectedTreeStructure, actualTreeStructure)
}

func TestFailedRawPatchRequestDueToValidations(t *testing.T) {
	server, serverSocketPath := runDiffServer(t)
	defer server.Stop()

	client, conn := newDiffClient(t, serverSocketPath)
	defer conn.Close()

	testCases := []struct {
		desc    string
		request *pb.RawPatchRequest
		code    codes.Code
	}{
		{
			desc: "empty left commit",
			request: &pb.RawPatchRequest{
				Repository:    testRepo,
				LeftCommitId:  "",
				RightCommitId: "e395f646b1499e8e0279445fc99a0596a65fab7e",
			},
			code: codes.InvalidArgument,
		},
		{
			desc: "empty right commit",
			request: &pb.RawPatchRequest{
				Repository:    testRepo,
				RightCommitId: "",
				LeftCommitId:  "e395f646b1499e8e0279445fc99a0596a65fab7e",
			},
			code: codes.InvalidArgument,
		},
		{
			desc: "empty repo",
			request: &pb.RawPatchRequest{
				Repository:    nil,
				RightCommitId: "8a0f2ee90d940bfb0ba1e14e8214b0649056e4ab",
				LeftCommitId:  "e395f646b1499e8e0279445fc99a0596a65fab7e",
			},
			code: codes.InvalidArgument,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.desc, func(t *testing.T) {
			ctx, cancel := testhelper.Context()
			defer cancel()

			c, _ := client.RawPatch(ctx, testCase.request)
			testhelper.AssertGrpcError(t, drainRawPatchResponse(c), testCase.code, "")
		})
	}
}

func drainRawDiffResponse(c pb.DiffService_RawDiffClient) error {
	var err error
	for err == nil {
		_, err = c.Recv()
	}
	return err
}

func drainRawPatchResponse(c pb.DiffService_RawPatchClient) error {
	var err error
	for err == nil {
		_, err = c.Recv()
	}
	return err
}
