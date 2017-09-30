package ssh

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path"
	"strings"
	"testing"

	"github.com/golang/protobuf/jsonpb"
	"github.com/stretchr/testify/require"

	"gitlab.com/gitlab-org/gitaly/internal/testhelper"

	pb "gitlab.com/gitlab-org/gitaly-proto/go"

	"golang.org/x/net/context"
	"google.golang.org/grpc/codes"
)

func TestFailedUploadPackRequestDueToValidationError(t *testing.T) {
	server := runSSHServer(t)
	defer server.Stop()

	client, conn := newSSHClient(t)
	defer conn.Close()

	tests := []struct {
		Desc string
		Req  *pb.SSHUploadPackRequest
		Code codes.Code
	}{
		{
			Desc: "Repository.RelativePath is empty",
			Req:  &pb.SSHUploadPackRequest{Repository: &pb.Repository{StorageName: "default", RelativePath: ""}},
			Code: codes.InvalidArgument,
		},
		{
			Desc: "Repository is nil",
			Req:  &pb.SSHUploadPackRequest{Repository: nil},
			Code: codes.InvalidArgument,
		},
		{
			Desc: "Data exists on first request",
			Req:  &pb.SSHUploadPackRequest{Repository: &pb.Repository{StorageName: "default", RelativePath: "path/to/repo"}, Stdin: []byte("Fail")},
			Code: codes.InvalidArgument,
		},
	}

	for _, test := range tests {
		t.Run(test.Desc, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			stream, err := client.SSHUploadPack(ctx)
			if err != nil {
				t.Fatal(err)
			}

			if err = stream.Send(test.Req); err != nil {
				t.Fatal(err)
			}
			stream.CloseSend()

			err = drainPostUploadPackResponse(stream)
			testhelper.AssertGrpcError(t, err, test.Code, "")
		})
	}
}

func TestUploadPackCloneSuccess(t *testing.T) {
	server := runSSHServer(t)
	defer server.Stop()

	localRepoPath := path.Join(testRepoRoot, "gitlab-test-upload-pack-local")

	tests := []struct {
		cmd  *exec.Cmd
		desc string
	}{
		{
			cmd:  exec.Command("git", "clone", "git@localhost:test/test.git", localRepoPath),
			desc: "full clone",
		},
		{
			cmd:  exec.Command("git", "clone", "--depth", "1", "git@localhost:test/test.git", localRepoPath),
			desc: "shallow clone",
		},
	}

	for _, tc := range tests {
		t.Run(tc.desc, func(t *testing.T) {
			lHead, rHead, _, _, err := testClone(t, testRepo.GetStorageName(), testRepo.GetRelativePath(), localRepoPath, "", tc.cmd)
			require.NoError(t, err, "clone failed")
			require.Equal(t, lHead, rHead, "local and remote head not equal")
		})
	}
}

func TestUploadPackCloneHideTags(t *testing.T) {
	server := runSSHServer(t)
	defer server.Stop()

	localRepoPath := path.Join(testRepoRoot, "gitlab-test-upload-pack-local-hide-tags")

	cmd := exec.Command("git", "clone", "--mirror", "git@localhost:test/test.git", localRepoPath)

	_, _, lTags, rTags, err := testClone(t, testRepo.GetStorageName(), testRepo.GetRelativePath(), localRepoPath, "transfer.hideRefs=refs/tags", cmd)

	if err != nil {
		t.Fatalf("clone failed: %v", err)
	}
	if lTags == rTags {
		t.Fatalf("local and remote tags are equal. clone failed: %q != %q", lTags, rTags)
	}
	if tag := "v1.0.0"; !strings.Contains(rTags, tag) {
		t.Fatalf("sanity check failed, tag %q not found in %q", tag, rTags)
	}
}

func TestUploadPackCloneFailure(t *testing.T) {
	server := runSSHServer(t)
	defer server.Stop()

	localRepoPath := path.Join(testRepoRoot, "gitlab-test-upload-pack-local-failure")

	cmd := exec.Command("git", "clone", "git@localhost:test/test.git", localRepoPath)

	_, _, _, _, err := testClone(t, "foobar", testRepo.GetRelativePath(), localRepoPath, "", cmd)
	if err == nil {
		t.Fatalf("clone didn't fail")
	}
}

func testClone(t *testing.T, storageName, relativePath, localRepoPath string, gitConfig string, cmd *exec.Cmd) (string, string, string, string, error) {
	defer os.RemoveAll(localRepoPath)

	pbTempRepo := &pb.Repository{StorageName: storageName, RelativePath: relativePath}
	req := &pb.SSHUploadPackRequest{
		Repository: pbTempRepo,
	}
	if gitConfig != "" {
		req.GitConfigOptions = strings.Split(gitConfig, " ")
	}
	pbMarshaler := &jsonpb.Marshaler{}
	payload, err := pbMarshaler.MarshalToString(req)

	require.NoError(t, err)

	cmd.Env = []string{
		fmt.Sprintf("GITALY_ADDRESS=unix:%s", serverSocketPath),
		fmt.Sprintf("GITALY_PAYLOAD=%s", payload),
		fmt.Sprintf("PATH=%s", ".:"+os.Getenv("PATH")),
		fmt.Sprintf(`GIT_SSH_COMMAND=%s upload-pack`, gitalySSHPath),
	}

	out, err := cmd.CombinedOutput()
	if err != nil {
		return "", "", "", "", fmt.Errorf("%v: %q", err, out)
	}
	if !cmd.ProcessState.Success() {
		return "", "", "", "", fmt.Errorf("Failed to run `git clone`: %q", out)
	}

	storagePath := testhelper.GitlabTestStoragePath()
	testRepoPath := path.Join(storagePath, testRepo.GetRelativePath())

	remoteHead := bytes.TrimSpace(testhelper.MustRunCommand(t, nil, "git", "-C", testRepoPath, "rev-parse", "master"))
	localHead := bytes.TrimSpace(testhelper.MustRunCommand(t, nil, "git", "-C", localRepoPath, "rev-parse", "master"))

	remoteTags := bytes.TrimSpace(testhelper.MustRunCommand(t, nil, "git", "-C", testRepoPath, "tag"))
	localTags := bytes.TrimSpace(testhelper.MustRunCommand(t, nil, "git", "-C", localRepoPath, "tag"))

	return string(localHead), string(remoteHead), string(localTags), string(remoteTags), nil
}

func drainPostUploadPackResponse(stream pb.SSH_SSHUploadPackClient) error {
	var err error
	for err == nil {
		_, err = stream.Recv()
	}
	return err
}
