package ssh

import (
	"fmt"
	"os/exec"

	log "github.com/Sirupsen/logrus"
	"gitlab.com/gitlab-org/gitaly/internal/helper"

	pb "gitlab.com/gitlab-org/gitaly-proto/go"
	pbhelper "gitlab.com/gitlab-org/gitaly-proto/go/helper"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
)

func (s *server) SSHReceivePack(stream pb.SSHService_SSHReceivePackServer) error {
	req, err := stream.Recv() // First request contains only Repository and GlId
	if err != nil {
		return err
	}
	if err = validateFirstReceivePackRequest(req); err != nil {
		return err
	}

	stdin := pbhelper.NewReceiveReader(func() ([]byte, error) {
		request, err := stream.Recv()
		return request.GetStdin(), err
	})
	stdout := pbhelper.NewSendWriter(func(p []byte) error {
		return stream.Send(&pb.SSHReceivePackResponse{Stdout: p})
	})
	stderr := pbhelper.NewSendWriter(func(p []byte) error {
		return stream.Send(&pb.SSHReceivePackResponse{Stderr: p})
	})
	env := []string{
		fmt.Sprintf("GL_ID=%s", req.GlId),
		"GL_PROTOCOL=ssh",
	}
	if req.GlRepository != "" {
		env = append(env, fmt.Sprintf("GL_REPOSITORY=%s", req.GlRepository))
	}

	repoPath, err := helper.GetRepoPath(req.Repository)
	if err != nil {
		return err
	}

	log.WithFields(log.Fields{
		"RepoPath":     repoPath,
		"GlID":         req.GlId,
		"GlRepository": req.GlRepository,
	}).Debug("SSHReceivePack")

	osCommand := exec.Command("git-receive-pack", repoPath)
	cmd, err := helper.NewCommand(osCommand, stdin, stdout, stderr, env...)

	if err != nil {
		return grpc.Errorf(codes.Unavailable, "SSHReceivePack: cmd: %v", err)
	}
	defer cmd.Kill()

	if err := cmd.Wait(); err != nil {
		if status, ok := helper.ExitStatus(err); ok {
			return helper.DecorateError(
				codes.Internal,
				stream.Send(&pb.SSHReceivePackResponse{ExitStatus: &pb.ExitStatus{Value: int32(status)}}),
			)
		}
		return grpc.Errorf(codes.Unavailable, "SSHReceivePack: cmd wait for %v: %v", cmd.Args, err)
	}

	return nil
}

func validateFirstReceivePackRequest(req *pb.SSHReceivePackRequest) error {
	if req.GlId == "" {
		return grpc.Errorf(codes.InvalidArgument, "SSHReceivePack: empty GlId")
	}
	if req.Stdin != nil {
		return grpc.Errorf(codes.InvalidArgument, "SSHReceivePack: non-empty data")
	}

	return nil
}
