package smarthttp

import (
	"fmt"
	"os/exec"

	log "github.com/Sirupsen/logrus"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus"
	"gitlab.com/gitlab-org/gitaly/internal/helper"

	pb "gitlab.com/gitlab-org/gitaly-proto/go"
	"gitlab.com/gitlab-org/gitaly/streamio"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
)

func (s *server) PostReceivePack(stream pb.SmartHTTPService_PostReceivePackServer) error {
	req, err := stream.Recv() // First request contains only Repository and GlId
	if err != nil {
		return err
	}

	grpc_logrus.Extract(stream.Context()).WithFields(log.Fields{
		"GlID":         req.GlId,
		"GlRepository": req.GlRepository,
	}).Debug("PostReceivePack")

	if err := validateReceivePackRequest(req); err != nil {
		return err
	}

	stdin := streamio.NewReader(func() ([]byte, error) {
		resp, err := stream.Recv()
		return resp.GetData(), err
	})
	stdout := streamio.NewWriter(func(p []byte) error {
		return stream.Send(&pb.PostReceivePackResponse{Data: p})
	})
	env := []string{
		fmt.Sprintf("GL_ID=%s", req.GlId),
		"GL_PROTOCOL=http",
	}
	if req.GlRepository != "" {
		env = append(env, fmt.Sprintf("GL_REPOSITORY=%s", req.GlRepository))
	}
	repoPath, err := helper.GetRepoPath(req.Repository)
	if err != nil {
		return err
	}

	osCommand := exec.Command(helper.GitPath(), "receive-pack", "--stateless-rpc", repoPath)
	cmd, err := helper.NewCommand(stream.Context(), osCommand, stdin, stdout, nil, env...)

	if err != nil {
		return grpc.Errorf(codes.Unavailable, "PostReceivePack: cmd: %v", err)
	}
	defer cmd.Kill()

	if err := cmd.Wait(); err != nil {
		return grpc.Errorf(codes.Unavailable, "PostReceivePack: cmd wait for %v: %v", cmd.Args, err)
	}

	return nil
}

func validateReceivePackRequest(req *pb.PostReceivePackRequest) error {
	if req.GlId == "" {
		return grpc.Errorf(codes.InvalidArgument, "PostReceivePack: empty GlId")
	}
	if req.Data != nil {
		return grpc.Errorf(codes.InvalidArgument, "PostReceivePack: non-empty Data")
	}

	return nil
}
