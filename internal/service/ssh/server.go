package ssh

import "gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"

type server struct {
	gitalypb.UnimplementedSSHServiceServer
}

// NewServer creates a new instance of a grpc SSHServer
func NewServer() gitalypb.SSHServiceServer {
	return &server{}
}
