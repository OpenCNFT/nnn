package cleanup

import (
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
)

type server struct {
	gitalypb.UnimplementedCleanupServiceServer
}

// NewServer creates a new instance of a grpc CleanupServer
func NewServer() gitalypb.CleanupServiceServer {
	return &server{}
}
