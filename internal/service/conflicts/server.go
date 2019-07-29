package conflicts

import (
	"gitlab.com/gitlab-org/gitaly/internal/rubyserver"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
)

type server struct {
	*rubyserver.Server
}

// NewServer creates a new instance of a grpc ConflictsServer
func NewServer(rs *rubyserver.Server) gitalypb.ConflictsServiceServer {
	return &server{rs}
}
