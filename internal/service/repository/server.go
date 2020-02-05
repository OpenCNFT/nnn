package repository

import (
	"context"
	"sync"

	"gitlab.com/gitlab-org/gitaly/internal/helper"
	"gitlab.com/gitlab-org/gitaly/internal/rubyserver"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"google.golang.org/grpc"
)

type server struct {
	ruby *rubyserver.Server
	gitalypb.UnimplementedRepositoryServiceServer
	connsByAddress map[string]*grpc.ClientConn
	connsMtx       sync.RWMutex
}

// NewServer creates a new instance of a gRPC repo server
func NewServer(rs *rubyserver.Server) gitalypb.RepositoryServiceServer {
	return &server{ruby: rs, connsByAddress: make(map[string]*grpc.ClientConn)}
}

func (*server) FetchHTTPRemote(context.Context, *gitalypb.FetchHTTPRemoteRequest) (*gitalypb.FetchHTTPRemoteResponse, error) {
	return nil, helper.Unimplemented
}
