package info

import (
	"context"
	"errors"

	"gitlab.com/gitlab-org/gitaly/internal/helper"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/config"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/datastore"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/nodes"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
)

// Server is a InfoService server
type Server struct {
	gitalypb.UnimplementedPraefectInfoServiceServer
	nodeMgr nodes.Manager
	conf    config.Config
	queue   datastore.ReplicationEventQueue
}

// NewServer creates a new instance of a grpc InfoServiceServer
func NewServer(nodeMgr nodes.Manager, conf config.Config, queue datastore.ReplicationEventQueue) gitalypb.PraefectInfoServiceServer {
	s := &Server{
		nodeMgr: nodeMgr,
		conf:    conf,
		queue:   queue,
	}

	return s
}

func (s *Server) EnableWrites(ctx context.Context, req *gitalypb.EnableWritesRequest) (*gitalypb.EnableWritesResponse, error) {
	if err := s.nodeMgr.EnableWrites(ctx, req.GetVirtualStorage()); err != nil {
		if errors.Is(err, nodes.ErrVirtualStorageNotExist) {
			return nil, helper.ErrInvalidArgument(err)
		}

		return nil, helper.ErrInternal(err)
	}

	return &gitalypb.EnableWritesResponse{}, nil
}
