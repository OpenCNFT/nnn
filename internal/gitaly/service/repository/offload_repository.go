package repository

import (
	"context"

	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// OffloadRepository is the OffloadRepository RPC entrypoint. It further calls housekeeping manager to
// perform actual offloading task with or without the transaction management.
func (s *server) OffloadRepository(ctx context.Context, in *gitalypb.OffloadRequest) (*gitalypb.OffloadResponse, error) {
	if !s.cfg.Offload.Enabled {
		return nil, status.Errorf(codes.FailedPrecondition, "feature disabled")
	}
	return &gitalypb.OffloadResponse{}, nil
}

// RehydrateRepository is the RehydrateRepository RPC entrypoint. It further calls housekeeping manager to
// perform actual rehydrating task with or without the transaction management.
func (s *server) RehydrateRepository(ctx context.Context, in *gitalypb.RehydrateRequest) (*gitalypb.RehydrateResponse, error) {
	if !s.cfg.Offload.Enabled {
		return nil, status.Errorf(codes.FailedPrecondition, "feature disabled")
	}
	return &gitalypb.RehydrateResponse{}, nil
}
