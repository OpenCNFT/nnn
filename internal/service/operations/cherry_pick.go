package operations

import (
	"context"

	"gitlab.com/gitlab-org/gitaly/internal/rubyserver"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *server) UserCherryPick(ctx context.Context, req *gitalypb.UserCherryPickRequest) (*gitalypb.UserCherryPickResponse, error) {
	if err := validateCherryPickOrRevertRequest(req); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "UserCherryPick: %v", err)
	}

	client, err := s.ruby.OperationServiceClient(ctx)
	if err != nil {
		return nil, err
	}

	clientCtx, err := rubyserver.SetHeaders(ctx, req.GetRepository())
	if err != nil {
		return nil, err
	}

	return client.UserCherryPick(clientCtx, req)
}
