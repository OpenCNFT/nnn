package wiki

import (
	"context"
	"fmt"

	"gitlab.com/gitlab-org/gitaly/internal/rubyserver"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *server) WikiDeletePage(ctx context.Context, request *gitalypb.WikiDeletePageRequest) (*gitalypb.WikiDeletePageResponse, error) {
	if err := validateWikiDeletePageRequest(request); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "WikiDeletePage: %v", err)
	}

	client, err := s.ruby.WikiServiceClient(ctx)
	if err != nil {
		return nil, err
	}

	clientCtx, err := rubyserver.SetHeaders(ctx, request.GetRepository())
	if err != nil {
		return nil, err
	}

	return client.WikiDeletePage(clientCtx, request)
}

func validateWikiDeletePageRequest(request *gitalypb.WikiDeletePageRequest) error {
	if len(request.GetPagePath()) == 0 {
		return fmt.Errorf("empty PagePath")
	}

	return validateRequestCommitDetails(request)
}
