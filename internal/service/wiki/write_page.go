package wiki

import (
	"fmt"

	"gitlab.com/gitlab-org/gitaly/internal/rubyserver"

	pb "gitlab.com/gitlab-org/gitaly-proto/go"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *server) WikiWritePage(stream pb.WikiService_WikiWritePageServer) error {
	firstRequest, err := stream.Recv()
	if err != nil {
		return err
	}

	if err := validateWikiWritePageRequest(firstRequest); err != nil {
		return status.Errorf(codes.InvalidArgument, "WikiWritePage: %v", err)
	}

	ctx := stream.Context()

	client, err := s.WikiServiceClient(ctx)
	if err != nil {
		return err
	}

	clientCtx, err := rubyserver.SetHeaders(ctx, firstRequest.GetRepository())
	if err != nil {
		return err
	}

	rubyStream, err := client.WikiWritePage(clientCtx)
	if err != nil {
		return err
	}

	if err := rubyStream.Send(firstRequest); err != nil {
		return err
	}

	err = rubyserver.Proxy(func() error {
		request, err := stream.Recv()
		if err != nil {
			return err
		}
		return rubyStream.Send(request)
	})

	if err != nil {
		return err
	}

	response, err := rubyStream.CloseAndRecv()
	if err != nil {
		return err
	}

	return stream.SendAndClose(response)
}

func validateWikiWritePageRequest(request *pb.WikiWritePageRequest) error {
	if len(request.GetName()) == 0 {
		return fmt.Errorf("empty Name")
	}

	if request.GetFormat() == "" {
		return fmt.Errorf("empty Format")
	}

	return validateRequestCommitDetails(request)
}
