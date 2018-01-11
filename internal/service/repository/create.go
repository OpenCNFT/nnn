package repository

import (
	pb "gitlab.com/gitlab-org/gitaly-proto/go"
	"gitlab.com/gitlab-org/gitaly/internal/rubyserver"

	"golang.org/x/net/context"
)

func (s *server) CreateRepository(ctx context.Context, req *pb.CreateRepositoryRequest) (*pb.CreateRepositoryResponse, error) {
	client, err := s.RepositoryServiceClient(ctx)
	if err != nil {
		return nil, err
	}

	clientCtx, err := rubyserver.SetHeadersWithoutRepoCheck(ctx, req.GetRepository())
	if err != nil {
		return nil, err
	}

	return client.CreateRepository(clientCtx, req)
}
