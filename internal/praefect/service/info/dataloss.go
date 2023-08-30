package info

import (
	"context"

	"gitlab.com/gitlab-org/gitaly/v16/internal/helper/chunk"
	"gitlab.com/gitlab-org/gitaly/v16/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
	"google.golang.org/protobuf/proto"
)

type repoSender struct {
	repos []*gitalypb.DatalossResponse_Repository
	send  func([]*gitalypb.DatalossResponse_Repository) error
}

func (t *repoSender) Reset() {
	t.repos = t.repos[:0]
}

func (t *repoSender) Append(m proto.Message) {
	t.repos = append(t.repos, m.(*gitalypb.DatalossResponse_Repository))
}

func (t *repoSender) Send() error {
	return t.send(t.repos)
}

// Dataloss implements the Dataloss RPC to return data about repositories that
// are outdated.
func (s *Server) Dataloss(
	req *gitalypb.DatalossRequest,
	stream gitalypb.PraefectInfoService_DatalossServer,
) error {
	ctx := stream.Context()

	repos, err := s.rs.GetPartiallyAvailableRepositories(ctx, req.GetVirtualStorage())
	if err != nil {
		return err
	}

	chunker := chunk.New(&repoSender{
		send: func(repos []*gitalypb.DatalossResponse_Repository) error {
			return stream.Send(&gitalypb.DatalossResponse{
				Repositories: repos,
			})
		},
	})

	for _, outdatedRepo := range repos {
		unavailable := true

		storages := make([]*gitalypb.DatalossResponse_Repository_Storage, 0, len(outdatedRepo.Replicas))
		for _, replica := range outdatedRepo.Replicas {
			if replica.ValidPrimary {
				unavailable = false
			}

			storages = append(storages, &gitalypb.DatalossResponse_Repository_Storage{
				Name:         replica.Storage,
				BehindBy:     outdatedRepo.Generation - replica.Generation,
				Assigned:     replica.Assigned,
				Healthy:      replica.Healthy,
				ValidPrimary: replica.ValidPrimary,
			})
		}

		if !req.IncludePartiallyReplicated && !unavailable {
			continue
		}

		if err := chunker.Send(&gitalypb.DatalossResponse_Repository{
			RelativePath: outdatedRepo.RelativePath,
			Primary:      outdatedRepo.Primary,
			Unavailable:  unavailable,
			Storages:     storages,
		}); err != nil {
			return structerr.NewInternal("sending repository info: %w", err)
		}
	}

	if err := chunker.Flush(); err != nil {
		return structerr.NewInternal("flushing repositories: %w", err)
	}

	return nil
}

// DatalossCheck implements the DatalossCheck RPC to return data about repositories that
// are outdated.
// Deprecated: The Dataloss streaming RPC will be used going forward
//
//nolint:staticcheck
func (s *Server) DatalossCheck(ctx context.Context, req *gitalypb.DatalossCheckRequest) (*gitalypb.DatalossCheckResponse, error) {
	repos, err := s.rs.GetPartiallyAvailableRepositories(ctx, req.GetVirtualStorage())
	if err != nil {
		return nil, err
	}

	pbRepos := make([]*gitalypb.DatalossCheckResponse_Repository, 0, len(repos))
	for _, outdatedRepo := range repos {
		unavailable := true

		storages := make([]*gitalypb.DatalossCheckResponse_Repository_Storage, 0, len(outdatedRepo.Replicas))
		for _, replica := range outdatedRepo.Replicas {
			if replica.ValidPrimary {
				unavailable = false
			}

			storages = append(storages, &gitalypb.DatalossCheckResponse_Repository_Storage{
				Name:         replica.Storage,
				BehindBy:     outdatedRepo.Generation - replica.Generation,
				Assigned:     replica.Assigned,
				Healthy:      replica.Healthy,
				ValidPrimary: replica.ValidPrimary,
			})
		}

		if !req.IncludePartiallyReplicated && !unavailable {
			continue
		}

		//nolint:staticcheck
		pbRepos = append(pbRepos, &gitalypb.DatalossCheckResponse_Repository{
			RelativePath: outdatedRepo.RelativePath,
			Primary:      outdatedRepo.Primary,
			Unavailable:  unavailable,
			Storages:     storages,
		})
	}

	return &gitalypb.DatalossCheckResponse{
		Repositories: pbRepos,
	}, nil
}
