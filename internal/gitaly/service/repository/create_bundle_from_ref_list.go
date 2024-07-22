package repository

import (
	"bytes"
	"errors"

	"gitlab.com/gitlab-org/gitaly/v16/internal/git/housekeeping"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v16/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/v16/streamio"
)

func (s *server) CreateBundleFromRefList(stream gitalypb.RepositoryService_CreateBundleFromRefListServer) error {
	ctx := stream.Context()

	firstRequest, err := stream.Recv()
	if err != nil {
		return err
	}
	if firstRequest.GetPatterns() != nil {
		return structerr.NewInvalidArgument("patterns should not be set for the first request in the stream")
	}

	repository := firstRequest.GetRepository()
	if err := s.locator.ValidateRepository(ctx, repository); err != nil {
		return structerr.NewInvalidArgument("%w", err)
	}

	// Acknowledge the stream header for client to start the data stream.
	if err := stream.Send(&gitalypb.CreateBundleFromRefListResponse{}); err != nil {
		return err
	}

	repo := s.localrepo(repository)

	if err := housekeeping.CleanupWorktrees(ctx, repo); err != nil {
		return structerr.NewInternal("cleaning up worktrees: %w", err)
	}

	patterns := streamio.NewReader(func() ([]byte, error) {
		request, err := stream.Recv()
		if err != nil {
			return nil, err
		}
		return append(bytes.Join(request.GetPatterns(), []byte("\n")), '\n'), nil
	})
	writer := streamio.NewWriter(func(p []byte) error {
		return stream.Send(&gitalypb.CreateBundleFromRefListResponse{Data: p})
	})

	err = repo.CreateBundle(ctx, writer, &localrepo.CreateBundleOpts{Patterns: patterns})
	switch {
	case errors.Is(err, localrepo.ErrEmptyBundle):
		return structerr.NewFailedPrecondition("%w", err)
	case err != nil:
		return structerr.NewInternal("%w", err)
	}

	return nil
}
