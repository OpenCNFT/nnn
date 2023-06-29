package repository

import (
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/repoutil"
	"gitlab.com/gitlab-org/gitaly/v16/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/v16/streamio"
)

// GetCustomHooks fetches the git hooks for a repository. The hooks are sent in
// a tar archive containing a `custom_hooks` directory. If no hooks are present
// in the repository, the response will have no data.
func (s *server) GetCustomHooks(in *gitalypb.GetCustomHooksRequest, stream gitalypb.RepositoryService_GetCustomHooksServer) error {
	ctx := stream.Context()

	if err := s.locator.ValidateRepository(in.GetRepository()); err != nil {
		return structerr.NewInvalidArgument("%w", err)
	}

	writer := streamio.NewWriter(func(p []byte) error {
		return stream.Send(&gitalypb.GetCustomHooksResponse{Data: p})
	})

	if err := repoutil.GetCustomHooks(ctx, s.locator, writer, in.Repository); err != nil {
		return structerr.NewInternal("reading custom hooks: %w", err)
	}

	return nil
}

// BackupCustomHooks fetches the git hooks for a repository. The hooks are sent
// in a tar archive containing a `custom_hooks` directory. If no hooks are
// present in the repository, the response will have no data.
func (s *server) BackupCustomHooks(in *gitalypb.BackupCustomHooksRequest, stream gitalypb.RepositoryService_BackupCustomHooksServer) error {
	ctx := stream.Context()

	if err := s.locator.ValidateRepository(in.GetRepository()); err != nil {
		return structerr.NewInvalidArgument("%w", err)
	}

	writer := streamio.NewWriter(func(p []byte) error {
		return stream.Send(&gitalypb.BackupCustomHooksResponse{Data: p})
	})

	if err := repoutil.GetCustomHooks(ctx, s.locator, writer, in.Repository); err != nil {
		return structerr.NewInternal("reading custom hooks: %w", err)
	}

	return nil
}
