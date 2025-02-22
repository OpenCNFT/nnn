package operations

import (
	"context"
	"errors"
	"fmt"

	"gitlab.com/gitlab-org/gitaly/v16/internal/git"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/hook/updateref"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v16/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
)

func validateUserCreateBranchRequest(ctx context.Context, locator storage.Locator, in *gitalypb.UserCreateBranchRequest) error {
	if err := locator.ValidateRepository(ctx, in.GetRepository()); err != nil {
		return err
	}
	if len(in.GetBranchName()) == 0 {
		return errors.New("empty branch name")
	}
	if in.GetUser() == nil {
		return errors.New("empty user")
	}
	if len(in.GetStartPoint()) == 0 {
		return errors.New("empty start point")
	}
	return nil
}

// UserCreateBranch creates a single branch in the context of a specific user. It executes
// hooks and contacts Rails to verify that the user is allowed to create the branch.
func (s *Server) UserCreateBranch(ctx context.Context, req *gitalypb.UserCreateBranchRequest) (*gitalypb.UserCreateBranchResponse, error) {
	if err := validateUserCreateBranchRequest(ctx, s.locator, req); err != nil {
		return nil, structerr.NewInvalidArgument("%w", err)
	}
	quarantineDir, quarantineRepo, err := s.quarantinedRepo(ctx, req.GetRepository())
	if err != nil {
		return nil, err
	}

	// BEGIN TODO: Uncomment if StartPoint started behaving sensibly
	// like BranchName. See
	// https://gitlab.com/gitlab-org/gitaly/-/issues/3331
	//
	// startPointReference, err := s.localrepo(req.GetRepository()).GetReference(ctx, "refs/heads/"+string(req.StartPoint))
	// startPointCommit, err := log.GetCommit(ctx, req.Repository, startPointReference.Target)
	startPointCommit, err := quarantineRepo.ReadCommit(ctx, git.Revision(req.GetStartPoint()))
	// END TODO
	if err != nil {
		return nil, structerr.NewFailedPrecondition("revspec '%s' not found", req.GetStartPoint())
	}

	objectHash, err := quarantineRepo.ObjectHash(ctx)
	if err != nil {
		return nil, fmt.Errorf("detecting object hash: %w", err)
	}

	startPointOID, err := objectHash.FromHex(startPointCommit.GetId())
	if err != nil {
		return nil, structerr.NewInvalidArgument("could not parse start point commit ID: %w", err)
	}

	referenceName := git.NewReferenceNameFromBranchName(string(req.GetBranchName()))

	if err := s.updateReferenceWithHooks(ctx, req.GetRepository(), req.GetUser(), quarantineDir, referenceName, startPointOID, objectHash.ZeroOID); err != nil {
		var customHookErr updateref.CustomHookError

		if errors.As(err, &customHookErr) {
			return nil, structerr.NewPermissionDenied("creation denied by custom hooks: %w", err).WithDetail(
				&gitalypb.UserCreateBranchError{
					Error: &gitalypb.UserCreateBranchError_CustomHook{
						CustomHook: customHookErr.Proto(),
					},
				},
			)
		}

		var updateRefError updateref.Error
		if errors.As(err, &updateRefError) {
			return nil, structerr.NewFailedPrecondition("%w", err)
		}

		return nil, err
	}

	return &gitalypb.UserCreateBranchResponse{
		Branch: &gitalypb.Branch{
			Name:         req.GetBranchName(),
			TargetCommit: startPointCommit,
		},
	}, nil
}
