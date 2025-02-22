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

func validateUserUpdateBranchGo(ctx context.Context, locator storage.Locator, req *gitalypb.UserUpdateBranchRequest) error {
	if err := locator.ValidateRepository(ctx, req.GetRepository()); err != nil {
		return err
	}

	if req.GetUser() == nil {
		return errors.New("empty user")
	}

	if len(req.GetBranchName()) == 0 {
		return errors.New("empty branch name")
	}

	if len(req.GetOldrev()) == 0 {
		return errors.New("empty oldrev")
	}

	if len(req.GetNewrev()) == 0 {
		return errors.New("empty newrev")
	}

	return nil
}

// UserUpdateBranch updates a branch to point to a new revision. It executes hooks and
// contacts Rails to verify that the user is allowed to update the branch.
func (s *Server) UserUpdateBranch(ctx context.Context, req *gitalypb.UserUpdateBranchRequest) (*gitalypb.UserUpdateBranchResponse, error) {
	// Validate the request
	if err := validateUserUpdateBranchGo(ctx, s.locator, req); err != nil {
		return nil, structerr.NewInvalidArgument("%w", err)
	}

	repo := s.localrepo(req.GetRepository())

	objectHash, err := repo.ObjectHash(ctx)
	if err != nil {
		return nil, fmt.Errorf("detecting object hash: %w", err)
	}

	newOID, err := objectHash.FromHex(string(req.GetNewrev()))
	if err != nil {
		return nil, structerr.NewInternal("could not parse newrev: %w", err)
	}

	oldOID, err := objectHash.FromHex(string(req.GetOldrev()))
	if err != nil {
		return nil, structerr.NewInternal("could not parse oldrev: %w", err)
	}

	referenceName := git.NewReferenceNameFromBranchName(string(req.GetBranchName()))

	quarantineDir, _, err := s.quarantinedRepo(ctx, req.GetRepository())
	if err != nil {
		return nil, err
	}

	if err := s.updateReferenceWithHooks(ctx, req.GetRepository(), req.GetUser(), quarantineDir, referenceName, newOID, oldOID); err != nil {
		var customHookErr updateref.CustomHookError
		if errors.As(err, &customHookErr) {
			return &gitalypb.UserUpdateBranchResponse{
				PreReceiveError: customHookErr.Error(),
			}, nil
		}

		// An oddball response for compatibility with the old
		// Ruby code. The "Could not update..."  message is
		// exactly like the default updateRefError, except we
		// say "branch-name", not
		// "refs/heads/branch-name". See the
		// "Gitlab::Git::CommitError" case in the Ruby code.
		return nil, structerr.NewFailedPrecondition("Could not update %s. Please refresh and try again.", req.GetBranchName())
	}

	return &gitalypb.UserUpdateBranchResponse{}, nil
}
