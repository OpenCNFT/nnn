package operations

import (
	"context"
	"errors"
	"fmt"

	"gitlab.com/gitlab-org/gitaly/v16/internal/git"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/gitcmd"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/remoterepo"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/hook/updateref"
	"gitlab.com/gitlab-org/gitaly/v16/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
)

//nolint:revive // This is unintentionally missing documentation.
func (s *Server) UserRevert(ctx context.Context, req *gitalypb.UserRevertRequest) (*gitalypb.UserRevertResponse, error) {
	if err := validateCherryPickOrRevertRequest(ctx, s.locator, req); err != nil {
		return nil, structerr.NewInvalidArgument("%w", err)
	}

	quarantineDir, quarantineRepo, err := s.quarantinedRepo(ctx, req.GetRepository())
	if err != nil {
		return nil, err
	}

	startRevision, err := s.fetchStartRevision(ctx, quarantineRepo, req)
	if err != nil {
		return nil, err
	}

	repoHadBranches, err := quarantineRepo.HasBranches(ctx)
	if err != nil {
		return nil, structerr.NewInternal("has branches: %w", err)
	}

	committerSignature, err := git.SignatureFromRequest(req)
	if err != nil {
		return nil, structerr.NewInvalidArgument("%w", err)
	}

	var newrev git.ObjectID

	oursCommit, err := quarantineRepo.ReadCommit(ctx, startRevision.Revision())
	if err != nil {
		if errors.Is(err, localrepo.ErrObjectNotFound) {
			return nil, structerr.NewNotFound("ours commit lookup: commit not found").
				WithMetadata("revision", startRevision)
		}

		return nil, structerr.NewInternal("read commit: %w", err)
	}
	revertCommit, err := quarantineRepo.ReadCommit(ctx, git.Revision(req.GetCommit().GetId()))
	if err != nil {
		if errors.Is(err, localrepo.ErrObjectNotFound) {
			return nil, structerr.NewNotFound("revert commit lookup: commit not found").
				WithMetadata("revision", req.GetCommit().GetId())
		}

		return nil, structerr.NewInternal("read commit: %w", err)
	}

	var theirs git.ObjectID
	if len(revertCommit.GetParentIds()) > 0 {
		// Use the first parent as `theirs` to implement mainline = 1.
		theirs = git.ObjectID(revertCommit.GetParentIds()[0])
	} else {
		// It is a root commit, use empty tree oid as `theirs`.
		hash, err := quarantineRepo.ObjectHash(ctx)
		if err != nil {
			return nil, structerr.NewInternal("detecting object hash: %w", err)
		}
		theirs = hash.EmptyTreeOID
	}

	// We "merge" in the "changes" from parent to child, which would apply the "opposite"
	// patch to "ours", thus reverting the commit.
	treeOID, err := quarantineRepo.MergeTree(
		ctx,
		oursCommit.GetId(),
		theirs.String(),
		localrepo.WithMergeBase(git.Revision(revertCommit.GetId())),
		localrepo.WithConflictingFileNamesOnly(),
	)
	if err != nil {
		var conflictErr *localrepo.MergeTreeConflictError
		if errors.As(err, &conflictErr) {
			conflictingFiles := make([][]byte, 0, len(conflictErr.ConflictingFileInfo))
			for _, conflictingFileInfo := range conflictErr.ConflictingFileInfo {
				conflictingFiles = append(conflictingFiles, []byte(conflictingFileInfo.FileName))
			}
			return nil, structerr.NewFailedPrecondition("revert: there are conflicting files").WithDetail(
				&gitalypb.UserRevertError{
					Error: &gitalypb.UserRevertError_MergeConflict{
						MergeConflict: &gitalypb.MergeConflictError{
							ConflictingFiles: conflictingFiles,
						},
					},
				})
		}

		return nil, structerr.NewInternal("merge-tree: %w", err)
	}

	if oursCommit.GetTreeId() == treeOID.String() {
		return nil, structerr.NewFailedPrecondition("revert: could not apply because the result was empty").WithDetail(
			&gitalypb.UserRevertError{
				Error: &gitalypb.UserRevertError_ChangesAlreadyApplied{
					ChangesAlreadyApplied: &gitalypb.ChangesAlreadyAppliedError{},
				},
			})
	}

	newrev, err = quarantineRepo.WriteCommit(
		ctx,
		localrepo.WriteCommitConfig{
			TreeID:         treeOID,
			Message:        string(req.GetMessage()),
			Parents:        []git.ObjectID{startRevision},
			AuthorName:     committerSignature.Name,
			AuthorEmail:    committerSignature.Email,
			AuthorDate:     committerSignature.When,
			CommitterName:  committerSignature.Name,
			CommitterEmail: committerSignature.Email,
			CommitterDate:  committerSignature.When,
			GitConfig:      s.gitConfig,
			Sign:           true,
		},
	)
	if err != nil {
		return nil, structerr.NewInternal("write commit: %w", err)
	}

	referenceName := git.NewReferenceNameFromBranchName(string(req.GetBranchName()))
	branchCreated := false
	var oldrev git.ObjectID

	objectHash, err := quarantineRepo.ObjectHash(ctx)
	if err != nil {
		return nil, structerr.NewInternal("detecting object hash: %w", err)
	}

	if expectedOldOID := req.GetExpectedOldOid(); expectedOldOID != "" {
		oldrev, err = objectHash.FromHex(expectedOldOID)
		if err != nil {
			return nil, structerr.NewInvalidArgument("invalid expected old object ID: %w", err).WithMetadata("old_object_id", expectedOldOID)
		}

		oldrev, err = quarantineRepo.ResolveRevision(
			ctx, git.Revision(fmt.Sprintf("%s^{object}", oldrev)),
		)
		if err != nil {
			return nil, structerr.NewInvalidArgument("cannot resolve expected old object ID: %w", err).
				WithMetadata("old_object_id", expectedOldOID)
		}
	} else {
		oldrev, err = quarantineRepo.ResolveRevision(ctx, referenceName.Revision()+"^{commit}")
		if errors.Is(err, git.ErrReferenceNotFound) {
			branchCreated = true
			oldrev = objectHash.ZeroOID
		} else if err != nil {
			return nil, structerr.NewInvalidArgument("resolve ref: %w", err)
		}
	}

	if req.GetDryRun() {
		newrev = startRevision
	}

	if !branchCreated {
		ancestor, err := quarantineRepo.IsAncestor(ctx, oldrev.Revision(), newrev.Revision())
		if err != nil {
			return nil, structerr.NewInternal("checking for ancestry: %w", err)
		}
		if !ancestor {
			return nil, structerr.NewFailedPrecondition("revert: branch diverged").WithDetail(
				&gitalypb.UserRevertError{
					Error: &gitalypb.UserRevertError_NotAncestor{
						NotAncestor: &gitalypb.NotAncestorError{
							ParentRevision: []byte(oldrev.Revision()),
							ChildRevision:  []byte(newrev.Revision()),
						},
					},
				})
		}
	}

	if err := s.updateReferenceWithHooks(ctx, req.GetRepository(), req.GetUser(), quarantineDir, referenceName, newrev, oldrev); err != nil {
		var customHookErr updateref.CustomHookError
		if errors.As(err, &customHookErr) {
			return nil, structerr.NewPermissionDenied("revert: custom hook error").WithDetail(
				&gitalypb.UserRevertError{
					Error: &gitalypb.UserRevertError_CustomHook{
						CustomHook: customHookErr.Proto(),
					},
				})
		}

		return nil, fmt.Errorf("update reference with hooks: %w", err)
	}

	return &gitalypb.UserRevertResponse{
		BranchUpdate: &gitalypb.OperationBranchUpdate{
			CommitId:      newrev.String(),
			BranchCreated: branchCreated,
			RepoCreated:   !repoHadBranches,
		},
	}, nil
}

type requestFetchingStartRevision interface {
	GetBranchName() []byte
	GetStartRepository() *gitalypb.Repository
	GetStartBranchName() []byte
}

func (s *Server) fetchStartRevision(
	ctx context.Context,
	localRepo *localrepo.Repo,
	req requestFetchingStartRevision,
) (git.ObjectID, error) {
	startBranchName := req.GetStartBranchName()
	if len(startBranchName) == 0 {
		startBranchName = req.GetBranchName()
	}

	var remoteRepo gitcmd.Repository = localRepo
	if startRepository := req.GetStartRepository(); startRepository != nil {
		var err error
		remoteRepo, err = remoterepo.New(ctx, startRepository, s.conns)
		if err != nil {
			return "", structerr.NewInternal("%w", err)
		}
	}

	startRevision, err := remoteRepo.ResolveRevision(ctx, git.Revision(fmt.Sprintf("%s^{commit}", startBranchName)))
	if err != nil {
		return "", structerr.NewInvalidArgument("resolve start ref: %w", err)
	}

	if req.GetStartRepository() == nil {
		return startRevision, nil
	}

	_, err = localRepo.ResolveRevision(ctx, startRevision.Revision()+"^{commit}")
	if errors.Is(err, git.ErrReferenceNotFound) {
		if err := localRepo.FetchInternal(
			ctx,
			req.GetStartRepository(),
			[]string{startRevision.String()},
			localrepo.FetchOpts{Tags: localrepo.FetchOptsTagsNone},
		); err != nil {
			return "", structerr.NewInternal("fetch start: %w", err)
		}
	} else if err != nil {
		return "", structerr.NewInvalidArgument("resolve start: %w", err)
	}

	return startRevision, nil
}
