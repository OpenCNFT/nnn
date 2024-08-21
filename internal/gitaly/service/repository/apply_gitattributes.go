package repository

import (
	"context"
	"errors"
	"fmt"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git"
	"gitlab.com/gitlab-org/gitaly/v16/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v16/internal/transaction/txinfo"
	"gitlab.com/gitlab-org/gitaly/v16/internal/transaction/voting"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
)

func (s *server) ApplyGitattributes(ctx context.Context, in *gitalypb.ApplyGitattributesRequest) (*gitalypb.ApplyGitattributesResponse, error) {
	// In git 2.43.0+, gitattributes supports reading from HEAD:.gitattributes,
	// so info/attributes is no longer needed. Besides that, info/attributes file needs to
	// be cleaned because it has a higher precedence
	// than HEAD:.gitattributes. We want to avoid HEAD:.gitattributes being
	// overridden.
	//
	// To make sure info/attributes file is cleaned up,
	// we delete it if it exists when reading from HEAD:.gitattributes is called.
	// This logic can be removed when ApplyGitattributes and GetInfoAttributes RPC are totally removed from
	// the code base.
	repository := in.GetRepository()
	if err := s.locator.ValidateRepository(repository); err != nil {
		return nil, structerr.NewInvalidArgument("validate repo error: %w", err)
	}
	repoPath, err := s.locator.GetRepoPath(repository)
	if err != nil {
		return nil, structerr.NewInternal("find repo path: %w", err).WithMetadata("path", repoPath)
	}

	hash, err := s.localrepo(repository).ObjectHash(ctx)
	if err != nil {
		return nil, structerr.NewInternal("get object hash: %w", err).WithMetadata("path", repoPath)
	}

	// We use the zero OID as placeholder to vote on removal of the
	// gitattributes file. Omitting this will cause praefect doesn't receive votes
	// and considers all secondaries are outdated.
	if err := s.vote(ctx, hash.ZeroOID, voting.Prepared); err != nil {
		return nil, fmt.Errorf("preimage vote: %w", err)
	}
	if deletionErr := deleteInfoAttributesFile(repoPath); deletionErr != nil {
		return nil, structerr.NewInternal("delete info/gitattributes file: %w", deletionErr).WithMetadata("path", repoPath)
	}
	if err := s.vote(ctx, hash.ZeroOID, voting.Committed); err != nil {
		return nil, fmt.Errorf("postimage vote: %w", err)
	}

	// Once git 2.43.0 is deployed, we can stop using info/attributes in related RPCs,
	// As a result, ApplyGitattributes() is made as a no-op,
	// so that Gitaly clients will stop writing to info/attributes.
	// This gRPC will be totally removed in the once all the housekeeping on removing info/attributes is done.
	return &gitalypb.ApplyGitattributesResponse{}, nil
}

func (s *server) vote(ctx context.Context, oid git.ObjectID, phase voting.Phase) error {
	tx, err := txinfo.TransactionFromContext(ctx)
	if errors.Is(err, txinfo.ErrTransactionNotFound) {
		return nil
	}

	hash, err := oid.Bytes()
	if err != nil {
		return fmt.Errorf("vote with invalid object ID: %w", err)
	}

	vote, err := voting.VoteFromHash(hash)
	if err != nil {
		return fmt.Errorf("cannot convert OID to vote: %w", err)
	}

	if err := s.txManager.Vote(ctx, tx, vote, phase); err != nil {
		return fmt.Errorf("vote failed: %w", err)
	}

	return nil
}
