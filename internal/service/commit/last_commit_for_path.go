package commit

import (
	"context"
	"fmt"

	"gitlab.com/gitlab-org/gitaly/internal/git/catfile"
	"gitlab.com/gitlab-org/gitaly/internal/git/log"
	"gitlab.com/gitlab-org/gitaly/internal/helper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
)

func (s *server) LastCommitForPath(ctx context.Context, in *gitalypb.LastCommitForPathRequest) (*gitalypb.LastCommitForPathResponse, error) {
	if err := validateLastCommitForPathRequest(in); err != nil {
		return nil, helper.ErrInvalidArgument(err)
	}

	resp, err := lastCommitForPath(ctx, in)
	if err != nil {
		return nil, helper.ErrInternal(err)
	}

	return resp, nil
}

func lastCommitForPath(ctx context.Context, in *gitalypb.LastCommitForPathRequest) (*gitalypb.LastCommitForPathResponse, error) {
	path := string(in.GetPath())
	if len(path) == 0 || path == "/" {
		path = "."
	}

	repo := in.GetRepository()
	c, err := catfile.New(ctx, repo)
	if err != nil {
		return nil, err
	}

	commit, err := log.LastCommitForPath(ctx, c, repo, string(in.GetRevision()), path)
	if log.IsNotFound(err) {
		return &gitalypb.LastCommitForPathResponse{}, nil
	}

	return &gitalypb.LastCommitForPathResponse{Commit: commit}, err
}

func validateLastCommitForPathRequest(in *gitalypb.LastCommitForPathRequest) error {
	if len(in.Revision) == 0 {
		return fmt.Errorf("empty Revision")
	}
	return nil
}
