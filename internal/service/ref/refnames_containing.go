package ref

import (
	"bytes"
	"fmt"

	"gitlab.com/gitlab-org/gitaly-proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/helper"
	"gitlab.com/gitlab-org/gitaly/internal/helper/chunker"
)

// ListBranchNamesContainingCommit returns a maximum of in.GetLimit() Branch names
// which contain the SHA1 passed as argument
func (*server) ListBranchNamesContainingCommit(in *gitalypb.ListBranchNamesContainingCommitRequest, stream gitalypb.RefService_ListBranchNamesContainingCommitServer) error {
	if err := git.ValidateCommitID(in.GetCommitId()); err != nil {
		return helper.ErrInvalidArgument(err)
	}

	sender := chunker.New(&branchNamesContainingCommitSender{stream: stream})
	ctx := stream.Context()
	if err := listRefNames(ctx, sender, "refs/heads", in.Repository, containingArgs(in)); err != nil {
		return helper.ErrInternal(err)
	}

	return nil
}

type containingRequest interface {
	GetCommitId() string
	GetLimit() uint32
}

func containingArgs(req containingRequest) []string {
	args := []string{fmt.Sprintf("--contains=%s", req.GetCommitId())}
	if limit := req.GetLimit(); limit != 0 {
		args = append(args, fmt.Sprintf("--count=%d", limit))
	}
	return args
}

type branchNamesContainingCommitSender struct {
	stream      gitalypb.RefService_ListBranchNamesContainingCommitServer
	branchNames [][]byte
}

func (bs *branchNamesContainingCommitSender) Reset() { bs.branchNames = nil }
func (bs *branchNamesContainingCommitSender) Append(it chunker.Item) {
	bs.branchNames = append(bs.branchNames, stripPrefix(it, "refs/heads/"))
}
func (bs *branchNamesContainingCommitSender) Send() error {
	return bs.stream.Send(&gitalypb.ListBranchNamesContainingCommitResponse{BranchNames: bs.branchNames})
}

// ListTagNamesContainingCommit returns a maximum of in.GetLimit() Tag names
// which contain the SHA1 passed as argument
func (*server) ListTagNamesContainingCommit(in *gitalypb.ListTagNamesContainingCommitRequest, stream gitalypb.RefService_ListTagNamesContainingCommitServer) error {
	if err := git.ValidateCommitID(in.GetCommitId()); err != nil {
		return helper.ErrInvalidArgument(err)
	}

	sender := chunker.New(&tagNamesContainingCommitSender{stream: stream})
	ctx := stream.Context()
	if err := listRefNames(ctx, sender, "refs/tags", in.Repository, containingArgs(in)); err != nil {
		return helper.ErrInternal(err)
	}

	return nil
}

type tagNamesContainingCommitSender struct {
	stream   gitalypb.RefService_ListTagNamesContainingCommitServer
	tagNames [][]byte
}

func (ts *tagNamesContainingCommitSender) Reset() { ts.tagNames = nil }
func (ts *tagNamesContainingCommitSender) Append(it chunker.Item) {
	ts.tagNames = append(ts.tagNames, stripPrefix(it, "refs/tags/"))
}
func (ts *tagNamesContainingCommitSender) Send() error {
	return ts.stream.Send(&gitalypb.ListTagNamesContainingCommitResponse{TagNames: ts.tagNames})
}

func stripPrefix(it chunker.Item, prefix string) []byte {
	return bytes.TrimPrefix(it.([]byte), []byte(prefix))
}
