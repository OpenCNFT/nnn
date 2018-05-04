package repository

import (
	"bytes"
	"errors"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "gitlab.com/gitlab-org/gitaly-proto/go"
	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/helper"
	"gitlab.com/gitlab-org/gitaly/internal/helper/lines"
)

const surroundContext = "2"

var contentDelimiter = []byte("--\n")

func (s *server) SearchFilesByContent(req *pb.SearchFilesByContentRequest, stream pb.RepositoryService_SearchFilesByContentServer) error {
	if err := validateSearchFilesRequest(req); err != nil {
		return helper.DecorateError(codes.InvalidArgument, err)
	}
	repo := req.GetRepository()
	if repo == nil {
		return status.Errorf(codes.InvalidArgument, "SearchFilesByContent: empty Repository")
	}

	ctx := stream.Context()
	cmd, err := git.Command(ctx, repo, "grep",
		"--ignore-case",
		"-I", // Don't match binary, there is no long-name for this one
		"--line-number",
		"--null",
		"--before-context", surroundContext,
		"--after-context", surroundContext,
		"--extended-regexp",
		"-e", // next arg is pattern, keep this last
		req.GetQuery(),
		string(req.GetRef()),
	)
	if err != nil {
		return status.Errorf(codes.Internal, "SearchFilesByContent: cmd start failed: %v", err)
	}

	var (
		buf     []byte
		matches [][]byte
	)
	reader := func(objs [][]byte) error {
		for _, obj := range objs {
			obj = append(obj, '\n')
			if bytes.Compare(obj, contentDelimiter) == 0 {
				matches = append(matches, buf)
				buf = nil
			} else {
				buf = append(buf, obj...)
			}
		}
		if len(matches) > 1 {
			err = stream.Send(&pb.SearchFilesByContentResponse{Matches: matches})
			matches = nil
			return err
		}
		return nil
	}

	err = lines.Send(cmd, reader, []byte{'\n'})
	if err != nil {
		return helper.DecorateError(codes.Internal, err)
	}
	if len(buf) > 1 {
		return stream.Send(&pb.SearchFilesByContentResponse{Matches: [][]byte{buf}})
	}
	return nil
}

func (s *server) SearchFilesByName(req *pb.SearchFilesByNameRequest, stream pb.RepositoryService_SearchFilesByNameServer) error {
	if err := validateSearchFilesRequest(req); err != nil {
		return helper.DecorateError(codes.InvalidArgument, err)
	}
	repo := req.GetRepository()
	if repo == nil {
		return status.Errorf(codes.InvalidArgument, "SearchFilesByName: empty Repository")
	}

	ctx := stream.Context()
	cmd, err := git.Command(ctx, repo, "ls-tree", "--full-tree", "--name-status", "-r", string(req.GetRef()), req.GetQuery())
	if err != nil {
		return status.Errorf(codes.Internal, "SearchFilesByName: cmd start failed: %v", err)
	}

	lr := func(objs [][]byte) error {
		return stream.Send(&pb.SearchFilesByNameResponse{Files: objs})
	}

	return lines.Send(cmd, lr, []byte{'\n'})
}

type searchFilesRequest interface {
	GetRef() []byte
	GetQuery() string
}

func validateSearchFilesRequest(req searchFilesRequest) error {
	if len(req.GetQuery()) == 0 {
		return errors.New("no query given")
	}
	if len(req.GetRef()) == 0 {
		return errors.New("no ref given")
	}
	return nil
}
