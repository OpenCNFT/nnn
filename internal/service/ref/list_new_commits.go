package ref

import (
	"bufio"
	"regexp"
	"strings"

	pb "gitlab.com/gitlab-org/gitaly-proto/go"
	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/git/catfile"
	"gitlab.com/gitlab-org/gitaly/internal/git/log"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *server) ListNewCommits(in *pb.ListNewCommitsRequest, stream pb.RefService_ListNewCommitsServer) error {
	oid := in.GetCommitId()
	if match, err := regexp.MatchString(`\A[0-9a-f]{40}\z`, oid); !match || err != nil {
		return status.Errorf(codes.InvalidArgument, "commit id shoud have 40 hexidecimal characters")
	}

	ctx := stream.Context()

	revList, err := git.Command(ctx, in.GetRepository(), "rev-list", oid, "--not", "--all")
	if err != nil {
		if _, ok := status.FromError(err); ok {
			return err
		}
		return status.Errorf(codes.Internal, "ListNewCommits: gitCommand: %v", err)
	}

	batch, err := catfile.New(ctx, in.GetRepository())
	if err != nil {
		return status.Errorf(codes.Internal, "ListNewCommits: catfile: %v", err)
	}

	i := 0
	commits := []*pb.GitCommit{}
	scanner := bufio.NewScanner(revList)
	for scanner.Scan() {
		line := scanner.Text()

		commit, err := log.GetCommitCatfile(batch, strings.TrimSpace(line))
		if err != nil {
			return status.Errorf(codes.Internal, "ListNewCommits: commit not found: %v", err)
		}
		commits = append(commits, commit)

		if i%10 == 0 {
			response := &pb.ListNewCommitsResponse{Commits: commits}
			stream.Send(response)
			commits = commits[:0]
		}
	}

	response := &pb.ListNewCommitsResponse{Commits: commits}
	stream.Send(response)

	return revList.Wait()
}
