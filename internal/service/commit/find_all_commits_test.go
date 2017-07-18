package commit

import (
	"io"
	"testing"

	"gitlab.com/gitlab-org/gitaly/internal/service/ref"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"

	pb "gitlab.com/gitlab-org/gitaly-proto/go"

	"github.com/golang/protobuf/ptypes/timestamp"
	"github.com/stretchr/testify/require"
	"golang.org/x/net/context"
	"google.golang.org/grpc/codes"
)

func TestSuccessfulFindAllCommitsRequest(t *testing.T) {
	defer func() {
		_findBranchNamesFunc = ref.FindBranchNames
	}()

	_findBranchNamesFunc = func(repoPath string) ([][]byte, error) {
		return [][]byte{
			[]byte("few-commits"),
			[]byte("two-commits"),
		}, nil
	}

	client := newCommitServiceClient(t)

	// Commits made on another branch in parallel to the normal commits below.
	// Will be used to test topology ordering.
	alternateCommits := []*pb.GitCommit{
		{
			Id:        "0031876facac3f2b2702a0e53a26e89939a42209",
			Subject:   []byte("Merge branch 'few-commits-4' into few-commits-2"),
			Body:      []byte("Merge branch 'few-commits-4' into few-commits-2\n"),
			Author:    dummyCommitAuthor(1500320762),
			Committer: dummyCommitAuthor(1500320762),
			ParentIds: []string{
				"bf6e164cac2dc32b1f391ca4290badcbe4ffc5fb",
				"48ca272b947f49eee601639d743784a176574a09",
			},
		},
		{
			Id:        "48ca272b947f49eee601639d743784a176574a09",
			Subject:   []byte("Commit #9 alternate"),
			Body:      []byte("Commit #9 alternate\n"),
			Author:    dummyCommitAuthor(1500320271),
			Committer: dummyCommitAuthor(1500320271),
			ParentIds: []string{"335bc94d5b7369b10251e612158da2e4a4aaa2a5"},
		},
		{
			Id:        "335bc94d5b7369b10251e612158da2e4a4aaa2a5",
			Subject:   []byte("Commit #8 alternate"),
			Body:      []byte("Commit #8 alternate\n"),
			Author:    dummyCommitAuthor(1500320269),
			Committer: dummyCommitAuthor(1500320269),
			ParentIds: []string{"1039376155a0d507eba0ea95c29f8f5b983ea34b"},
		},
	}

	// Nothing special about these commits.
	normalCommits := []*pb.GitCommit{
		{
			Id:        "bf6e164cac2dc32b1f391ca4290badcbe4ffc5fb",
			Subject:   []byte("Commit #10"),
			Body:      []byte("Commit #10\n"),
			Author:    dummyCommitAuthor(1500320272),
			Committer: dummyCommitAuthor(1500320272),
			ParentIds: []string{"9d526f87b82e2b2fd231ca44c95508e5e85624ca"},
		},
		{
			Id:        "9d526f87b82e2b2fd231ca44c95508e5e85624ca",
			Subject:   []byte("Commit #9"),
			Body:      []byte("Commit #9\n"),
			Author:    dummyCommitAuthor(1500320270),
			Committer: dummyCommitAuthor(1500320270),
			ParentIds: []string{"1039376155a0d507eba0ea95c29f8f5b983ea34b"},
		},
		{
			Id:        "1039376155a0d507eba0ea95c29f8f5b983ea34b",
			Subject:   []byte("Commit #8"),
			Body:      []byte("Commit #8\n"),
			Author:    dummyCommitAuthor(1500320268),
			Committer: dummyCommitAuthor(1500320268),
			ParentIds: []string{"54188278422b1fa877c2e71c4e37fc6640a58ad1"},
		}, {
			Id:        "54188278422b1fa877c2e71c4e37fc6640a58ad1",
			Subject:   []byte("Commit #7"),
			Body:      []byte("Commit #7\n"),
			Author:    dummyCommitAuthor(1500320266),
			Committer: dummyCommitAuthor(1500320266),
			ParentIds: []string{"8b9270332688d58e25206601900ee5618fab2390"},
		}, {
			Id:        "8b9270332688d58e25206601900ee5618fab2390",
			Subject:   []byte("Commit #6"),
			Body:      []byte("Commit #6\n"),
			Author:    dummyCommitAuthor(1500320264),
			Committer: dummyCommitAuthor(1500320264),
			ParentIds: []string{"f9220df47bce1530e90c189064d301bfc8ceb5ab"},
		}, {
			Id:        "f9220df47bce1530e90c189064d301bfc8ceb5ab",
			Subject:   []byte("Commit #5"),
			Body:      []byte("Commit #5\n"),
			Author:    dummyCommitAuthor(1500320262),
			Committer: dummyCommitAuthor(1500320262),
			ParentIds: []string{"40d408f89c1fd26b7d02e891568f880afe06a9f8"},
		}, {
			Id:        "40d408f89c1fd26b7d02e891568f880afe06a9f8",
			Subject:   []byte("Commit #4"),
			Body:      []byte("Commit #4\n"),
			Author:    dummyCommitAuthor(1500320260),
			Committer: dummyCommitAuthor(1500320260),
			ParentIds: []string{"df914c609a1e16d7d68e4a61777ff5d6f6b6fde3"},
		}, {
			Id:        "df914c609a1e16d7d68e4a61777ff5d6f6b6fde3",
			Subject:   []byte("Commit #3"),
			Body:      []byte("Commit #3\n"),
			Author:    dummyCommitAuthor(1500320258),
			Committer: dummyCommitAuthor(1500320258),
			ParentIds: []string{"6762605237fc246ae146ac64ecb467f71d609120"},
		}, {
			Id:        "6762605237fc246ae146ac64ecb467f71d609120",
			Subject:   []byte("Commit #2"),
			Body:      []byte("Commit #2\n"),
			Author:    dummyCommitAuthor(1500320256),
			Committer: dummyCommitAuthor(1500320256),
			ParentIds: []string{"79b06233d3dc769921576771a4e8bee4b439595d"},
		}, {
			Id:        "79b06233d3dc769921576771a4e8bee4b439595d",
			Subject:   []byte("Commit #1"),
			Body:      []byte("Commit #1\n"),
			Author:    dummyCommitAuthor(1500320254),
			Committer: dummyCommitAuthor(1500320254),
			ParentIds: []string{"1a0b36b3cdad1d2ee32457c102a8c0b7056fa863"},
		},
		{
			Id:      "1a0b36b3cdad1d2ee32457c102a8c0b7056fa863",
			Subject: []byte("Initial commit"),
			Body:    []byte("Initial commit\n"),
			Author: &pb.CommitAuthor{
				Name:  []byte("Dmitriy Zaporozhets"),
				Email: []byte("dmitriy.zaporozhets@gmail.com"),
				Date:  &timestamp.Timestamp{Seconds: 1393488198},
			},
			Committer: &pb.CommitAuthor{
				Name:  []byte("Dmitriy Zaporozhets"),
				Email: []byte("dmitriy.zaporozhets@gmail.com"),
				Date:  &timestamp.Timestamp{Seconds: 1393488198},
			},
			ParentIds: []string{""},
		},
	}

	// A commit that exists on "two-commits" branch.
	singleCommit := []*pb.GitCommit{
		{
			Id:        "304d257dcb821665ab5110318fc58a007bd104ed",
			Subject:   []byte("Commit #11"),
			Body:      []byte("Commit #11\n"),
			Author:    dummyCommitAuthor(1500322381),
			Committer: dummyCommitAuthor(1500322381),
			ParentIds: []string{"1a0b36b3cdad1d2ee32457c102a8c0b7056fa863"},
		},
	}

	timeOrderedCommits := []*pb.GitCommit{
		alternateCommits[0], normalCommits[0],
		alternateCommits[1], normalCommits[1],
		alternateCommits[2],
	}
	timeOrderedCommits = append(timeOrderedCommits, normalCommits[2:]...)
	topoOrderedCommits := append(alternateCommits, normalCommits...)

	testCases := []struct {
		desc            string
		request         *pb.FindAllCommitsRequest
		expectedCommits []*pb.GitCommit
	}{
		{
			desc: "all commits of a revision",
			request: &pb.FindAllCommitsRequest{
				Revision: []byte("few-commits"),
			},
			expectedCommits: timeOrderedCommits,
		},
		{
			desc: "maximum number of commits of a revision",
			request: &pb.FindAllCommitsRequest{
				MaxCount: 5,
				Revision: []byte("few-commits"),
			},
			expectedCommits: timeOrderedCommits[:5],
		},
		{
			desc: "skipping number of commits of a revision",
			request: &pb.FindAllCommitsRequest{
				Skip:     5,
				Revision: []byte("few-commits"),
			},
			expectedCommits: timeOrderedCommits[5:],
		},
		{
			desc: "maximum number of commits of a revision plus skipping",
			request: &pb.FindAllCommitsRequest{
				Skip:     5,
				MaxCount: 2,
				Revision: []byte("few-commits"),
			},
			expectedCommits: timeOrderedCommits[5:7],
		},
		{
			desc: "all commits of a revision ordered by date",
			request: &pb.FindAllCommitsRequest{
				Revision: []byte("few-commits"),
				Order:    pb.FindAllCommitsRequest_DATE,
			},
			expectedCommits: timeOrderedCommits,
		},
		{
			desc: "all commits of a revision ordered by topology",
			request: &pb.FindAllCommitsRequest{
				Revision: []byte("few-commits"),
				Order:    pb.FindAllCommitsRequest_TOPO,
			},
			expectedCommits: topoOrderedCommits,
		},
		{
			desc:            "all commits of all branches",
			request:         &pb.FindAllCommitsRequest{},
			expectedCommits: append(singleCommit, timeOrderedCommits...),
		},
		{
			desc:            "non-existing revision",
			request:         &pb.FindAllCommitsRequest{Revision: []byte("i-do-not-exist")},
			expectedCommits: []*pb.GitCommit{},
		},
	}

	for _, testCase := range testCases {
		t.Logf("test case: %v", testCase.desc)

		request := testCase.request
		request.Repository = testRepo

		c, err := client.FindAllCommits(context.Background(), request)
		if err != nil {
			t.Fatal(err)
		}

		receivedCommits := []*pb.GitCommit{}
		for {
			resp, err := c.Recv()
			if err == io.EOF {
				break
			} else if err != nil {
				t.Fatal(err)
			}

			receivedCommits = append(receivedCommits, resp.GetCommits()...)
		}

		require.Equal(t, len(testCase.expectedCommits), len(receivedCommits), "number of commits received")

		for i, receivedCommit := range receivedCommits {
			if !testhelper.CommitsEqual(receivedCommit, testCase.expectedCommits[i]) {
				t.Fatalf("Expected commit\n%v\ngot\n%v", testCase.expectedCommits[i], receivedCommit)
			}
		}
	}
}

func TestFailedFindAllCommitsRequest(t *testing.T) {
	client := newCommitServiceClient(t)
	invalidRepo := &pb.Repository{StorageName: "fake", RelativePath: "path"}

	testCases := []struct {
		desc    string
		request *pb.FindAllCommitsRequest
		code    codes.Code
	}{
		{
			desc:    "Invalid repository",
			request: &pb.FindAllCommitsRequest{Repository: invalidRepo},
			code:    codes.InvalidArgument,
		},
		{
			desc:    "Repository is nil",
			request: &pb.FindAllCommitsRequest{},
			code:    codes.InvalidArgument,
		},
	}

	for _, testCase := range testCases {
		t.Logf("test case: %v", testCase.desc)

		c, err := client.FindAllCommits(context.Background(), testCase.request)
		if err != nil {
			t.Fatal(err)
		}

		err = drainFindAllCommitsResponse(c)
		testhelper.AssertGrpcError(t, err, testCase.code, "")
	}
}

func dummyCommitAuthor(ts int64) *pb.CommitAuthor {
	return &pb.CommitAuthor{
		Name:  []byte("Ahmad Sherif"),
		Email: []byte("ahmad+gitlab-test@gitlab.com"),
		Date:  &timestamp.Timestamp{Seconds: ts},
	}
}

func drainFindAllCommitsResponse(c pb.CommitService_FindAllCommitsClient) error {
	var err error
	for err == nil {
		_, err = c.Recv()
	}
	return err
}
