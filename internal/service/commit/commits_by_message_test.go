package commit

import (
	"io"
	"testing"

	"gitlab.com/gitlab-org/gitaly/internal/testhelper"

	pb "gitlab.com/gitlab-org/gitaly-proto/go"

	"github.com/golang/protobuf/ptypes/timestamp"
	"github.com/stretchr/testify/require"
	"golang.org/x/net/context"
	"google.golang.org/grpc/codes"
)

func TestSuccessfulCommitsByMessageRequest(t *testing.T) {
	service, ruby, serverSocketPath := startTestServices(t)
	defer stopTestServices(service, ruby)

	client := newCommitServiceClient(t, serverSocketPath)

	commits := []*pb.GitCommit{
		{
			Id:        "bf6e164cac2dc32b1f391ca4290badcbe4ffc5fb",
			Subject:   []byte("Commit #10"),
			Body:      []byte("Commit #10\n"),
			Author:    dummyCommitAuthor(1500320272),
			Committer: dummyCommitAuthor(1500320272),
			ParentIds: []string{"9d526f87b82e2b2fd231ca44c95508e5e85624ca"},
		},
		{
			Id:        "79b06233d3dc769921576771a4e8bee4b439595d",
			Subject:   []byte("Commit #1"),
			Body:      []byte("Commit #1\n"),
			Author:    dummyCommitAuthor(1500320254),
			Committer: dummyCommitAuthor(1500320254),
			ParentIds: []string{"1a0b36b3cdad1d2ee32457c102a8c0b7056fa863"},
		},
	}

	testCases := []struct {
		desc            string
		request         *pb.CommitsByMessageRequest
		expectedCommits []*pb.GitCommit
	}{
		{
			desc: "revision + query",
			request: &pb.CommitsByMessageRequest{
				Revision: []byte("few-commits"),
				Query:    "commit #1",
			},
			expectedCommits: commits,
		},
		{
			desc: "revision + query + limit",
			request: &pb.CommitsByMessageRequest{
				Revision: []byte("few-commits"),
				Query:    "commit #1",
				Limit:    1,
			},
			expectedCommits: commits[0:1],
		},
		{
			desc: "revision + query + offset",
			request: &pb.CommitsByMessageRequest{
				Revision: []byte("few-commits"),
				Query:    "commit #1",
				Offset:   1,
			},
			expectedCommits: commits[1:],
		},
		{
			desc: "query + empty revision + path",
			request: &pb.CommitsByMessageRequest{
				Query: "much more",
				Path:  []byte("files/ruby"),
			},
			expectedCommits: []*pb.GitCommit{
				{
					Id:      "913c66a37b4a45b9769037c55c2d238bd0942d2e",
					Subject: []byte("Files, encoding and much more"),
					Body:    []byte("Files, encoding and much more\n\nSigned-off-by: Dmitriy Zaporozhets <dmitriy.zaporozhets@gmail.com>\n"),
					Author: &pb.CommitAuthor{
						Name:  []byte("Dmitriy Zaporozhets"),
						Email: []byte("dmitriy.zaporozhets@gmail.com"),
						Date:  &timestamp.Timestamp{Seconds: 1393488896},
					},
					Committer: &pb.CommitAuthor{
						Name:  []byte("Dmitriy Zaporozhets"),
						Email: []byte("dmitriy.zaporozhets@gmail.com"),
						Date:  &timestamp.Timestamp{Seconds: 1393488896},
					},
					ParentIds: []string{"cfe32cf61b73a0d5e9f13e774abde7ff789b1660"},
				},
			},
		},
		{
			desc: "query + empty revision + path not in the commits",
			request: &pb.CommitsByMessageRequest{
				Query: "much more",
				Path:  []byte("bar"),
			},
			expectedCommits: []*pb.GitCommit{},
		},
		{
			desc: "query + bad revision",
			request: &pb.CommitsByMessageRequest{
				Revision: []byte("maaaaasterrrrr"),
				Query:    "much more",
			},
			expectedCommits: []*pb.GitCommit{},
		},
	}

	for _, testCase := range testCases {
		t.Logf("test case: %v", testCase.desc)

		request := testCase.request
		request.Repository = testRepo

		c, err := client.CommitsByMessage(context.Background(), request)
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
			require.Equal(t, testCase.expectedCommits[i], receivedCommit, "mismatched commit")
		}
	}
}

func TestFailedCommitsByMessageRequest(t *testing.T) {
	service, ruby, serverSocketPath := startTestServices(t)
	defer stopTestServices(service, ruby)

	client := newCommitServiceClient(t, serverSocketPath)

	invalidRepo := &pb.Repository{StorageName: "fake", RelativePath: "path"}

	testCases := []struct {
		desc    string
		request *pb.CommitsByMessageRequest
		code    codes.Code
	}{
		{
			desc:    "Invalid repository",
			request: &pb.CommitsByMessageRequest{Repository: invalidRepo, Query: "foo"},
			code:    codes.InvalidArgument,
		},
		{
			desc:    "Repository is nil",
			request: &pb.CommitsByMessageRequest{Query: "foo"},
			code:    codes.InvalidArgument,
		},
		{
			desc:    "Query is missing",
			request: &pb.CommitsByMessageRequest{Repository: testRepo},
			code:    codes.InvalidArgument,
		},
	}

	for _, testCase := range testCases {
		t.Logf("test case: %v", testCase.desc)

		c, err := client.CommitsByMessage(context.Background(), testCase.request)
		if err != nil {
			t.Fatal(err)
		}

		testhelper.AssertGrpcError(t, drainCommitsByMessageResponse(c), testCase.code, "")
	}
}

func drainCommitsByMessageResponse(c pb.CommitService_CommitsByMessageClient) error {
	var err error
	for err == nil {
		_, err = c.Recv()
	}
	return err
}
