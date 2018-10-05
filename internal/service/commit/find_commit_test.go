package commit

import (
	"io/ioutil"
	"strings"
	"testing"

	"github.com/golang/protobuf/ptypes/timestamp"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly-proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/internal/featureflag"
	"gitlab.com/gitlab-org/gitaly/internal/git/log"
	"gitlab.com/gitlab-org/gitaly/internal/helper"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"golang.org/x/net/context"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func TestSuccessfulFindCommitRequest(t *testing.T) {
	windows1251Message, err := ioutil.ReadFile("testdata/commit-c809470461118b7bcab850f6e9a7ca97ac42f8ea-message.txt")
	require.NoError(t, err)

	server, serverSocketPath := startTestServices(t)
	defer server.Stop()

	client, conn := newCommitServiceClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, testRepoPath, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	ctx, cancel := testhelper.Context()
	defer cancel()

	bigMessage := "An empty commit with REALLY BIG message\n\n" + strings.Repeat("MOAR!\n", 20*1024)
	bigCommitID := testhelper.CreateCommit(t, testRepoPath, "local-big-commits", &testhelper.CreateCommitOpts{
		Message:  bigMessage,
		ParentID: "60ecb67744cb56576c30214ff52294f8ce2def98",
	})
	bigCommit, err := log.GetCommit(ctx, testRepo, bigCommitID)
	require.NoError(t, err)

	testCases := []struct {
		description string
		revision    string
		commit      *gitalypb.GitCommit
	}{
		{
			description: "With a branch name",
			revision:    "branch-merged",
			commit: &gitalypb.GitCommit{
				Id:      "498214de67004b1da3d820901307bed2a68a8ef6",
				Subject: []byte("adds bar folder and branch-test text file to check Repository merged_to_root_ref method"),
				Body:    []byte("adds bar folder and branch-test text file to check Repository merged_to_root_ref method\n"),
				Author: &gitalypb.CommitAuthor{
					Name:  []byte("tiagonbotelho"),
					Email: []byte("tiagonbotelho@hotmail.com"),
					Date:  &timestamp.Timestamp{Seconds: 1474470806},
				},
				Committer: &gitalypb.CommitAuthor{
					Name:  []byte("tiagonbotelho"),
					Email: []byte("tiagonbotelho@hotmail.com"),
					Date:  &timestamp.Timestamp{Seconds: 1474470806},
				},
				ParentIds: []string{"1b12f15a11fc6e62177bef08f47bc7b5ce50b141"},
				BodySize:  88,
			},
		},
		{
			description: "With a tag name",
			revision:    "v1.0.0",
			commit: &gitalypb.GitCommit{
				Id:      "6f6d7e7ed97bb5f0054f2b1df789b39ca89b6ff9",
				Subject: []byte("More submodules"),
				Body:    []byte("More submodules\n\nSigned-off-by: Dmitriy Zaporozhets <dmitriy.zaporozhets@gmail.com>\n"),
				Author: &gitalypb.CommitAuthor{
					Name:  []byte("Dmitriy Zaporozhets"),
					Email: []byte("dmitriy.zaporozhets@gmail.com"),
					Date:  &timestamp.Timestamp{Seconds: 1393491261},
				},
				Committer: &gitalypb.CommitAuthor{
					Name:  []byte("Dmitriy Zaporozhets"),
					Email: []byte("dmitriy.zaporozhets@gmail.com"),
					Date:  &timestamp.Timestamp{Seconds: 1393491261},
				},
				ParentIds: []string{"d14d6c0abdd253381df51a723d58691b2ee1ab08"},
				BodySize:  84,
			},
		},
		{
			description: "With a hash",
			revision:    "b83d6e391c22777fca1ed3012fce84f633d7fed0",
			commit: &gitalypb.GitCommit{
				Id:      "b83d6e391c22777fca1ed3012fce84f633d7fed0",
				Subject: []byte("Merge branch 'branch-merged' into 'master'"),
				Body:    []byte("Merge branch 'branch-merged' into 'master'\r\n\r\nadds bar folder and branch-test text file to check Repository merged_to_root_ref method\r\n\r\n\r\n\r\nSee merge request !12"),
				Author: &gitalypb.CommitAuthor{
					Name:  []byte("Job van der Voort"),
					Email: []byte("job@gitlab.com"),
					Date:  &timestamp.Timestamp{Seconds: 1474987066},
				},
				Committer: &gitalypb.CommitAuthor{
					Name:  []byte("Job van der Voort"),
					Email: []byte("job@gitlab.com"),
					Date:  &timestamp.Timestamp{Seconds: 1474987066},
				},
				ParentIds: []string{
					"1b12f15a11fc6e62177bef08f47bc7b5ce50b141",
					"498214de67004b1da3d820901307bed2a68a8ef6",
				},
				BodySize: 162,
			},
		},
		{
			description: "With an initial commit",
			revision:    "1a0b36b3cdad1d2ee32457c102a8c0b7056fa863",
			commit: &gitalypb.GitCommit{
				Id:      "1a0b36b3cdad1d2ee32457c102a8c0b7056fa863",
				Subject: []byte("Initial commit"),
				Body:    []byte("Initial commit\n"),
				Author: &gitalypb.CommitAuthor{
					Name:  []byte("Dmitriy Zaporozhets"),
					Email: []byte("dmitriy.zaporozhets@gmail.com"),
					Date:  &timestamp.Timestamp{Seconds: 1393488198},
				},
				Committer: &gitalypb.CommitAuthor{
					Name:  []byte("Dmitriy Zaporozhets"),
					Email: []byte("dmitriy.zaporozhets@gmail.com"),
					Date:  &timestamp.Timestamp{Seconds: 1393488198},
				},
				ParentIds: nil,
				BodySize:  15,
			},
		},
		{
			description: "with non-utf8 message encoding, recognized by Git",
			revision:    "c809470461118b7bcab850f6e9a7ca97ac42f8ea",
			commit: &gitalypb.GitCommit{
				Id:      "c809470461118b7bcab850f6e9a7ca97ac42f8ea",
				Subject: windows1251Message[:len(windows1251Message)-1],
				Body:    windows1251Message,
				Author: &gitalypb.CommitAuthor{
					Name:  []byte("Jacob Vosmaer"),
					Email: []byte("jacob@gitlab.com"),
					Date:  &timestamp.Timestamp{Seconds: 1512132977},
				},
				Committer: &gitalypb.CommitAuthor{
					Name:  []byte("Jacob Vosmaer"),
					Email: []byte("jacob@gitlab.com"),
					Date:  &timestamp.Timestamp{Seconds: 1512132977},
				},
				ParentIds: []string{"e63f41fe459e62e1228fcef60d7189127aeba95a"},
				BodySize:  49,
			},
		},
		{
			description: "with non-utf8 garbage message encoding, not recognized by Git",
			revision:    "0999bb770f8dc92ab5581cc0b474b3e31a96bf5c",
			commit: &gitalypb.GitCommit{
				Id:      "0999bb770f8dc92ab5581cc0b474b3e31a96bf5c",
				Subject: []byte("Hello\xf0world"),
				Body:    []byte("Hello\xf0world\n"),
				Author: &gitalypb.CommitAuthor{
					Name:  []byte("Jacob Vosmaer"),
					Email: []byte("jacob@gitlab.com"),
					Date:  &timestamp.Timestamp{Seconds: 1517328273},
				},
				Committer: &gitalypb.CommitAuthor{
					Name:  []byte("Jacob Vosmaer"),
					Email: []byte("jacob@gitlab.com"),
					Date:  &timestamp.Timestamp{Seconds: 1517328273},
				},
				ParentIds: []string{"60ecb67744cb56576c30214ff52294f8ce2def98"},
				BodySize:  12,
			},
		},
		{
			description: "with a very large message",
			revision:    bigCommitID,
			commit: &gitalypb.GitCommit{
				Id:      bigCommitID,
				Subject: []byte("An empty commit with REALLY BIG message"),
				Author: &gitalypb.CommitAuthor{
					Name:  []byte("Scrooge McDuck"),
					Email: []byte("scrooge@mcduck.com"),
					Date:  &timestamp.Timestamp{Seconds: bigCommit.Author.Date.Seconds},
				},
				Committer: &gitalypb.CommitAuthor{
					Name:  []byte("Scrooge McDuck"),
					Email: []byte("scrooge@mcduck.com"),
					Date:  &timestamp.Timestamp{Seconds: bigCommit.Committer.Date.Seconds},
				},
				ParentIds: []string{"60ecb67744cb56576c30214ff52294f8ce2def98"},
				Body:      []byte(bigMessage[:helper.MaxCommitOrTagMessageSize]),
				BodySize:  int64(len(bigMessage)),
			},
		},
		{
			description: "with different author and committer",
			revision:    "77e835ef0856f33c4f0982f84d10bdb0567fe440",
			commit: &gitalypb.GitCommit{
				Id:      "77e835ef0856f33c4f0982f84d10bdb0567fe440",
				Subject: []byte("Add file larger than 1 mb"),
				Body:    []byte("Add file larger than 1 mb\n\nIn order to test Max File Size push rule we need a file larger than 1 MB\n"),
				Author: &gitalypb.CommitAuthor{
					Name:  []byte("Ruben Davila"),
					Email: []byte("rdavila84@gmail.com"),
					Date:  &timestamp.Timestamp{Seconds: 1523247267},
				},
				Committer: &gitalypb.CommitAuthor{
					Name:  []byte("Jacob Vosmaer"),
					Email: []byte("jacob@gitlab.com"),
					Date:  &timestamp.Timestamp{Seconds: 1527855450},
				},
				ParentIds: []string{"60ecb67744cb56576c30214ff52294f8ce2def98"},
				BodySize:  100,
			},
		},
		{
			description: "With a non-existing ref name",
			revision:    "this-doesnt-exists",
			commit:      nil,
		},
		{
			description: "With a non-existing hash",
			revision:    "f48214de67004b1da3d820901307bed2a68a8ef6",
			commit:      nil,
		},
	}

	allCommits := []*gitalypb.GitCommit{}
	for _, testCase := range testCases {
		t.Run(testCase.description, func(t *testing.T) {
			request := &gitalypb.FindCommitRequest{
				Repository: testRepo,
				Revision:   []byte(testCase.revision),
			}

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			response, err := client.FindCommit(ctx, request)
			require.NoError(t, err)

			require.Equal(t, testCase.commit, response.Commit, "mismatched commits")
			allCommits = append(allCommits, response.Commit)
		})
	}
	require.Equal(t, len(testCases), len(allCommits), "length of allCommits")
}

func TestFailedFindCommitRequest(t *testing.T) {
	server, serverSocketPath := startTestServices(t)
	defer server.Stop()

	client, conn := newCommitServiceClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, _, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	invalidRepo := &gitalypb.Repository{StorageName: "fake", RelativePath: "path"}

	testCases := []struct {
		description string
		revision    []byte
		repo        *gitalypb.Repository
	}{
		{repo: invalidRepo, revision: []byte("master"), description: "Invalid repo"},
		{repo: testRepo, revision: []byte(""), description: "Empty revision"},
		{repo: testRepo, revision: []byte("-master"), description: "Invalid revision"},
		{repo: testRepo, revision: []byte("mas:ter"), description: "Invalid revision"},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	gogitCtx := metadata.NewOutgoingContext(
		ctx,
		metadata.New(map[string]string{featureflag.HeaderKey("gogit-findcommit"): "true"}),
	)

	for _, testCase := range testCases {
		t.Run(testCase.description, func(t *testing.T) {
			request := &gitalypb.FindCommitRequest{
				Repository: testCase.repo,
				Revision:   testCase.revision,
			}

			_, err := client.FindCommit(ctx, request)
			require.Equal(t, codes.InvalidArgument, status.Code(err), "default lookup should fail")

			_, err = client.FindCommit(gogitCtx, request)
			require.Equal(t, codes.InvalidArgument, status.Code(err), "go-git lookup should fail")
		})
	}
}
