package commit

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"os/exec"
	"testing"

	"google.golang.org/grpc/codes"

	pb "gitlab.com/gitlab-org/gitaly-proto/go"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"

	"github.com/golang/protobuf/ptypes/timestamp"
	"github.com/stretchr/testify/require"
)

func TestFindCommitsFields(t *testing.T) {
	windows1251Message, err := ioutil.ReadFile("testdata/commit-c809470461118b7bcab850f6e9a7ca97ac42f8ea-message.txt")
	require.NoError(t, err)

	server, serverSocketPath := startTestServices(t)
	defer server.Stop()

	client, conn := newCommitServiceClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, _, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	testCases := []struct {
		id     string
		commit *pb.GitCommit
	}{
		{
			id: "b83d6e391c22777fca1ed3012fce84f633d7fed0",
			commit: &pb.GitCommit{
				Id:      "b83d6e391c22777fca1ed3012fce84f633d7fed0",
				Subject: []byte("Merge branch 'branch-merged' into 'master'"),
				Body:    []byte("Merge branch 'branch-merged' into 'master'\r\n\r\nadds bar folder and branch-test text file to check Repository merged_to_root_ref method\r\n\r\n\r\n\r\nSee merge request !12"),
				Author: &pb.CommitAuthor{
					Name:  []byte("Job van der Voort"),
					Email: []byte("job@gitlab.com"),
					Date:  &timestamp.Timestamp{Seconds: 1474987066},
				},
				Committer: &pb.CommitAuthor{
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
			id: "c809470461118b7bcab850f6e9a7ca97ac42f8ea",
			commit: &pb.GitCommit{
				Id:      "c809470461118b7bcab850f6e9a7ca97ac42f8ea",
				Subject: windows1251Message[:len(windows1251Message)-1],
				Body:    windows1251Message,
				Author: &pb.CommitAuthor{
					Name:  []byte("Jacob Vosmaer"),
					Email: []byte("jacob@gitlab.com"),
					Date:  &timestamp.Timestamp{Seconds: 1512132977},
				},
				Committer: &pb.CommitAuthor{
					Name:  []byte("Jacob Vosmaer"),
					Email: []byte("jacob@gitlab.com"),
					Date:  &timestamp.Timestamp{Seconds: 1512132977},
				},
				ParentIds: []string{"e63f41fe459e62e1228fcef60d7189127aeba95a"},
				BodySize:  49,
			},
		},
		{
			id: "0999bb770f8dc92ab5581cc0b474b3e31a96bf5c",
			commit: &pb.GitCommit{
				Id:      "0999bb770f8dc92ab5581cc0b474b3e31a96bf5c",
				Subject: []byte("Hello\xf0world"),
				Body:    []byte("Hello\xf0world\n"),
				Author: &pb.CommitAuthor{
					Name:  []byte("Jacob Vosmaer"),
					Email: []byte("jacob@gitlab.com"),
					Date:  &timestamp.Timestamp{Seconds: 1517328273},
				},
				Committer: &pb.CommitAuthor{
					Name:  []byte("Jacob Vosmaer"),
					Email: []byte("jacob@gitlab.com"),
					Date:  &timestamp.Timestamp{Seconds: 1517328273},
				},
				ParentIds: []string{"60ecb67744cb56576c30214ff52294f8ce2def98"},
				BodySize:  12,
			},
		},
		{
			id: "77e835ef0856f33c4f0982f84d10bdb0567fe440",
			commit: &pb.GitCommit{
				Id:      "77e835ef0856f33c4f0982f84d10bdb0567fe440",
				Subject: []byte("Add file larger than 1 mb"),
				Body:    []byte("Add file larger than 1 mb\n\nIn order to test Max File Size push rule we need a file larger than 1 MB\n"),
				Author: &pb.CommitAuthor{
					Name:  []byte("Ruben Davila"),
					Email: []byte("rdavila84@gmail.com"),
					Date:  &timestamp.Timestamp{Seconds: 1523247267},
				},
				Committer: &pb.CommitAuthor{
					Name:  []byte("Jacob Vosmaer"),
					Email: []byte("jacob@gitlab.com"),
					Date:  &timestamp.Timestamp{Seconds: 1527855450},
				},
				ParentIds: []string{"60ecb67744cb56576c30214ff52294f8ce2def98"},
				BodySize:  100,
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.id, func(t *testing.T) {
			request := &pb.FindCommitsRequest{
				Repository: testRepo,
				Revision:   []byte(tc.id),
				Limit:      1,
			}

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			stream, err := client.FindCommits(ctx, request)
			require.NoError(t, err)

			resp, err := stream.Recv()
			require.NoError(t, err)

			require.Equal(t, 1, len(resp.Commits), "expected exactly one commit in the first message")
			firstCommit := resp.Commits[0]

			require.Equal(t, tc.commit, firstCommit, "mismatched commits")

			_, err = stream.Recv()
			require.Equal(t, io.EOF, err, "there should be no further messages in the stream")
		})
	}
}

func TestSuccessfulFindCommitsRequest(t *testing.T) {
	server, serverSocketPath := startTestServices(t)
	defer server.Stop()

	client, conn := newCommitServiceClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, _, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	testCases := []struct {
		desc    string
		request *pb.FindCommitsRequest
		// Use 'ids' if you know the exact commits id's that should be returned
		ids []string
		// Use minCommits if you don't know the exact commit id's
		minCommits int
	}{
		{
			desc: "only revision, limit commits",
			request: &pb.FindCommitsRequest{
				Repository: testRepo,
				Revision:   []byte("0031876facac3f2b2702a0e53a26e89939a42209"),
				Limit:      3,
			},
			ids: []string{
				"0031876facac3f2b2702a0e53a26e89939a42209",
				"bf6e164cac2dc32b1f391ca4290badcbe4ffc5fb",
				"48ca272b947f49eee601639d743784a176574a09",
			},
		},
		{
			desc: "revision, default commit limit",
			request: &pb.FindCommitsRequest{
				Repository: testRepo,
				Revision:   []byte("0031876facac3f2b2702a0e53a26e89939a42209"),
			},
		},
		{
			desc: "revision, default commit limit, bypassing rugged walk",
			request: &pb.FindCommitsRequest{
				Repository:  testRepo,
				Revision:    []byte("0031876facac3f2b2702a0e53a26e89939a42209"),
				DisableWalk: true,
			},
		}, {
			desc: "revision and paths",
			request: &pb.FindCommitsRequest{
				Repository: testRepo,
				Revision:   []byte("0031876facac3f2b2702a0e53a26e89939a42209"),
				Paths:      [][]byte{[]byte("LICENSE")},
				Limit:      10,
			},
			ids: []string{"1a0b36b3cdad1d2ee32457c102a8c0b7056fa863"},
		},
		{
			desc: "empty revision",
			request: &pb.FindCommitsRequest{
				Repository: testRepo,
				Limit:      35,
			},
			minCommits: 35,
		},
		{
			desc: "before and after",
			request: &pb.FindCommitsRequest{
				Repository: testRepo,
				Before:     &timestamp.Timestamp{Seconds: 1483225200},
				After:      &timestamp.Timestamp{Seconds: 1472680800},
				Limit:      10,
			},
			ids: []string{
				"b83d6e391c22777fca1ed3012fce84f633d7fed0",
				"498214de67004b1da3d820901307bed2a68a8ef6",
			},
		},
		{
			desc: "no merges",
			request: &pb.FindCommitsRequest{
				Repository: testRepo,
				Revision:   []byte("e63f41fe459e62e1228fcef60d7189127aeba95a"),
				SkipMerges: true,
				Limit:      10,
			},
			ids: []string{
				"4a24d82dbca5c11c61556f3b35ca472b7463187e",
				"498214de67004b1da3d820901307bed2a68a8ef6",
				"38008cb17ce1466d8fec2dfa6f6ab8dcfe5cf49e",
				"c347ca2e140aa667b968e51ed0ffe055501fe4f4",
				"d59c60028b053793cecfb4022de34602e1a9218e",
				"a5391128b0ef5d21df5dd23d98557f4ef12fae20",
				"54fcc214b94e78d7a41a9a8fe6d87a5e59500e51",
				"048721d90c449b244b7b4c53a9186b04330174ec",
				"5f923865dde3436854e9ceb9cdb7815618d4e849",
				"2ea1f3dec713d940208fb5ce4a38765ecb5d3f73",
			},
		},
		{
			desc: "following renames",
			request: &pb.FindCommitsRequest{
				Repository: testRepo,
				Revision:   []byte("94bb47ca1297b7b3731ff2a36923640991e9236f"),
				Paths:      [][]byte{[]byte("CHANGELOG.md")},
				Follow:     true,
				Limit:      10,
			},
			ids: []string{
				"94bb47ca1297b7b3731ff2a36923640991e9236f",
				"5f923865dde3436854e9ceb9cdb7815618d4e849",
				"913c66a37b4a45b9769037c55c2d238bd0942d2e",
			},
		},
		{
			desc: "all refs",
			request: &pb.FindCommitsRequest{
				Repository: testRepo,
				All:        true,
				Limit:      90,
			},
			minCommits: 90,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			ctx, cancel := testhelper.Context()
			defer cancel()

			stream, err := client.FindCommits(ctx, tc.request)
			require.NoError(t, err)

			var ids []string
			for err == nil {
				var resp *pb.FindCommitsResponse
				resp, err = stream.Recv()
				for _, c := range resp.GetCommits() {
					ids = append(ids, c.Id)
				}
			}
			require.Equal(t, io.EOF, err)

			if tc.minCommits > 0 {
				require.True(t, len(ids) >= tc.minCommits, "expected at least %d commits, got %d", tc.minCommits, len(ids))
				return
			}

			require.Equal(t, len(tc.ids), len(ids))
			for i, id := range tc.ids {
				require.Equal(t, id, ids[i])
			}
		})
	}
}

func TestSuccessfulFindCommitsRequestWithAltGitObjectDirs(t *testing.T) {
	server, serverSocketPath := startTestServices(t)
	defer server.Stop()

	client, conn := newCommitServiceClient(t, serverSocketPath)
	defer conn.Close()

	committerName := "Scrooge McDuck"
	committerEmail := "scrooge@mcduck.com"

	testRepoCopy, testRepoCopyPath, cleanupFn := testhelper.NewTestRepoWithWorktree(t)
	defer cleanupFn()

	cmd := exec.Command("git", "-C", testRepoCopyPath,
		"-c", fmt.Sprintf("user.name=%s", committerName),
		"-c", fmt.Sprintf("user.email=%s", committerEmail),
		"commit", "--allow-empty", "-m", "An empty commit")
	currentHead, altObjectsDir := testhelper.CreateCommitInAlternateObjectDirectory(t, testRepoCopyPath, cmd)

	testCases := []struct {
		desc          string
		altDirs       []string
		expectedCount int
	}{
		{
			desc:          "present GIT_ALTERNATE_OBJECT_DIRECTORIES",
			altDirs:       []string{altObjectsDir},
			expectedCount: 1,
		},
		{
			desc:          "empty GIT_ALTERNATE_OBJECT_DIRECTORIES",
			altDirs:       []string{},
			expectedCount: 0,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.desc, func(t *testing.T) {
			testRepoCopy.GitAlternateObjectDirectories = testCase.altDirs
			request := &pb.FindCommitsRequest{
				Repository: testRepoCopy,
				Revision:   currentHead,
				Limit:      1,
			}

			ctx, cancel := testhelper.Context()
			defer cancel()

			c, err := client.FindCommits(ctx, request)
			require.NoError(t, err)

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

			require.Equal(t, testCase.expectedCount, len(receivedCommits), "number of commits received")
		})
	}
}

func TestFailureFindCommitsRequest(t *testing.T) {
	server, serverSocketPath := startTestServices(t)
	defer server.Stop()

	client, conn := newCommitServiceClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, _, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	testCases := []struct {
		desc    string
		request *pb.FindCommitsRequest
		code    codes.Code
	}{
		{
			desc: "empty path string",
			request: &pb.FindCommitsRequest{
				Repository: testRepo,
				Paths:      [][]byte{[]byte("")},
			},
			code: codes.InvalidArgument,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			ctx, cancel := testhelper.Context()
			defer cancel()

			stream, err := client.FindCommits(ctx, tc.request)
			require.NoError(t, err)

			for err == nil {
				_, err = stream.Recv()
			}

			testhelper.AssertGrpcError(t, err, tc.code, "")
		})
	}
}
