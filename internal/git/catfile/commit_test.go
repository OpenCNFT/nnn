package catfile

import (
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
	"google.golang.org/grpc/metadata"
)

func TestGetCommit(t *testing.T) {
	ctx := testhelper.Context(t)
	ctx = metadata.NewIncomingContext(ctx, metadata.MD{})

	cfg, objectReader, _, repoPath := setupObjectReader(t, ctx)

	blobID := gittest.WriteBlob(t, cfg, repoPath, []byte("data"))
	treeID := gittest.WriteTree(t, cfg, repoPath, []gittest.TreeEntry{
		{Path: "file", Mode: "100644", OID: blobID},
	})
	commitID := gittest.WriteCommit(t, cfg, repoPath, gittest.WithMessage("commit message\n\ncommit body"), gittest.WithTree(treeID))

	for _, tc := range []struct {
		desc           string
		revision       string
		expectedErr    error
		expectedCommit *gitalypb.GitCommit
	}{
		{
			desc:     "commit",
			revision: commitID.String(),
			expectedCommit: &gitalypb.GitCommit{
				Id:        commitID.String(),
				TreeId:    treeID.String(),
				Author:    gittest.DefaultCommitAuthor,
				Committer: gittest.DefaultCommitAuthor,
				Body:      []byte("commit message\n\ncommit body"),
				BodySize:  27,
				Subject:   []byte("commit message"),
			},
		},
		{
			desc:        "not existing commit",
			revision:    "not existing revision",
			expectedErr: NotFoundError{"not existing revision^{commit}"},
		},
		{
			desc:        "blob sha",
			revision:    blobID.String(),
			expectedErr: NotFoundError{blobID.String() + "^{commit}"},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			commit, err := GetCommit(ctx, objectReader, git.Revision(tc.revision))
			if tc.expectedErr != nil || err != nil {
				require.Equal(t, tc.expectedErr, err)
			} else {
				testhelper.ProtoEqual(t, tc.expectedCommit, commit.GitCommit)
			}
		})
	}
}

func TestGetCommitWithTrailers(t *testing.T) {
	ctx := testhelper.Context(t)
	ctx = metadata.NewIncomingContext(ctx, metadata.MD{})

	cfg, objectReader, repoProto, repoPath := setupObjectReader(t, ctx)
	repo := newRepoExecutor(t, cfg, repoProto)

	commitID := gittest.WriteCommit(t, cfg, repoPath, gittest.WithMessage(
		"some header\n"+
			"\n"+
			"Commit message.\n"+
			"\n"+
			"Signed-off-by: John Doe <john.doe@example.com>\n"+
			"Signed-off-by: Jane Doe <jane.doe@example.com>\n",
	))

	commit, err := GetCommitWithTrailers(ctx, repo, objectReader, commitID.Revision())

	require.NoError(t, err)

	require.Equal(t, commit.GetTrailers(), []*gitalypb.CommitTrailer{
		{
			Key:   []byte("Signed-off-by"),
			Value: []byte("John Doe <john.doe@example.com>"),
		},
		{
			Key:   []byte("Signed-off-by"),
			Value: []byte("Jane Doe <jane.doe@example.com>"),
		},
	})
}
