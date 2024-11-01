package repository

import (
	"fmt"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper"
)

func TestOffloadRepository(t *testing.T) {
	t.Parallel()

	cfg, client := setupRepositoryService(t)
	cfg.Transactions.Enabled = true
	ctx := testhelper.Context(t)

	repo, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
		RelativePath: gittest.NewRepositoryName(t),
	})

	require.DirExists(t, repoPath)

	// Add the following objects into the repo
	// 2ef63084d7fd7848f5893cc5959fb81f1eeb0e62 tree 71
	// 6a45195f33d722462b36a95b28ba0d11e556e96f blob 18
	// 7b98f0ca009fb8818fd642d31d2e940e5c586dbb blob 15
	// 890c35272cc2438f9b765795dccb7f3bc7062d3d blob 15
	// 9dc0e9401d8fa380b2d0b84066cc9f6ad4f60649 tree 38
	// c9c78906e41cc0bb83f0acfd8794713f92469e2c tree 105
	// df0bb8fb85e8dc17f3166b73a24a3c8a28785f4b commit 177
	// f3efabedadaa69545b109e054bdf9fd1a17fd22b blob 14
	commitID := gittest.WriteCommit(t, cfg, repoPath, gittest.WithTreeEntries(
		gittest.TreeEntry{Path: "LICENSE", Mode: "100644", Content: "license content"},
		gittest.TreeEntry{Path: "README.md", Mode: "100644", Content: "readme content"},
		gittest.TreeEntry{Path: "subdir", Mode: "040000", OID: gittest.WriteTree(t, cfg, repoPath, []gittest.TreeEntry{
			{Path: "subfile", Mode: "100644", Content: "subfile content"},
			{Path: "subsubdir", Mode: "040000", OID: gittest.WriteTree(t, cfg, repoPath, []gittest.TreeEntry{
				{Path: "subsubfile", Mode: "100644", Content: "subsubfile content"},
			})},
		})},
	))
	gittest.Exec(t, cfg, "-C", repoPath, "gc")

	packDir := fmt.Sprintf("%s/objects/pack", repoPath)

	require.NotEmpty(t, commitID)
	_, err := client.OffloadRepository(ctx, &gitalypb.OffloadRequest{
		Repository: repo,
	})
	require.NoError(t, err)
	require.NoFileExists(t, packDir, fmt.Sprintf("%s should be empty", packDir))

	// New test case:
	// * can cat-file from promisor remote
}
