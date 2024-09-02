package gitcmd_test

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/gitcmd"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v16/internal/helper/text"
	"gitlab.com/gitlab-org/gitaly/v16/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
)

func TestDetectObjectHash(t *testing.T) {
	t.Parallel()

	cfg := testcfg.Build(t)
	ctx := testhelper.Context(t)
	gitCmdFactory := gittest.NewCommandFactory(t, cfg)

	for _, tc := range []struct {
		desc         string
		setup        func(t *testing.T) *gitalypb.Repository
		expectedErr  error
		expectedHash git.ObjectHash
	}{
		{
			desc: "defaults to SHA1",
			setup: func(t *testing.T) *gitalypb.Repository {
				repo, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
					SkipCreationViaService: true,
					ObjectFormat:           "sha1",
				})

				// Verify that the repo doesn't explicitly mention it's using SHA1
				// as object hash.
				content := testhelper.MustReadFile(t, filepath.Join(repoPath, "config"))
				require.NotContains(t, text.ChompBytes(content), "sha1")

				return repo
			},
			expectedHash: git.ObjectHashSHA1,
		},
		{
			desc: "explicitly set to SHA1",
			setup: func(t *testing.T) *gitalypb.Repository {
				repo, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
					SkipCreationViaService: true,
					ObjectFormat:           "sha1",
				})

				// Explicitly set the object format to SHA1. Note that setting the
				// object format explicitly requires the repository format version
				// to be at least `1`.
				gittest.Exec(t, cfg, "-C", repoPath, "config", "core.repositoryFormatVersion", "1")
				gittest.Exec(t, cfg, "-C", repoPath, "config", "extensions.objectFormat", "sha1")

				return repo
			},
			expectedHash: git.ObjectHashSHA1,
		},
		{
			desc: "explicitly set to SHA256",
			setup: func(t *testing.T) *gitalypb.Repository {
				repo, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
					SkipCreationViaService: true,
					ObjectFormat:           "sha256",
				})

				require.Equal(t,
					"sha256",
					text.ChompBytes(gittest.Exec(t, cfg, "-C", repoPath, "config", "extensions.objectFormat")),
				)

				return repo
			},
			expectedHash: git.ObjectHashSHA256,
		},
		{
			desc: "invalid repository configuration",
			setup: func(t *testing.T) *gitalypb.Repository {
				testhelper.SkipWithReftable(t, "creating a repository with reftables sets the core.repositoryformatversion to 1")

				repo, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
					SkipCreationViaService: true,
					ObjectFormat:           "sha1",
				})

				gittest.Exec(t, cfg, "-C", repoPath, "config", "extensions.objectFormat", "sha1")

				return repo
			},
			expectedErr: structerr.New("reading object format: exit status 128").WithMetadata(
				"stderr", "fatal: repo version is 0, but v1-only extension found:\n\tobjectformat\n",
			),
		},
		{
			desc: "unknown hash",
			setup: func(t *testing.T) *gitalypb.Repository {
				repo, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
					SkipCreationViaService: true,
				})

				// Explicitly set the object format to something unknown.
				gittest.Exec(t, cfg, "-C", repoPath, "config", "extensions.objectFormat", "blake2")

				return repo
			},
			expectedErr: structerr.New("reading object format: exit status 128").WithMetadata(
				"stderr", "error: invalid value for 'extensions.objectformat'",
			),
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			repoProto := tc.setup(t)

			hash, err := gitcmd.DetectObjectHash(ctx, gitCmdFactory, repoProto)
			if tc.expectedErr != nil {
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.expectedErr.Error())
			} else {
				require.NoError(t, err)
			}

			// Function pointers cannot be compared, so we need to unset them.
			hash.Hash = nil
			tc.expectedHash.Hash = nil

			require.Equal(t, tc.expectedHash, hash)
		})
	}
}
