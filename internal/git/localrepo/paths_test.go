package localrepo_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/quarantine"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/mode"
	"gitlab.com/gitlab-org/gitaly/v16/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func TestRepo_Path(t *testing.T) {
	ctx := testhelper.Context(t)
	cfg := testcfg.Build(t)

	t.Run("valid repository", func(t *testing.T) {
		repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
			SkipCreationViaService: true,
		})
		repo := localrepo.NewTestRepo(t, cfg, repoProto)

		path, err := repo.Path(ctx)
		require.NoError(t, err)
		require.Equal(t, repoPath, path)
	})

	t.Run("deleted repository", func(t *testing.T) {
		repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
			SkipCreationViaService: true,
		})
		repo := localrepo.NewTestRepo(t, cfg, repoProto)

		require.NoError(t, os.RemoveAll(repoPath))

		_, err := repo.Path(ctx)
		require.Equal(t, storage.NewRepositoryNotFoundError(cfg.Storages[0].Name, repoProto.GetRelativePath()), err)
	})

	t.Run("non-git repository", func(t *testing.T) {
		repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
			SkipCreationViaService: true,
		})
		repo := localrepo.NewTestRepo(t, cfg, repoProto)

		// Recreate the repository as a simple empty directory to simulate
		// that the repository is in a partially-created state.
		require.NoError(t, os.RemoveAll(repoPath))
		require.NoError(t, os.MkdirAll(repoPath, mode.Directory))

		_, err := repo.Path(ctx)
		require.Equal(t, structerr.NewFailedPrecondition("%w: %q does not exist", storage.ErrRepositoryNotValid, "objects").WithMetadata("repository_path", repoPath), err)
	})
}

func TestRepo_ObjectDirectoryPath(t *testing.T) {
	ctx := testhelper.Context(t)
	cfg := testcfg.Build(t)

	repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
		SkipCreationViaService: true,
	})
	locator := config.NewLocator(cfg)

	quarantine, err := quarantine.New(ctx, repoProto, testhelper.NewLogger(t), locator)
	require.NoError(t, err)
	quarantinedRepo := quarantine.QuarantinedRepo()

	// Transactions store their set a quarantine directory in the transaction's temporary
	// directory with a path ending in `quarantine` directory. Emulate that by creating
	// such a directory in the root of the storage.
	transactionStateDir := filepath.Join(cfg.Storages[0].Path, "tx-tmp")
	transactionQuarantineDir := filepath.Join(transactionStateDir, "quarantine")
	transactionQuarantineDirWithGitPush := filepath.Join(transactionQuarantineDir, "tmp_objdir-incoming-Gbc29N")
	require.NoError(t, os.MkdirAll(transactionQuarantineDirWithGitPush, mode.Directory))
	transactionQuarantineDirRelativePath, err := filepath.Rel(repoPath, transactionQuarantineDir)
	require.NoError(t, err)
	transactionQuarantineDirWithGitPushRelativePath, err := filepath.Rel(repoPath, transactionQuarantineDirWithGitPush)
	require.NoError(t, err)

	repoWithGitObjDir := func(repo *gitalypb.Repository, dir string) *gitalypb.Repository {
		repo = proto.Clone(repo).(*gitalypb.Repository)
		repo.GitObjectDirectory = dir
		return repo
	}

	testCases := []struct {
		desc string
		repo *gitalypb.Repository
		path string
		err  codes.Code
	}{
		{
			desc: "storages configured",
			repo: repoWithGitObjDir(repoProto, "objects/"),
			path: filepath.Join(repoPath, "objects/"),
		},
		{
			desc: "repo quarantined by transaction manager",
			repo: repoWithGitObjDir(quarantinedRepo, transactionQuarantineDirRelativePath),
			path: transactionQuarantineDir,
		},
		{
			desc: "repo quarantined by transaction manager additionally quarantined by git push",
			repo: repoWithGitObjDir(quarantinedRepo, transactionQuarantineDirWithGitPushRelativePath),
			path: transactionQuarantineDirWithGitPush,
		},
		{
			desc: "object directory path points outside of storage",
			repo: repoWithGitObjDir(quarantinedRepo, func() string {
				escapingPath, err := filepath.Rel(repoPath, filepath.Dir(cfg.Storages[0].Path))
				require.NoError(t, err)
				return escapingPath
			}()),
			err: codes.InvalidArgument,
		},
		{
			desc: "no GitObjectDirectoryPath",
			repo: repoProto,
			err:  codes.InvalidArgument,
		},
		{
			desc: "with directory traversal",
			repo: repoWithGitObjDir(repoProto, "../bazqux.git"),
			err:  codes.InvalidArgument,
		},
		{
			desc: "valid path but doesn't exist",
			repo: repoWithGitObjDir(repoProto, "foo../bazqux.git"),
			err:  codes.NotFound,
		},
		{
			desc: "with sneaky directory traversal",
			repo: repoWithGitObjDir(repoProto, "/../bazqux.git"),
			err:  codes.InvalidArgument,
		},
		{
			desc: "with traversal outside repository",
			repo: repoWithGitObjDir(repoProto, "objects/../.."),
			err:  codes.InvalidArgument,
		},
		{
			desc: "with traversal outside repository with trailing separator",
			repo: repoWithGitObjDir(repoProto, "objects/../../"),
			err:  codes.InvalidArgument,
		},
		{
			desc: "with deep traversal at the end",
			repo: repoWithGitObjDir(repoProto, "bazqux.git/../.."),
			err:  codes.InvalidArgument,
		},
		{
			desc: "quarantined repo",
			repo: quarantinedRepo,
			path: filepath.Join(repoPath, quarantinedRepo.GetGitObjectDirectory()),
		},
		{
			desc: "quarantined repo with parent directory",
			repo: repoWithGitObjDir(quarantinedRepo, quarantinedRepo.GetGitObjectDirectory()+"/.."),
			err:  codes.InvalidArgument,
		},
		{
			desc: "quarantined repo with directory traversal",
			repo: repoWithGitObjDir(quarantinedRepo, quarantinedRepo.GetGitObjectDirectory()+"/../foobar.git"),
			err:  codes.InvalidArgument,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			repo := localrepo.NewTestRepo(t, cfg, tc.repo)

			path, err := repo.ObjectDirectoryPath(ctx)

			if tc.err != codes.OK {
				st, ok := status.FromError(err)
				require.True(t, ok)
				require.Equal(t, tc.err, st.Code())
				return
			}

			require.NoError(t, err)
			require.Equal(t, tc.path, path)
		})
	}
}
