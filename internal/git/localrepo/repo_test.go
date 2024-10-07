package localrepo

import (
	"bytes"
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/catfile"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/gitcmd"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
)

func TestRepo(t *testing.T) {
	cfg := testcfg.Build(t)

	gittest.TestRepository(t, cfg, func(tb testing.TB, ctx context.Context) (gitcmd.Repository, string) {
		tb.Helper()

		repoProto, repoPath := gittest.CreateRepository(tb, ctx, cfg, gittest.CreateRepositoryConfig{
			SkipCreationViaService: true,
		})

		gitCmdFactory := gittest.NewCommandFactory(tb, cfg)
		catfileCache := catfile.NewCache(cfg)
		tb.Cleanup(catfileCache.Stop)
		return New(testhelper.NewLogger(t), config.NewLocator(cfg), gitCmdFactory, catfileCache, repoProto), repoPath
	})
}

func TestRepo_Quarantine(t *testing.T) {
	t.Parallel()

	cfg := testcfg.Build(t)
	catfileCache := catfile.NewCache(cfg)
	defer catfileCache.Stop()

	ctx := testhelper.Context(t)
	repoProto, _ := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
		SkipCreationViaService: true,
	})

	unquarantinedRepo := New(
		testhelper.NewLogger(t),
		config.NewLocator(cfg),
		gittest.NewCommandFactory(t, cfg),
		catfileCache,
		repoProto,
	)

	quarantineDir := testhelper.TempDir(t)

	quarantinedRepo, err := unquarantinedRepo.Quarantine(ctx, quarantineDir)
	require.NoError(t, err)

	quarantinedBlob := []byte("quarantined blob")
	quarantinedBlobOID, err := quarantinedRepo.WriteBlob(ctx, bytes.NewReader(quarantinedBlob), WriteBlobConfig{})
	require.NoError(t, err)

	unquarantinedBlob := []byte("unquarantined blob")
	unquarantinedBlobOID, err := unquarantinedRepo.WriteBlob(ctx, bytes.NewReader(unquarantinedBlob), WriteBlobConfig{})
	require.NoError(t, err)

	for _, tc := range []struct {
		desc            string
		repo            *Repo
		oid             git.ObjectID
		expectedContent []byte
		expectedError   error
	}{
		{
			desc:            "unquarantined repo reads unquarantined blob",
			repo:            unquarantinedRepo,
			oid:             unquarantinedBlobOID,
			expectedContent: unquarantinedBlob,
		},
		{
			desc:          "unquarantined repo reads quarantined blob",
			repo:          unquarantinedRepo,
			oid:           quarantinedBlobOID,
			expectedError: InvalidObjectError(quarantinedBlobOID),
		},
		{
			desc:            "quarantined repo reads unquarantined blob",
			repo:            quarantinedRepo,
			oid:             unquarantinedBlobOID,
			expectedContent: unquarantinedBlob,
		},
		{
			desc:            "quarantined repo reads quarantined blob",
			repo:            quarantinedRepo,
			oid:             quarantinedBlobOID,
			expectedContent: quarantinedBlob,
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			content, err := tc.repo.ReadObject(ctx, tc.oid)
			require.Equal(t, tc.expectedError, err)
			require.Equal(t, tc.expectedContent, content)
		})
	}
}

func TestRepo_Quarantine_nonExistentRepository(t *testing.T) {
	t.Parallel()

	cfg := testcfg.Build(t)

	quarantineDir := filepath.Join(cfg.Storages[0].Path, "quarantine")

	for _, tc := range []struct {
		desc          string
		inputRepo     *gitalypb.Repository
		expectedRepo  *gitalypb.Repository
		expectedError error
	}{
		{
			desc: "non-existent storage",
			inputRepo: &gitalypb.Repository{
				StorageName:  "non-existent-storage",
				RelativePath: "non-existent-relative-path",
			},
			expectedError: storage.ErrStorageNotFound,
		},
		{
			desc: "non-existent relative-path",
			inputRepo: &gitalypb.Repository{
				StorageName:   cfg.Storages[0].Name,
				RelativePath:  "non-existent-relative-path",
				GlRepository:  "project-1",
				GlProjectPath: "project/path",
			},
			expectedRepo: &gitalypb.Repository{
				StorageName:                   cfg.Storages[0].Name,
				RelativePath:                  "non-existent-relative-path",
				GitObjectDirectory:            "../quarantine",
				GitAlternateObjectDirectories: []string{"objects"},
				GlRepository:                  "project-1",
				GlProjectPath:                 "project/path",
			},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			ctx := testhelper.Context(t)

			catfileCache := catfile.NewCache(cfg)
			defer catfileCache.Stop()

			repo := New(
				testhelper.NewLogger(t),
				config.NewLocator(cfg),
				gittest.NewCommandFactory(t, cfg),
				catfileCache,
				tc.inputRepo,
			)

			quarantinedRepo, err := repo.Quarantine(ctx, quarantineDir)
			if tc.expectedError != nil {
				require.ErrorIs(t, err, tc.expectedError)
				return
			}

			require.NoError(t, err)
			testhelper.ProtoEqual(t, tc.expectedRepo, quarantinedRepo.Repository)
		})
	}
}

func TestRepo_QuarantineOnly(t *testing.T) {
	t.Parallel()

	cfg := testcfg.Build(t)
	catfileCache := catfile.NewCache(cfg)
	defer catfileCache.Stop()

	ctx := testhelper.Context(t)
	repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
		SkipCreationViaService: true,
	})

	unquarantinedRepo := New(
		testhelper.NewLogger(t),
		config.NewLocator(cfg),
		gittest.NewCommandFactory(t, cfg),
		catfileCache,
		repoProto,
	)

	t.Run("fails with unquarantined repository", func(t *testing.T) {
		t.Parallel()

		_, err := unquarantinedRepo.QuarantineOnly()
		require.Equal(t, err, errors.New("repository wasn't quarantined"))
	})

	t.Run("returns the repository with only the quarantine directory", func(t *testing.T) {
		t.Parallel()

		quarantinedRepo, err := unquarantinedRepo.Quarantine(ctx, filepath.Join(repoPath, "quarantine-directory"))
		require.NoError(t, err)

		expectedRepo := &gitalypb.Repository{
			StorageName:                   repoProto.GetStorageName(),
			RelativePath:                  repoProto.GetRelativePath(),
			GlRepository:                  repoProto.GetGlRepository(),
			GlProjectPath:                 repoProto.GetGlProjectPath(),
			GitObjectDirectory:            "quarantine-directory",
			GitAlternateObjectDirectories: []string{"objects"},
		}

		testhelper.ProtoEqual(t, expectedRepo, quarantinedRepo.Repository)

		onlyQuarantineRepo, err := quarantinedRepo.QuarantineOnly()
		require.NoError(t, err)

		expectedRepo.GitAlternateObjectDirectories = nil
		testhelper.ProtoEqual(t, expectedRepo, onlyQuarantineRepo.Repository)
	})
}

func TestRepo_StorageTempDir(t *testing.T) {
	ctx := testhelper.Context(t)
	cfg := testcfg.Build(t)

	gitCmdFactory := gittest.NewCommandFactory(t, cfg)
	catfileCache := catfile.NewCache(cfg)
	t.Cleanup(catfileCache.Stop)
	locator := config.NewLocator(cfg)

	repoProto, _ := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
		SkipCreationViaService: true,
	})
	repo := New(testhelper.NewLogger(t), locator, gitCmdFactory, catfileCache, repoProto)

	expected, err := locator.TempDir(cfg.Storages[0].Name)
	require.NoError(t, err)
	require.NoDirExists(t, expected)

	tempPath, err := repo.StorageTempDir()
	require.NoError(t, err)
	require.DirExists(t, expected)
	require.Equal(t, expected, tempPath)
}

func TestRepo_ObjectHash(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg := testcfg.Build(t)

	catfileCache := catfile.NewCache(cfg)
	t.Cleanup(catfileCache.Stop)
	locator := config.NewLocator(cfg)

	repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
		SkipCreationViaService: true,
	})

	repo := New(testhelper.NewLogger(t), locator, gittest.NewCommandFactory(t, cfg), catfileCache, repoProto)

	objectHash, err := repo.ObjectHash(ctx)
	require.NoError(t, err)
	require.Equal(t, gittest.DefaultObjectHash.EmptyTreeOID, objectHash.EmptyTreeOID)

	// The first call to ObjectHash should have cached the value. Set the object format to an unsupported
	// one so it would break if we read it for the second time and didn't use the cached value.
	gittest.Exec(t, cfg, "-C", repoPath, "config", "extensions.objectformat", "unsupported-hash")

	// Verify that running this a second time continues to return the object hash alright
	// regardless of the cache.
	objectHash, err = repo.ObjectHash(ctx)
	require.NoError(t, err)
	require.Equal(t, gittest.DefaultObjectHash.EmptyTreeOID, objectHash.EmptyTreeOID)
}
