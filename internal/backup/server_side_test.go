package backup_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v16/client"
	"gitlab.com/gitlab-org/gitaly/v16/internal/backup"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/service/setup"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v16/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper/testserver"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
)

func TestServerSideAdapter_Create(t *testing.T) {
	t.Parallel()
	ctx := testhelper.Context(t)

	type setupData struct {
		repo     *gitalypb.Repository
		backupID string
	}

	for _, tc := range []struct {
		desc        string
		setup       func(t *testing.T, ctx context.Context, cfg config.Cfg) setupData
		expectedErr error
	}{
		{
			desc: "success",
			setup: func(t *testing.T, ctx context.Context, cfg config.Cfg) setupData {
				repo, repoPath := gittest.CreateRepository(t, ctx, cfg)
				gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch(git.DefaultBranch))

				return setupData{
					repo:     repo,
					backupID: "abc123",
				}
			},
		},
		{
			desc: "missing backup ID",
			setup: func(t *testing.T, ctx context.Context, cfg config.Cfg) setupData {
				repo, repoPath := gittest.CreateRepository(t, ctx, cfg)
				gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch(git.DefaultBranch))

				return setupData{
					repo:     repo,
					backupID: "",
				}
			},
			expectedErr: structerr.NewInvalidArgument("server-side create: rpc error: code = InvalidArgument desc = empty BackupId"),
		},
		{
			desc: "repository with no branches",
			setup: func(t *testing.T, ctx context.Context, cfg config.Cfg) setupData {
				repo, _ := gittest.CreateRepository(t, ctx, cfg)

				return setupData{
					repo:     repo,
					backupID: "abc123",
				}
			},
		},
		{
			desc: "repository does not exist",
			setup: func(t *testing.T, ctx context.Context, cfg config.Cfg) setupData {
				repo := &gitalypb.Repository{
					StorageName:  cfg.Storages[0].Name,
					RelativePath: gittest.NewRepositoryName(t),
				}

				return setupData{
					repo:     repo,
					backupID: "abc123",
				}
			},
			expectedErr: fmt.Errorf("server-side create: not found: %w", backup.ErrSkipped),
		},
	} {
		tc := tc

		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			backupRoot := testhelper.TempDir(t)
			backupSink, err := backup.ResolveSink(ctx, backupRoot)
			require.NoError(t, err)

			backupLocator, err := backup.ResolveLocator("pointer", backupSink)
			require.NoError(t, err)

			cfg := testcfg.Build(t)
			cfg.SocketPath = testserver.RunGitalyServer(t, cfg, setup.RegisterAll,
				testserver.WithBackupSink(backupSink),
				testserver.WithBackupLocator(backupLocator),
			)

			pool := client.NewPool()
			defer testhelper.MustClose(t, pool)

			adapter := backup.NewServerSideAdapter(pool)

			data := tc.setup(t, ctx, cfg)

			ctx := testhelper.MergeIncomingMetadata(ctx, testcfg.GitalyServersMetadataFromCfg(t, cfg))

			err = adapter.Create(ctx, &backup.CreateRequest{
				Repository:       data.repo,
				VanityRepository: data.repo,
				BackupID:         data.backupID,
			})
			if tc.expectedErr != nil {
				testhelper.RequireGrpcError(t, tc.expectedErr, err)
				return
			}

			require.NoError(t, err)
		})
	}
}

func TestServerSideAdapter_Restore(t *testing.T) {
	t.Parallel()
	ctx := testhelper.Context(t)

	type setupData struct {
		repo             *gitalypb.Repository
		repoPath         string
		backupID         string
		expectedChecksum *git.Checksum
	}

	for _, tc := range []struct {
		desc        string
		setup       func(t *testing.T, ctx context.Context, cfg config.Cfg, backupSink backup.Sink, backupLocator backup.Locator) setupData
		expectedErr error
	}{
		{
			desc: "success",
			setup: func(t *testing.T, ctx context.Context, cfg config.Cfg, backupSink backup.Sink, backupLocator backup.Locator) setupData {
				_, templateRepoPath := gittest.CreateRepository(t, ctx, cfg)
				oid := gittest.WriteCommit(t, cfg, templateRepoPath, gittest.WithBranch(git.DefaultBranch))
				gittest.WriteCommit(t, cfg, templateRepoPath, gittest.WithBranch("feature"), gittest.WithParents(oid))
				checksum := gittest.ChecksumRepo(t, cfg, templateRepoPath)

				repo, repoPath := gittest.CreateRepository(t, ctx, cfg)
				step := backupLocator.BeginFull(ctx, repo, "abc123")

				w, err := backupSink.GetWriter(ctx, step.BundlePath)
				require.NoError(t, err)
				bundle := gittest.BundleRepo(t, cfg, templateRepoPath, "-")
				_, err = w.Write(bundle)
				require.NoError(t, err)
				require.NoError(t, w.Close())

				w, err = backupSink.GetWriter(ctx, step.RefPath)
				require.NoError(t, err)
				refs := gittest.Exec(t, cfg, "-C", templateRepoPath, "show-ref", "--head")
				_, err = w.Write(refs)
				require.NoError(t, err)
				require.NoError(t, w.Close())

				require.NoError(t, backupLocator.Commit(ctx, step))

				return setupData{
					repo:             repo,
					repoPath:         repoPath,
					backupID:         "abc123",
					expectedChecksum: checksum,
				}
			},
		},
		{
			desc: "missing bundle",
			setup: func(t *testing.T, ctx context.Context, cfg config.Cfg, backupSink backup.Sink, backupLocator backup.Locator) setupData {
				repo, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
					RelativePath: "@test/restore/latest/missing.git",
				})

				return setupData{
					repo:     repo,
					repoPath: repoPath,
					backupID: "",
				}
			},
			expectedErr: structerr.NewFailedPrecondition("server-side restore: restore repository: manager: repository skipped: restore bundle: \"@test/restore/latest/missing.bundle\": doesn't exist").WithDetail(
				&gitalypb.RestoreRepositoryResponse_SkippedError{},
			),
		},
	} {
		tc := tc

		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			backupRoot := testhelper.TempDir(t)
			backupSink, err := backup.ResolveSink(ctx, backupRoot)
			require.NoError(t, err)

			backupLocator, err := backup.ResolveLocator("pointer", backupSink)
			require.NoError(t, err)

			cfg := testcfg.Build(t)
			cfg.SocketPath = testserver.RunGitalyServer(t, cfg, setup.RegisterAll,
				testserver.WithBackupSink(backupSink),
				testserver.WithBackupLocator(backupLocator),
			)

			pool := client.NewPool()
			defer testhelper.MustClose(t, pool)

			adapter := backup.NewServerSideAdapter(pool)

			data := tc.setup(t, ctx, cfg, backupSink, backupLocator)

			ctx := testhelper.MergeIncomingMetadata(ctx, testcfg.GitalyServersMetadataFromCfg(t, cfg))

			err = adapter.Restore(ctx, &backup.RestoreRequest{
				Repository:       data.repo,
				VanityRepository: data.repo,
				BackupID:         data.backupID,
			})
			if tc.expectedErr != nil {
				testhelper.RequireGrpcError(t, tc.expectedErr, err)
				return
			}

			require.NoError(t, err)
		})
	}
}

func TestServerSideAdapter_RemoveAllRepositories(t *testing.T) {
	t.Parallel()

	backupRoot := testhelper.TempDir(t)
	sink := backup.NewFilesystemSink(backupRoot)
	defer testhelper.MustClose(t, sink)

	locator, err := backup.ResolveLocator("pointer", sink)
	require.NoError(t, err)

	cfg := testcfg.Build(t)
	cfg.SocketPath = testserver.RunGitalyServer(t, cfg, setup.RegisterAll,
		testserver.WithBackupSink(sink),
		testserver.WithBackupLocator(locator),
	)

	ctx := testhelper.Context(t)

	repo, repoPath := gittest.CreateRepository(t, ctx, cfg)
	gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("main"))

	pool := client.NewPool()
	defer testhelper.MustClose(t, pool)

	adapter := backup.NewServerSideAdapter(pool)
	err = adapter.RemoveAllRepositories(ctx, &backup.RemoveAllRepositoriesRequest{
		Server:      storage.ServerInfo{Address: cfg.SocketPath, Token: cfg.Auth.Token},
		StorageName: repo.StorageName,
	})
	require.NoError(t, err)
}
