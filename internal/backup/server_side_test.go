package backup_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v16/internal/backup"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/service/setup"
	"gitlab.com/gitlab-org/gitaly/v16/internal/grpc/client"
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
		repo        *gitalypb.Repository
		backupID    string
		incremental bool
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
			desc: "success - incremental",
			setup: func(t *testing.T, ctx context.Context, cfg config.Cfg) setupData {
				repo, repoPath := gittest.CreateRepository(t, ctx, cfg)
				gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch(git.DefaultBranch))

				return setupData{
					repo:        repo,
					backupID:    "abc123",
					incremental: true,
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
			expectedErr: fmt.Errorf("server-side create: %w: rpc error: code = NotFound desc = repository not found", backup.ErrSkipped),
		},
	} {
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
				Incremental:      data.incremental,
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
		setup       func(t *testing.T, ctx context.Context, cfg config.Cfg, backupSink *backup.Sink, backupLocator backup.Locator) setupData
		expectedErr error
	}{
		{
			desc: "success",
			setup: func(t *testing.T, ctx context.Context, cfg config.Cfg, backupSink *backup.Sink, backupLocator backup.Locator) setupData {
				_, templateRepoPath := gittest.CreateRepository(t, ctx, cfg)
				oid := gittest.WriteCommit(t, cfg, templateRepoPath, gittest.WithBranch(git.DefaultBranch))
				gittest.WriteCommit(t, cfg, templateRepoPath, gittest.WithBranch("feature"), gittest.WithParents(oid))
				checksum := gittest.ChecksumRepo(t, cfg, templateRepoPath)

				repo, repoPath := gittest.CreateRepository(t, ctx, cfg)
				backup := backupLocator.BeginFull(ctx, repo, "abc123")
				step := backup.Steps[len(backup.Steps)-1]

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

				backup.ObjectFormat = gittest.DefaultObjectHash.Format

				require.NoError(t, backupLocator.Commit(ctx, backup))

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
			setup: func(t *testing.T, ctx context.Context, cfg config.Cfg, backupSink *backup.Sink, backupLocator backup.Locator) setupData {
				repo, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
					RelativePath: "@test/restore/latest/missing.git",
				})

				return setupData{
					repo:     repo,
					repoPath: repoPath,
					backupID: "",
				}
			},
			expectedErr: structerr.NewInternal("server-side restore: restore repository: manifest: find latest: read manifest: sink: new reader for \"manifests/default/@test/restore/latest/missing.git/+latest.toml\": doesn't exist"),
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			backupRoot := testhelper.TempDir(t)
			backupSink, err := backup.ResolveSink(ctx, backupRoot)
			require.NoError(t, err)

			backupLocator, err := backup.ResolveLocator("manifest", backupSink)
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
