//go:build !gitaly_test_sha256

package backup_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v16/client"
	"gitlab.com/gitlab-org/gitaly/v16/internal/archive"
	"gitlab.com/gitlab-org/gitaly/v16/internal/backup"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/catfile"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/service/setup"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/transaction"
	"gitlab.com/gitlab-org/gitaly/v16/internal/helper/perm"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper/testserver"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
	"google.golang.org/protobuf/proto"
)

func TestManager_RemoveAllRepositories(t *testing.T) {
	t.Parallel()

	const backupID = "abc123"

	cfg := testcfg.Build(t)
	cfg.SocketPath = testserver.RunGitalyServer(t, cfg, setup.RegisterAll)

	ctx := testhelper.Context(t)

	repo, repoPath := gittest.CreateRepository(t, ctx, cfg)
	commitID := gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("main"))
	gittest.WriteTag(t, cfg, repoPath, "v1.0.0", commitID.Revision())

	pool := client.NewPool()
	defer testhelper.MustClose(t, pool)

	backupRoot := testhelper.TempDir(t)
	sink := backup.NewFilesystemSink(backupRoot)
	locator, err := backup.ResolveLocator("pointer", sink)
	require.NoError(t, err)

	fsBackup := backup.NewManager(sink, locator, pool, backupID)
	err = fsBackup.RemoveAllRepositories(ctx, &backup.RemoveAllRepositoriesRequest{
		Server:      storage.ServerInfo{Address: cfg.SocketPath, Token: cfg.Auth.Token},
		StorageName: repo.StorageName,
	})
	require.NoError(t, err)
}

func TestManager_Create(t *testing.T) {
	t.Parallel()

	const backupID = "abc123"

	cfg := testcfg.Build(t)

	cfg.SocketPath = testserver.RunGitalyServer(t, cfg, setup.RegisterAll)

	ctx := testhelper.Context(t)

	for _, managerTC := range []struct {
		desc  string
		setup func(t testing.TB, sink backup.Sink, locator backup.Locator) *backup.Manager
	}{
		{
			desc: "RPC manager",
			setup: func(tb testing.TB, sink backup.Sink, locator backup.Locator) *backup.Manager {
				pool := client.NewPool()
				tb.Cleanup(func() {
					testhelper.MustClose(tb, pool)
				})

				return backup.NewManager(sink, locator, pool, backupID)
			},
		},
		{
			desc: "Local manager",
			setup: func(tb testing.TB, sink backup.Sink, locator backup.Locator) *backup.Manager {
				if testhelper.IsPraefectEnabled() {
					tb.Skip("local backup manager expects to operate on the local filesystem so cannot operate through praefect")
				}

				storageLocator := config.NewLocator(cfg)
				gitCmdFactory := gittest.NewCommandFactory(tb, cfg)
				catfileCache := catfile.NewCache(cfg)
				tb.Cleanup(catfileCache.Stop)
				txManager := transaction.NewTrackingManager()

				return backup.NewManagerLocal(sink, locator, storageLocator, gitCmdFactory, catfileCache, txManager, backupID)
			},
		},
	} {
		for _, tc := range []struct {
			desc               string
			setup              func(tb testing.TB) (*gitalypb.Repository, string)
			createsBundle      bool
			createsCustomHooks bool
			err                error
		}{
			{
				desc: "no hooks",
				setup: func(tb testing.TB) (*gitalypb.Repository, string) {
					noHooksRepo, repoPath := gittest.CreateRepository(tb, ctx, cfg, gittest.CreateRepositoryConfig{
						Seed: gittest.SeedGitLabTest,
					})
					return noHooksRepo, repoPath
				},
				createsBundle:      true,
				createsCustomHooks: false,
			},
			{
				desc: "hooks",
				setup: func(tb testing.TB) (*gitalypb.Repository, string) {
					hooksRepo, hooksRepoPath := gittest.CreateRepository(tb, ctx, cfg, gittest.CreateRepositoryConfig{
						Seed: gittest.SeedGitLabTest,
					})
					require.NoError(tb, os.Mkdir(filepath.Join(hooksRepoPath, "custom_hooks"), perm.PublicDir))
					require.NoError(tb, os.WriteFile(filepath.Join(hooksRepoPath, "custom_hooks/pre-commit.sample"), []byte("Some hooks"), perm.PublicFile))
					return hooksRepo, hooksRepoPath
				},
				createsBundle:      true,
				createsCustomHooks: true,
			},
			{
				desc: "empty repo",
				setup: func(tb testing.TB) (*gitalypb.Repository, string) {
					emptyRepo, repoPath := gittest.CreateRepository(tb, ctx, cfg)
					return emptyRepo, repoPath
				},
				createsBundle:      false,
				createsCustomHooks: false,
				err:                fmt.Errorf("manager: repository empty: %w", backup.ErrSkipped),
			},
			{
				desc: "nonexistent repo",
				setup: func(tb testing.TB) (*gitalypb.Repository, string) {
					emptyRepo, repoPath := gittest.CreateRepository(tb, ctx, cfg)
					nonexistentRepo := proto.Clone(emptyRepo).(*gitalypb.Repository)
					nonexistentRepo.RelativePath = gittest.NewRepositoryName(t)
					return nonexistentRepo, repoPath
				},
				createsBundle:      false,
				createsCustomHooks: false,
				err:                fmt.Errorf("manager: repository empty: %w", backup.ErrSkipped),
			},
		} {
			t.Run(tc.desc, func(t *testing.T) {
				repo, repoPath := tc.setup(t)
				backupRoot := testhelper.TempDir(t)

				refsPath := joinBackupPath(t, backupRoot, repo, backupID, "001.refs")
				bundlePath := joinBackupPath(t, backupRoot, repo, backupID, "001.bundle")
				customHooksPath := joinBackupPath(t, backupRoot, repo, backupID, "001.custom_hooks.tar")

				sink := backup.NewFilesystemSink(backupRoot)
				locator, err := backup.ResolveLocator("pointer", sink)
				require.NoError(t, err)

				fsBackup := managerTC.setup(t, sink, locator)
				err = fsBackup.Create(ctx, &backup.CreateRequest{
					Server:     storage.ServerInfo{Address: cfg.SocketPath, Token: cfg.Auth.Token},
					Repository: repo,
				})
				if tc.err == nil {
					require.NoError(t, err)
				} else {
					require.Equal(t, tc.err, err)
				}

				if tc.createsBundle {
					require.FileExists(t, refsPath)
					require.FileExists(t, bundlePath)

					dirInfo, err := os.Stat(filepath.Dir(bundlePath))
					require.NoError(t, err)
					require.Equal(t, perm.PrivateDir, dirInfo.Mode().Perm(), "expecting restricted directory permissions")

					bundleInfo, err := os.Stat(bundlePath)
					require.NoError(t, err)
					require.Equal(t, perm.PrivateFile, bundleInfo.Mode().Perm(), "expecting restricted file permissions")

					output := gittest.Exec(t, cfg, "-C", repoPath, "bundle", "verify", bundlePath)
					require.Contains(t, string(output), "The bundle records a complete history")

					expectedRefs := gittest.Exec(t, cfg, "-C", repoPath, "show-ref", "--head")
					actualRefs := testhelper.MustReadFile(t, refsPath)
					require.Equal(t, string(expectedRefs), string(actualRefs))
				} else {
					require.NoFileExists(t, bundlePath)
				}

				if tc.createsCustomHooks {
					require.FileExists(t, customHooksPath)
				} else {
					require.NoFileExists(t, customHooksPath)
				}
			})
		}
	}
}

func TestManager_Create_incremental(t *testing.T) {
	t.Parallel()

	const backupID = "abc123"

	cfg := testcfg.Build(t)

	cfg.SocketPath = testserver.RunGitalyServer(t, cfg, setup.RegisterAll)
	ctx := testhelper.Context(t)

	for _, managerTC := range []struct {
		desc  string
		setup func(t testing.TB, sink backup.Sink, locator backup.Locator) *backup.Manager
	}{
		{
			desc: "RPC manager",
			setup: func(tb testing.TB, sink backup.Sink, locator backup.Locator) *backup.Manager {
				pool := client.NewPool()
				tb.Cleanup(func() {
					testhelper.MustClose(tb, pool)
				})

				return backup.NewManager(sink, locator, pool, backupID)
			},
		},
		{
			desc: "Local manager",
			setup: func(tb testing.TB, sink backup.Sink, locator backup.Locator) *backup.Manager {
				if testhelper.IsPraefectEnabled() {
					tb.Skip("local backup manager expects to operate on the local filesystem so cannot operate through praefect")
				}

				storageLocator := config.NewLocator(cfg)
				gitCmdFactory := gittest.NewCommandFactory(tb, cfg)
				catfileCache := catfile.NewCache(cfg)
				tb.Cleanup(catfileCache.Stop)
				txManager := transaction.NewTrackingManager()

				return backup.NewManagerLocal(sink, locator, storageLocator, gitCmdFactory, catfileCache, txManager, backupID)
			},
		},
	} {
		for _, tc := range []struct {
			desc              string
			setup             func(t testing.TB, backupRoot string) (*gitalypb.Repository, string)
			expectedIncrement string
			expectedErr       error
		}{
			{
				desc: "no previous backup",
				setup: func(tb testing.TB, backupRoot string) (*gitalypb.Repository, string) {
					repo, repoPath := gittest.CreateRepository(tb, ctx, cfg, gittest.CreateRepositoryConfig{
						Seed: gittest.SeedGitLabTest,
					})
					return repo, repoPath
				},
				expectedIncrement: "001",
			},
			{
				desc: "previous backup, no updates",
				setup: func(tb testing.TB, backupRoot string) (*gitalypb.Repository, string) {
					repo, repoPath := gittest.CreateRepository(tb, ctx, cfg, gittest.CreateRepositoryConfig{
						Seed: gittest.SeedGitLabTest,
					})

					backupRepoPath := joinBackupPath(tb, backupRoot, repo)
					backupPath := filepath.Join(backupRepoPath, backupID)
					bundlePath := filepath.Join(backupPath, "001.bundle")
					refsPath := filepath.Join(backupPath, "001.refs")

					require.NoError(tb, os.MkdirAll(backupPath, perm.PublicDir))
					gittest.Exec(tb, cfg, "-C", repoPath, "bundle", "create", bundlePath, "--all")

					refs := gittest.Exec(tb, cfg, "-C", repoPath, "show-ref", "--head")
					require.NoError(tb, os.WriteFile(refsPath, refs, perm.PublicFile))

					require.NoError(tb, os.WriteFile(filepath.Join(backupRepoPath, "LATEST"), []byte(backupID), perm.PublicFile))
					require.NoError(tb, os.WriteFile(filepath.Join(backupPath, "LATEST"), []byte("001"), perm.PublicFile))

					return repo, repoPath
				},
				expectedErr: fmt.Errorf("manager: %w", fmt.Errorf("write bundle: %w: no changes to bundle", backup.ErrSkipped)),
			},
			{
				desc: "previous backup, updates",
				setup: func(tb testing.TB, backupRoot string) (*gitalypb.Repository, string) {
					repo, repoPath := gittest.CreateRepository(tb, ctx, cfg, gittest.CreateRepositoryConfig{
						Seed: gittest.SeedGitLabTest,
					})

					backupRepoPath := joinBackupPath(tb, backupRoot, repo)
					backupPath := filepath.Join(backupRepoPath, backupID)
					bundlePath := filepath.Join(backupPath, "001.bundle")
					refsPath := filepath.Join(backupPath, "001.refs")

					require.NoError(tb, os.MkdirAll(backupPath, perm.PublicDir))
					gittest.Exec(tb, cfg, "-C", repoPath, "bundle", "create", bundlePath, "--all")

					refs := gittest.Exec(tb, cfg, "-C", repoPath, "show-ref", "--head")
					require.NoError(tb, os.WriteFile(refsPath, refs, perm.PublicFile))

					require.NoError(tb, os.WriteFile(filepath.Join(backupRepoPath, "LATEST"), []byte(backupID), perm.PublicFile))
					require.NoError(tb, os.WriteFile(filepath.Join(backupPath, "LATEST"), []byte("001"), perm.PublicFile))

					gittest.WriteCommit(tb, cfg, repoPath, gittest.WithBranch("master"))

					return repo, repoPath
				},
				expectedIncrement: "002",
			},
		} {
			t.Run(tc.desc, func(t *testing.T) {
				backupRoot := testhelper.TempDir(t)
				repo, repoPath := tc.setup(t, backupRoot)

				refsPath := joinBackupPath(t, backupRoot, repo, backupID, tc.expectedIncrement+".refs")
				bundlePath := joinBackupPath(t, backupRoot, repo, backupID, tc.expectedIncrement+".bundle")

				sink := backup.NewFilesystemSink(backupRoot)
				locator, err := backup.ResolveLocator("pointer", sink)
				require.NoError(t, err)

				fsBackup := managerTC.setup(t, sink, locator)
				err = fsBackup.Create(ctx, &backup.CreateRequest{
					Server:      storage.ServerInfo{Address: cfg.SocketPath, Token: cfg.Auth.Token},
					Repository:  repo,
					Incremental: true,
				})
				if tc.expectedErr == nil {
					require.NoError(t, err)
				} else {
					require.Equal(t, tc.expectedErr, err)
					return
				}

				require.FileExists(t, refsPath)
				require.FileExists(t, bundlePath)

				expectedRefs := gittest.Exec(t, cfg, "-C", repoPath, "show-ref", "--head")
				actualRefs := testhelper.MustReadFile(t, refsPath)
				require.Equal(t, string(expectedRefs), string(actualRefs))
			})
		}
	}
}

func TestManager_Restore_latest(t *testing.T) {
	t.Parallel()

	cfg := testcfg.Build(t)
	testcfg.BuildGitalyHooks(t, cfg)
	cfg.SocketPath = testserver.RunGitalyServer(t, cfg, setup.RegisterAll)

	for _, managerTC := range []struct {
		desc  string
		setup func(t testing.TB, sink backup.Sink, locator backup.Locator) *backup.Manager
	}{
		{
			desc: "RPC manager",
			setup: func(tb testing.TB, sink backup.Sink, locator backup.Locator) *backup.Manager {
				pool := client.NewPool()
				tb.Cleanup(func() {
					testhelper.MustClose(tb, pool)
				})

				return backup.NewManager(sink, locator, pool, "")
			},
		},
		{
			desc: "Local manager",
			setup: func(tb testing.TB, sink backup.Sink, locator backup.Locator) *backup.Manager {
				if testhelper.IsPraefectEnabled() {
					tb.Skip("local backup manager expects to operate on the local filesystem so cannot operate through praefect")
				}

				storageLocator := config.NewLocator(cfg)
				gitCmdFactory := gittest.NewCommandFactory(tb, cfg)
				catfileCache := catfile.NewCache(cfg)
				tb.Cleanup(catfileCache.Stop)
				txManager := transaction.NewTrackingManager()

				return backup.NewManagerLocal(sink, locator, storageLocator, gitCmdFactory, catfileCache, txManager, "")
			},
		},
	} {
		managerTC := managerTC

		t.Run(managerTC.desc, func(t *testing.T) {
			t.Parallel()

			ctx := testhelper.Context(t)

			cc, err := client.Dial(cfg.SocketPath, nil)
			require.NoError(t, err)
			defer testhelper.MustClose(t, cc)

			repoClient := gitalypb.NewRepositoryServiceClient(cc)

			_, repoPath := gittest.CreateRepository(t, ctx, cfg)
			commitID := gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("main"))
			gittest.WriteTag(t, cfg, repoPath, "v1.0.0", commitID.Revision())
			repoChecksum := gittest.ChecksumRepo(t, cfg, repoPath)

			backupRoot := testhelper.TempDir(t)

			for _, tc := range []struct {
				desc          string
				locators      []string
				setup         func(tb testing.TB) (*gitalypb.Repository, *git.Checksum)
				alwaysCreate  bool
				expectExists  bool
				expectedPaths []string
				expectedErrAs error
			}{
				{
					desc:     "existing repo, without hooks",
					locators: []string{"legacy", "pointer"},
					setup: func(tb testing.TB) (*gitalypb.Repository, *git.Checksum) {
						repo, _ := gittest.CreateRepository(t, ctx, cfg)

						relativePath := stripRelativePath(tb, repo)
						require.NoError(tb, os.MkdirAll(filepath.Join(backupRoot, relativePath), perm.PublicDir))
						bundlePath := filepath.Join(backupRoot, relativePath+".bundle")
						gittest.BundleRepo(tb, cfg, repoPath, bundlePath)

						return repo, repoChecksum
					},
					expectExists: true,
				},
				{
					desc:     "existing repo, with hooks",
					locators: []string{"legacy", "pointer"},
					setup: func(tb testing.TB) (*gitalypb.Repository, *git.Checksum) {
						repo, _ := gittest.CreateRepository(t, ctx, cfg)

						relativePath := stripRelativePath(tb, repo)
						bundlePath := filepath.Join(backupRoot, relativePath+".bundle")
						customHooksPath := filepath.Join(backupRoot, relativePath, "custom_hooks.tar")
						require.NoError(tb, os.MkdirAll(filepath.Join(backupRoot, relativePath), perm.PublicDir))
						gittest.BundleRepo(tb, cfg, repoPath, bundlePath)
						testhelper.CopyFile(tb, mustCreateCustomHooksArchive(t, ctx), customHooksPath)

						return repo, repoChecksum
					},
					expectedPaths: []string{
						"custom_hooks/pre-commit.sample",
						"custom_hooks/prepare-commit-msg.sample",
						"custom_hooks/pre-push.sample",
					},
					expectExists: true,
				},
				{
					desc:     "missing bundle",
					locators: []string{"legacy", "pointer"},
					setup: func(tb testing.TB) (*gitalypb.Repository, *git.Checksum) {
						repo, _ := gittest.CreateRepository(t, ctx, cfg)
						return repo, nil
					},
					expectedErrAs: backup.ErrSkipped,
				},
				{
					desc:     "missing bundle, always create",
					locators: []string{"legacy", "pointer"},
					setup: func(tb testing.TB) (*gitalypb.Repository, *git.Checksum) {
						repo, _ := gittest.CreateRepository(t, ctx, cfg)
						return repo, new(git.Checksum)
					},
					alwaysCreate: true,
					expectExists: true,
				},
				{
					desc:     "nonexistent repo",
					locators: []string{"legacy", "pointer"},
					setup: func(tb testing.TB) (*gitalypb.Repository, *git.Checksum) {
						repo := &gitalypb.Repository{
							StorageName:  "default",
							RelativePath: gittest.NewRepositoryName(tb),
						}

						relativePath := stripRelativePath(tb, repo)
						require.NoError(tb, os.MkdirAll(filepath.Dir(filepath.Join(backupRoot, relativePath)), perm.PublicDir))
						bundlePath := filepath.Join(backupRoot, relativePath+".bundle")
						gittest.BundleRepo(tb, cfg, repoPath, bundlePath)

						return repo, repoChecksum
					},
					expectExists: true,
				},
				{
					desc:     "single incremental",
					locators: []string{"pointer"},
					setup: func(tb testing.TB) (*gitalypb.Repository, *git.Checksum) {
						const backupID = "abc123"
						repo, _ := gittest.CreateRepository(t, ctx, cfg)
						repoBackupPath := joinBackupPath(tb, backupRoot, repo)
						backupPath := filepath.Join(repoBackupPath, backupID)
						require.NoError(tb, os.MkdirAll(backupPath, perm.PublicDir))
						require.NoError(tb, os.WriteFile(filepath.Join(repoBackupPath, "LATEST"), []byte(backupID), perm.PublicFile))
						require.NoError(tb, os.WriteFile(filepath.Join(backupPath, "LATEST"), []byte("001"), perm.PublicFile))
						bundlePath := filepath.Join(backupPath, "001.bundle")
						gittest.BundleRepo(tb, cfg, repoPath, bundlePath)

						return repo, repoChecksum
					},
					expectExists: true,
				},
				{
					desc:     "many incrementals",
					locators: []string{"pointer"},
					setup: func(tb testing.TB) (*gitalypb.Repository, *git.Checksum) {
						const backupID = "abc123"

						_, expectedRepoPath := gittest.CreateRepository(t, ctx, cfg)

						repo, _ := gittest.CreateRepository(t, ctx, cfg)
						repoBackupPath := joinBackupPath(tb, backupRoot, repo)
						backupPath := filepath.Join(repoBackupPath, backupID)
						require.NoError(tb, os.MkdirAll(backupPath, perm.PublicDir))
						require.NoError(tb, os.WriteFile(filepath.Join(repoBackupPath, "LATEST"), []byte(backupID), perm.PublicFile))
						require.NoError(tb, os.WriteFile(filepath.Join(backupPath, "LATEST"), []byte("002"), perm.PublicFile))

						root := gittest.WriteCommit(tb, cfg, expectedRepoPath,
							gittest.WithBranch("master"),
						)
						master1 := gittest.WriteCommit(tb, cfg, expectedRepoPath,
							gittest.WithBranch("master"),
							gittest.WithParents(root),
						)
						other := gittest.WriteCommit(tb, cfg, expectedRepoPath,
							gittest.WithBranch("other"),
							gittest.WithParents(root),
						)
						gittest.Exec(tb, cfg, "-C", expectedRepoPath, "symbolic-ref", "HEAD", "refs/heads/master")
						bundlePath1 := filepath.Join(backupPath, "001.bundle")
						gittest.Exec(tb, cfg, "-C", expectedRepoPath, "bundle", "create", bundlePath1,
							"HEAD",
							"refs/heads/master",
							"refs/heads/other",
						)

						master2 := gittest.WriteCommit(tb, cfg, expectedRepoPath,
							gittest.WithBranch("master"),
							gittest.WithParents(master1),
						)
						bundlePath2 := filepath.Join(backupPath, "002.bundle")
						gittest.Exec(tb, cfg, "-C", expectedRepoPath, "bundle", "create", bundlePath2,
							"HEAD",
							"^"+master1.String(),
							"^"+other.String(),
							"refs/heads/master",
							"refs/heads/other",
						)

						checksum := new(git.Checksum)
						checksum.Add(git.NewReference("HEAD", master2.String()))
						checksum.Add(git.NewReference("refs/heads/master", master2.String()))
						checksum.Add(git.NewReference("refs/heads/other", other.String()))

						return repo, checksum
					},
					expectExists: true,
				},
			} {
				t.Run(tc.desc, func(t *testing.T) {
					require.GreaterOrEqual(t, len(tc.locators), 1, "each test case must specify a locator")

					for _, locatorName := range tc.locators {
						t.Run(locatorName, func(t *testing.T) {
							repo, expectedChecksum := tc.setup(t)

							sink := backup.NewFilesystemSink(backupRoot)
							locator, err := backup.ResolveLocator(locatorName, sink)
							require.NoError(t, err)

							fsBackup := managerTC.setup(t, sink, locator)
							err = fsBackup.Restore(ctx, &backup.RestoreRequest{
								Server:       storage.ServerInfo{Address: cfg.SocketPath, Token: cfg.Auth.Token},
								Repository:   repo,
								AlwaysCreate: tc.alwaysCreate,
							})
							if tc.expectedErrAs != nil {
								require.ErrorAs(t, err, &tc.expectedErrAs)
							} else {
								require.NoError(t, err)
							}

							exists, err := repoClient.RepositoryExists(ctx, &gitalypb.RepositoryExistsRequest{
								Repository: repo,
							})
							require.NoError(t, err)
							require.Equal(t, tc.expectExists, exists.Exists, "repository exists")

							if expectedChecksum != nil {
								checksum, err := repoClient.CalculateChecksum(ctx, &gitalypb.CalculateChecksumRequest{
									Repository: repo,
								})
								require.NoError(t, err)

								require.Equal(t, expectedChecksum.String(), checksum.GetChecksum())
							}

							if len(tc.expectedPaths) > 0 {
								// Restore has to use the rewritten path as the relative path due to the test creating
								// the repository through Praefect. In order to get to the correct disk paths, we need
								// to get the replica path of the rewritten repository.
								repoPath := filepath.Join(cfg.Storages[0].Path, gittest.GetReplicaPath(t, ctx, cfg, repo))
								for _, p := range tc.expectedPaths {
									require.FileExists(t, filepath.Join(repoPath, p))
								}
							}
						})
					}
				})
			}
		})
	}
}

func TestManager_Restore_specific(t *testing.T) {
	t.Parallel()

	const backupID = "abc123"

	cfg := testcfg.Build(t)
	testcfg.BuildGitalyHooks(t, cfg)
	cfg.SocketPath = testserver.RunGitalyServer(t, cfg, setup.RegisterAll)

	for _, managerTC := range []struct {
		desc  string
		setup func(t testing.TB, sink backup.Sink, locator backup.Locator) *backup.Manager
	}{
		{
			desc: "RPC manager",
			setup: func(tb testing.TB, sink backup.Sink, locator backup.Locator) *backup.Manager {
				pool := client.NewPool()
				tb.Cleanup(func() {
					testhelper.MustClose(tb, pool)
				})

				return backup.NewManager(sink, locator, pool, backupID)
			},
		},
		{
			desc: "Local manager",
			setup: func(tb testing.TB, sink backup.Sink, locator backup.Locator) *backup.Manager {
				if testhelper.IsPraefectEnabled() {
					tb.Skip("local backup manager expects to operate on the local filesystem so cannot operate through praefect")
				}

				storageLocator := config.NewLocator(cfg)
				gitCmdFactory := gittest.NewCommandFactory(tb, cfg)
				catfileCache := catfile.NewCache(cfg)
				tb.Cleanup(catfileCache.Stop)
				txManager := transaction.NewTrackingManager()

				return backup.NewManagerLocal(sink, locator, storageLocator, gitCmdFactory, catfileCache, txManager, backupID)
			},
		},
	} {
		managerTC := managerTC

		t.Run(managerTC.desc, func(t *testing.T) {
			t.Parallel()

			ctx := testhelper.Context(t)

			cc, err := client.Dial(cfg.SocketPath, nil)
			require.NoError(t, err)
			defer testhelper.MustClose(t, cc)

			repoClient := gitalypb.NewRepositoryServiceClient(cc)

			_, repoPath := gittest.CreateRepository(t, ctx, cfg)
			commitID := gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("main"))
			gittest.WriteTag(t, cfg, repoPath, "v1.0.0", commitID.Revision())
			repoChecksum := gittest.ChecksumRepo(t, cfg, repoPath)

			backupRoot := testhelper.TempDir(t)

			for _, tc := range []struct {
				desc          string
				setup         func(tb testing.TB) (*gitalypb.Repository, *git.Checksum)
				alwaysCreate  bool
				expectExists  bool
				expectedPaths []string
				expectedErrAs error
			}{
				{
					desc: "single incremental",
					setup: func(tb testing.TB) (*gitalypb.Repository, *git.Checksum) {
						repo, _ := gittest.CreateRepository(t, ctx, cfg)
						repoBackupPath := joinBackupPath(tb, backupRoot, repo)
						backupPath := filepath.Join(repoBackupPath, backupID)
						require.NoError(tb, os.MkdirAll(backupPath, perm.PublicDir))
						require.NoError(tb, os.WriteFile(filepath.Join(backupPath, "LATEST"), []byte("001"), perm.PublicFile))
						bundlePath := filepath.Join(backupPath, "001.bundle")
						gittest.BundleRepo(tb, cfg, repoPath, bundlePath)

						return repo, repoChecksum
					},
					expectExists: true,
				},
				{
					desc: "many incrementals",
					setup: func(tb testing.TB) (*gitalypb.Repository, *git.Checksum) {
						_, expectedRepoPath := gittest.CreateRepository(t, ctx, cfg)

						repo, _ := gittest.CreateRepository(t, ctx, cfg)
						repoBackupPath := joinBackupPath(tb, backupRoot, repo)
						backupPath := filepath.Join(repoBackupPath, backupID)
						require.NoError(tb, os.MkdirAll(backupPath, perm.PublicDir))
						require.NoError(tb, os.WriteFile(filepath.Join(backupPath, "LATEST"), []byte("002"), perm.PublicFile))

						root := gittest.WriteCommit(tb, cfg, expectedRepoPath,
							gittest.WithBranch("master"),
						)
						master1 := gittest.WriteCommit(tb, cfg, expectedRepoPath,
							gittest.WithBranch("master"),
							gittest.WithParents(root),
						)
						other := gittest.WriteCommit(tb, cfg, expectedRepoPath,
							gittest.WithBranch("other"),
							gittest.WithParents(root),
						)
						gittest.Exec(tb, cfg, "-C", expectedRepoPath, "symbolic-ref", "HEAD", "refs/heads/master")
						bundlePath1 := filepath.Join(backupPath, "001.bundle")
						gittest.Exec(tb, cfg, "-C", expectedRepoPath, "bundle", "create", bundlePath1,
							"HEAD",
							"refs/heads/master",
							"refs/heads/other",
						)

						master2 := gittest.WriteCommit(tb, cfg, expectedRepoPath,
							gittest.WithBranch("master"),
							gittest.WithParents(master1),
						)
						bundlePath2 := filepath.Join(backupPath, "002.bundle")
						gittest.Exec(tb, cfg, "-C", expectedRepoPath, "bundle", "create", bundlePath2,
							"HEAD",
							"^"+master1.String(),
							"^"+other.String(),
							"refs/heads/master",
							"refs/heads/other",
						)

						checksum := new(git.Checksum)
						checksum.Add(git.NewReference("HEAD", master2.String()))
						checksum.Add(git.NewReference("refs/heads/master", master2.String()))
						checksum.Add(git.NewReference("refs/heads/other", other.String()))

						return repo, checksum
					},
					expectExists: true,
				},
			} {
				t.Run(tc.desc, func(t *testing.T) {
					repo, expectedChecksum := tc.setup(t)

					sink := backup.NewFilesystemSink(backupRoot)
					locator, err := backup.ResolveLocator("pointer", sink)
					require.NoError(t, err)

					fsBackup := managerTC.setup(t, sink, locator)
					err = fsBackup.Restore(ctx, &backup.RestoreRequest{
						Server:       storage.ServerInfo{Address: cfg.SocketPath, Token: cfg.Auth.Token},
						Repository:   repo,
						AlwaysCreate: tc.alwaysCreate,
					})
					if tc.expectedErrAs != nil {
						require.ErrorAs(t, err, &tc.expectedErrAs)
					} else {
						require.NoError(t, err)
					}

					exists, err := repoClient.RepositoryExists(ctx, &gitalypb.RepositoryExistsRequest{
						Repository: repo,
					})
					require.NoError(t, err)
					require.Equal(t, tc.expectExists, exists.Exists, "repository exists")

					if expectedChecksum != nil {
						checksum, err := repoClient.CalculateChecksum(ctx, &gitalypb.CalculateChecksumRequest{
							Repository: repo,
						})
						require.NoError(t, err)

						require.Equal(t, expectedChecksum.String(), checksum.GetChecksum())
					}

					if len(tc.expectedPaths) > 0 {
						// Restore has to use the rewritten path as the relative path due to the test creating
						// the repository through Praefect. In order to get to the correct disk paths, we need
						// to get the replica path of the rewritten repository.
						repoPath := filepath.Join(cfg.Storages[0].Path, gittest.GetReplicaPath(t, ctx, cfg, repo))
						for _, p := range tc.expectedPaths {
							require.FileExists(t, filepath.Join(repoPath, p))
						}
					}
				})
			}
		})
	}
}

func TestManager_CreateRestore_contextServerInfo(t *testing.T) {
	t.Parallel()

	cfg := testcfg.Build(t)
	testcfg.BuildGitalyHooks(t, cfg)
	cfg.SocketPath = testserver.RunGitalyServer(t, cfg, setup.RegisterAll)

	ctx := testhelper.Context(t)

	repo, repoPath := gittest.CreateRepository(t, ctx, cfg)
	commitID := gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("main"))
	gittest.WriteTag(t, cfg, repoPath, "v1.0.0", commitID.Revision())

	backupRoot := testhelper.TempDir(t)

	pool := client.NewPool()
	defer testhelper.MustClose(t, pool)

	sink := backup.NewFilesystemSink(backupRoot)
	locator, err := backup.ResolveLocator("pointer", sink)
	require.NoError(t, err)

	fsBackup := backup.NewManager(sink, locator, pool, "unused-backup-id")

	ctx = testhelper.MergeIncomingMetadata(ctx, testcfg.GitalyServersMetadataFromCfg(t, cfg))

	require.NoError(t, fsBackup.Create(ctx, &backup.CreateRequest{
		Repository: repo,
	}))
	require.NoError(t, fsBackup.Restore(ctx, &backup.RestoreRequest{
		Repository: repo,
	}))
}

func TestResolveLocator(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		layout      string
		expectedErr string
	}{
		{layout: "legacy"},
		{layout: "pointer"},
		{
			layout:      "unknown",
			expectedErr: "unknown layout: \"unknown\"",
		},
	} {
		t.Run(tc.layout, func(t *testing.T) {
			l, err := backup.ResolveLocator(tc.layout, nil)

			if tc.expectedErr == "" {
				require.NoError(t, err)
			} else {
				require.EqualError(t, err, tc.expectedErr)
				return
			}

			require.NotNil(t, l)
		})
	}
}

func joinBackupPath(tb testing.TB, backupRoot string, repo *gitalypb.Repository, elements ...string) string {
	return filepath.Join(append([]string{
		backupRoot,
		stripRelativePath(tb, repo),
	}, elements...)...)
}

func stripRelativePath(tb testing.TB, repo *gitalypb.Repository) string {
	return strings.TrimSuffix(repo.GetRelativePath(), ".git")
}

func mustCreateCustomHooksArchive(t *testing.T, ctx context.Context) string {
	t.Helper()

	tmpDir := testhelper.TempDir(t)

	hooksDirPath := filepath.Join(tmpDir, "custom_hooks")
	require.NoError(t, os.Mkdir(hooksDirPath, os.ModePerm))

	require.NoError(t, os.WriteFile(filepath.Join(hooksDirPath, "pre-commit.sample"), []byte("foo"), os.ModePerm))
	require.NoError(t, os.WriteFile(filepath.Join(hooksDirPath, "prepare-commit-msg.sample"), []byte("bar"), os.ModePerm))
	require.NoError(t, os.WriteFile(filepath.Join(hooksDirPath, "pre-push.sample"), []byte("baz"), os.ModePerm))

	archivePath := filepath.Join(tmpDir, "custom_hooks.tar")
	file, err := os.Create(archivePath)
	require.NoError(t, err)
	defer testhelper.MustClose(t, file)

	require.NoError(t, archive.WriteTarball(ctx, file, tmpDir, "custom_hooks"))

	return archivePath
}
