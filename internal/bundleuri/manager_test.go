package bundleuri

import (
	"fmt"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v16/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper/testcfg"
)

func TestGenerationManager_GenerateIfAboveThreshold(t *testing.T) {
	t.Parallel()

	cfg := testcfg.Build(t)
	ctx := testhelper.Context(t)

	for _, tc := range []struct {
		desc             string
		concurrencyLimit int
		threshold        uint
		setup            func(t *testing.T, repoPath string)
		expectedErr      error
		expectFileExist  bool
	}{
		{
			desc:             "creates bundle successfully",
			concurrencyLimit: 1,
			threshold:        0,
			expectFileExist:  true,
			setup: func(t *testing.T, repoPath string) {
				gittest.WriteCommit(t, cfg, repoPath,
					gittest.WithTreeEntries(gittest.TreeEntry{Mode: "100644", Path: "README", Content: "much"}),
					gittest.WithBranch("main"))
			},
		},
		{
			desc:             "fails with missing HEAD",
			concurrencyLimit: 1,
			expectFileExist:  false,
			setup:            func(t *testing.T, repoPath string) {},
			expectedErr:      structerr.NewFailedPrecondition("ref %q does not exist: %w", "refs/heads/main", fmt.Errorf("create bundle: %w", localrepo.ErrEmptyBundle)),
		},
		{
			desc:             "below threshold",
			concurrencyLimit: 1,
			threshold:        2,
			expectFileExist:  false,
			setup: func(t *testing.T, repoPath string) {
				gittest.WriteCommit(t, cfg, repoPath,
					gittest.WithTreeEntries(gittest.TreeEntry{Mode: "100644", Path: "README", Content: "much"}),
					gittest.WithBranch("main"))
			},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
				SkipCreationViaService: true,
			})
			repo := localrepo.NewTestRepo(t, cfg, repoProto)

			tc.setup(t, repoPath)

			sinkDir := t.TempDir()
			sink, err := NewSink(
				ctx,
				"file://"+sinkDir,
			)
			require.NoError(t, err)

			logger := testhelper.NewLogger(t)
			hook := testhelper.AddLoggerHook(logger)

			manager := NewGenerationManager(sink, logger, tc.concurrencyLimit, tc.threshold, NewInProgressTracker())

			err = manager.GenerateIfAboveThreshold(repo, func() error {
				manager.wg.Wait()
				return nil
			})
			require.NoError(t, err)

			if tc.expectedErr != nil {
				e := hook.LastEntry().Data["error"].(error)
				require.ErrorIs(t, e, tc.expectedErr)
				return
			}

			if tc.expectFileExist {
				require.FileExists(t, filepath.Join(sinkDir, sink.relativePath(repo, "default")))
				return
			}
			require.NoFileExists(t, filepath.Join(sinkDir, sink.relativePath(repo, "default")))
		})
	}

	t.Run("current bundle generation in progress", func(t *testing.T) {
		sinkDir := t.TempDir()
		sink, err := NewSink(
			ctx,
			"file://"+sinkDir,
		)
		require.NoError(t, err)

		repoProto, _ := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
			SkipCreationViaService: true,
		})
		repo := localrepo.NewTestRepo(t, cfg, repoProto)

		manager := NewGenerationManager(sink, testhelper.NewLogger(t), 1, 1, NewInProgressTracker())

		// pretend like there is already another bundle generation happening for
		// this repo.
		bundlePath := sink.relativePath(repo, defaultBundle)
		manager.bundleGenerationInProgress[bundlePath] = struct{}{}

		err = manager.GenerateIfAboveThreshold(repo, func() error {
			manager.wg.Wait()
			return nil
		})
		require.NoError(t, err)
		require.NoFileExists(t, filepath.Join(sinkDir, bundlePath))
	})

	t.Run("concurrency of bundle generation reached", func(t *testing.T) {
		sinkDir := t.TempDir()
		sink, err := NewSink(
			ctx,
			"file://"+sinkDir,
		)
		require.NoError(t, err)

		repoProto, _ := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
			SkipCreationViaService: true,
		})
		repo := localrepo.NewTestRepo(t, cfg, repoProto)

		manager := NewGenerationManager(sink, testhelper.NewLogger(t), 2, 1, NewInProgressTracker())

		// pretend like there is already another bundle generation happening for
		// another repo
		manager.bundleGenerationInProgress["other path"] = struct{}{}
		manager.bundleGenerationInProgress["another path"] = struct{}{}

		err = manager.GenerateIfAboveThreshold(repo, func() error {
			manager.wg.Wait()
			return nil
		})
		require.NoError(t, err)
		require.NoFileExists(t, filepath.Join(sinkDir, sink.relativePath(repo, "default")))
	})
}
