package bundleuri

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v16/internal/limiter"
	"gitlab.com/gitlab-org/gitaly/v16/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper/testcfg"
)

type passthroughLimiter struct{}

func (p *passthroughLimiter) Limit(ctx context.Context, lockKey string, f limiter.LimitedFunc) (interface{}, error) {
	return f()
}

func TestBundleGenerationManager_generateOneAtATime(t *testing.T) {
	t.Parallel()

	cfg := testcfg.Build(t)
	ctx := testhelper.Context(t)

	for _, tc := range []struct {
		desc        string
		setup       func(t *testing.T, repoPath string)
		expectedErr error
	}{
		{
			desc: "creates bundle successfully",
			setup: func(t *testing.T, repoPath string) {
				gittest.WriteCommit(t, cfg, repoPath,
					gittest.WithTreeEntries(gittest.TreeEntry{Mode: "100644", Path: "README", Content: "much"}),
					gittest.WithBranch("main"))
			},
		},
		{
			desc:        "fails with missing HEAD",
			setup:       func(t *testing.T, repoPath string) {},
			expectedErr: structerr.NewFailedPrecondition("ref %q does not exist: %w", "refs/heads/main", fmt.Errorf("create bundle: %w", localrepo.ErrEmptyBundle)),
		},
	} {
		tc := tc

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

			manager := NewBundleGenerationManager(sink, &passthroughLimiter{})

			_, err = manager.generateOneAtATime(ctx, repo)

			if tc.expectedErr == nil {
				require.FileExists(t, filepath.Join(sinkDir, sink.relativePath(repo, "default")))
			} else {
				require.Equal(t, tc.expectedErr, err)
			}
		})
	}
}

func TestSink_generateOneAtATime_inProgress(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg := testcfg.Build(t)

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

	manager := NewBundleGenerationManager(sink, &passthroughLimiter{})

	// pretend like there is already another bundle generation happening for
	// this repo.
	bundlePath := sink.relativePath(repo, defaultBundle)
	manager.bundleGenerationInProgress.Store(bundlePath, struct{}{})

	_, err = manager.generateOneAtATime(testhelper.Context(t), repo)
	require.Equal(t, ErrBundleGenerationInProgress, err)
}
