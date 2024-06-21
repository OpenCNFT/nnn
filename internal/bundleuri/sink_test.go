package bundleuri

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v16/internal/helper/perm"
	"gitlab.com/gitlab-org/gitaly/v16/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper/testcfg"
)

func TestSink_Generate(t *testing.T) {
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
			sink, err := NewSink(ctx, "file://"+sinkDir)
			require.NoError(t, err)

			err = sink.Generate(ctx, repo)
			if tc.expectedErr == nil {
				require.NoError(t, err)
				require.FileExists(t, filepath.Join(sinkDir, sink.relativePath(repo, "default")))
			} else {
				require.Equal(t, err, tc.expectedErr, err)
			}
		})
	}
}

func TestSink_SignedURL(t *testing.T) {
	t.Parallel()

	cfg := testcfg.Build(t)
	ctx := testhelper.Context(t)

	repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
		SkipCreationViaService: true,
	})
	repo := localrepo.NewTestRepo(t, cfg, repoProto)

	gittest.WriteCommit(t, cfg, repoPath,
		gittest.WithTreeEntries(gittest.TreeEntry{Mode: "100644", Path: "README", Content: "much"}),
		gittest.WithBranch("main"))

	tempDir := testhelper.TempDir(t)
	keyFile, err := os.Create(filepath.Join(tempDir, "secret.key"))
	require.NoError(t, err)
	_, err = keyFile.WriteString("super-secret-key")
	require.NoError(t, err)
	require.NoError(t, keyFile.Close())

	for _, tc := range []struct {
		desc        string
		setup       func(t *testing.T, sinkDir string, sink *Sink)
		expectedErr error
	}{
		{
			desc: "signs bundle successfully",
			setup: func(t *testing.T, sinkDir string, sink *Sink) {
				path := filepath.Join(sinkDir, sink.relativePath(repo, "default"))
				require.NoError(t, os.MkdirAll(filepath.Dir(path), perm.PrivateDir))
				require.NoError(t, os.WriteFile(path, []byte("hello"), perm.SharedFile))
			},
		},
		{
			desc:        "fails with missing bundle",
			setup:       func(t *testing.T, sinkDir string, sink *Sink) {},
			expectedErr: ErrBundleNotFound,
		},
	} {
		tc := tc

		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			sinkDir := t.TempDir()
			sink, err := NewSink(ctx, "file://"+sinkDir+"?base_url=http://example.com&secret_key_path="+keyFile.Name())
			require.NoError(t, err)

			tc.setup(t, sinkDir, sink)

			uri, err := sink.SignedURL(ctx, repoProto)
			if tc.expectedErr == nil {
				require.NoError(t, err)
				require.Regexp(t, "http://example\\.com", uri)
			} else {
				require.ErrorIs(t, err, tc.expectedErr)
			}
		})
	}
}

func TestSink_GenerateOneAtATime(t *testing.T) {
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

			doneChan := make(chan struct{})
			errChan := make(chan error)
			sinkDir := t.TempDir()
			sink, err := NewSink(
				ctx,
				"file://"+sinkDir,
				WithBundleGenerationNotifier(
					func(_ string, err error) {
						close(doneChan)
						errChan <- err
					},
				),
			)
			require.NoError(t, err)

			go func() {
				err := sink.GenerateOneAtATime(ctx, repo)
				require.NoError(t, err)
			}()

			<-doneChan
			err = <-errChan

			if tc.expectedErr == nil {
				require.NoError(t, err)
				require.FileExists(t, filepath.Join(sinkDir, sink.relativePath(repo, "default")))
			} else {
				require.Equal(t, err, tc.expectedErr, err)
			}
		})
	}
}

func TestSink_GenerateOneAtATimeConcurrent(t *testing.T) {
	t.Parallel()

	cfg := testcfg.Build(t)
	ctx := testhelper.Context(t)

	repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
		SkipCreationViaService: true,
	})
	repo := localrepo.NewTestRepo(t, cfg, repoProto)

	gittest.WriteCommit(t, cfg, repoPath,
		gittest.WithTreeEntries(gittest.TreeEntry{Mode: "100644", Path: "README", Content: "much"}),
		gittest.WithBranch("main"))

	doneChan, startNotifierCh := make(chan struct{}), make(chan struct{})
	errChan := make(chan error)

	sinkDir := t.TempDir()
	sink, err := NewSink(
		ctx,
		"file://"+sinkDir,
		WithBundleGenerationNotifier(
			func(_ string, err error) {
				close(startNotifierCh)
				close(doneChan)
				errChan <- err
			},
		),
	)
	require.NoError(t, err)

	go func() {
		err := sink.GenerateOneAtATime(ctx, repo)
		require.NoError(t, err)
	}()

	<-startNotifierCh

	err = sink.GenerateOneAtATime(ctx, repo)
	require.ErrorIs(t, err, ErrBundleGenerationInProgress)

	<-doneChan
	err = <-errChan

	require.NoError(t, err)
	require.FileExists(t, filepath.Join(sinkDir, sink.relativePath(repo, "default")))
}

func TestSink_GenerateOneAtATime_ContextCancelled(t *testing.T) {
	t.Parallel()

	cfg := testcfg.Build(t)
	ctx := testhelper.Context(t)
	ctx, cancel := context.WithCancel(ctx)

	repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
		SkipCreationViaService: true,
	})
	repo := localrepo.NewTestRepo(t, cfg, repoProto)

	gittest.WriteCommit(t, cfg, repoPath,
		gittest.WithTreeEntries(gittest.TreeEntry{Mode: "100644", Path: "README", Content: "much"}),
		gittest.WithBranch("main"))

	errChan := make(chan error)

	sinkDir := t.TempDir()
	sink, err := NewSink(
		ctx,
		"file://"+sinkDir,
		WithBundleGenerationNotifier(
			func(_ string, err error) {
				errChan <- err
			},
		),
	)
	require.NoError(t, err)

	cancel()

	go func() {
		require.NoError(t, sink.GenerateOneAtATime(ctx, repo))
	}()

	err = <-errChan

	require.ErrorIs(t, err, context.Canceled)
	require.NoFileExists(t, filepath.Join(sinkDir, sink.relativePath(repo, "default")))
}
