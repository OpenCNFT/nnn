package updateref_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v16/internal/featureflag"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/gitcmd"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/quarantine"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/hook"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/hook/updateref"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/service"
	hookservice "gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/service/hook"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper/testserver"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
	"google.golang.org/grpc"
)

func TestUpdaterWithHooks_UpdateReference_invalidParameters(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg := testcfg.Build(t)
	gitCmdFactory := gittest.NewCommandFactory(t, cfg)
	repo, _ := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
		SkipCreationViaService: true,
	})

	revA := git.ObjectID(strings.Repeat("a", gittest.DefaultObjectHash.EncodedLen()))
	revB := git.ObjectID(strings.Repeat("b", gittest.DefaultObjectHash.EncodedLen()))

	updater := updateref.NewUpdaterWithHooks(cfg, testhelper.NewLogger(t), config.NewLocator(cfg), &hook.MockManager{}, gitCmdFactory, nil)

	testCases := []struct {
		desc           string
		ref            git.ReferenceName
		newRev, oldRev git.ObjectID
		expectedErr    error
	}{
		{
			desc:        "missing reference",
			oldRev:      revA,
			newRev:      revB,
			expectedErr: fmt.Errorf("reference cannot be empty"),
		},
		{
			desc:        "missing old rev",
			ref:         "refs/heads/master",
			newRev:      revB,
			expectedErr: fmt.Errorf("validating old value: %w", git.InvalidObjectIDLengthError{OID: "", CorrectLength: gittest.DefaultObjectHash.EncodedLen(), Length: 0}),
		},
		{
			desc:        "missing new rev",
			ref:         "refs/heads/master",
			oldRev:      revB,
			expectedErr: fmt.Errorf("validating new value: %w", git.InvalidObjectIDLengthError{OID: "", CorrectLength: gittest.DefaultObjectHash.EncodedLen(), Length: 0}),
		},
		{
			desc:        "invalid old rev",
			ref:         "refs/heads/master",
			newRev:      revA,
			oldRev:      "foobar",
			expectedErr: fmt.Errorf("validating old value: %w", git.InvalidObjectIDLengthError{OID: "foobar", CorrectLength: gittest.DefaultObjectHash.EncodedLen(), Length: 6}),
		},
		{
			desc:        "invalid new rev",
			ref:         "refs/heads/master",
			newRev:      "foobar",
			oldRev:      revB,
			expectedErr: fmt.Errorf("validating new value: %w", git.InvalidObjectIDLengthError{OID: "foobar", CorrectLength: gittest.DefaultObjectHash.EncodedLen(), Length: 6}),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			err := updater.UpdateReference(ctx, repo, gittest.TestUser, nil, tc.ref, tc.newRev, tc.oldRev)
			require.Equal(t, tc.expectedErr, err)
		})
	}
}

func TestUpdaterWithHooks_UpdateReference(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg := testcfg.Build(t)

	// We need to set up a separate "real" hook service here, as it will be used in
	// git-update-ref(1) spawned by `updateRefWithHooks()`
	testserver.RunGitalyServer(t, cfg, func(srv *grpc.Server, deps *service.Dependencies) {
		gitalypb.RegisterHookServiceServer(srv, hookservice.NewServer(deps))
	})

	repo, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
		SkipCreationViaService: true,
	})
	commitID := gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("main"))

	requirePayload := func(t *testing.T, env []string) {
		require.Len(t, env, 1)

		expectedPayload := gitcmd.NewHooksPayload(
			cfg,
			repo,
			gittest.DefaultObjectHash,
			nil,
			&gitcmd.UserDetails{
				UserID:   gittest.TestUser.GetGlId(),
				Username: gittest.TestUser.GetGlUsername(),
				Protocol: "web",
			},
			gitcmd.ReceivePackHooks,
			featureflag.FromContext(ctx),
			storage.ExtractTransactionID(ctx),
		)

		actualPayload, err := gitcmd.HooksPayloadFromEnv(env)
		require.NoError(t, err)

		// Flags aren't sorted, so we just verify they contain the same elements.
		require.ElementsMatch(t, expectedPayload.FeatureFlagsWithValue, actualPayload.FeatureFlagsWithValue)
		expectedPayload.FeatureFlagsWithValue = nil
		actualPayload.FeatureFlagsWithValue = nil

		testhelper.ProtoEqual(t, expectedPayload, actualPayload)
	}

	referenceTransactionCalls := 0
	testCases := []struct {
		desc                 string
		preReceive           func(t *testing.T, ctx context.Context, repo *gitalypb.Repository, pushOptions, env []string, stdin io.Reader, stdout, stderr io.Writer) error
		postReceive          func(t *testing.T, ctx context.Context, repo *gitalypb.Repository, pushOptions, env []string, stdin io.Reader, stdout, stderr io.Writer) error
		update               func(t *testing.T, ctx context.Context, repo *gitalypb.Repository, ref, oldValue, newValue string, env []string, stdout, stderr io.Writer) error
		referenceTransaction func(t *testing.T, ctx context.Context, state hook.ReferenceTransactionState, env []string, stdin io.Reader) error
		expectedErr          string
		expectedRefDeletion  bool
	}{
		{
			desc: "successful update",
			preReceive: func(t *testing.T, ctx context.Context, repo *gitalypb.Repository, pushOptions, env []string, stdin io.Reader, stdout, stderr io.Writer) error {
				changes, err := io.ReadAll(stdin)
				require.NoError(t, err)
				require.Equal(t, fmt.Sprintf("%s %s refs/heads/main\n", commitID, gittest.DefaultObjectHash.ZeroOID.String()), string(changes))
				require.Empty(t, pushOptions)
				requirePayload(t, env)
				return nil
			},
			update: func(t *testing.T, ctx context.Context, repo *gitalypb.Repository, ref, oldValue, newValue string, env []string, stdout, stderr io.Writer) error {
				require.Equal(t, "refs/heads/main", ref)
				require.Equal(t, commitID.String(), oldValue)
				require.Equal(t, newValue, gittest.DefaultObjectHash.ZeroOID.String())
				requirePayload(t, env)
				return nil
			},
			postReceive: func(t *testing.T, ctx context.Context, repo *gitalypb.Repository, pushOptions, env []string, stdin io.Reader, stdout, stderr io.Writer) error {
				changes, err := io.ReadAll(stdin)
				require.NoError(t, err)
				require.Equal(t, fmt.Sprintf("%s %s refs/heads/main\n", commitID.String(), gittest.DefaultObjectHash.ZeroOID.String()), string(changes))
				requirePayload(t, env)
				require.Empty(t, pushOptions)
				return nil
			},
			referenceTransaction: func(t *testing.T, ctx context.Context, state hook.ReferenceTransactionState, env []string, stdin io.Reader) error {
				changes, err := io.ReadAll(stdin)
				require.NoError(t, err)
				require.Equal(t, fmt.Sprintf("%s %s refs/heads/main\n", commitID.String(), gittest.DefaultObjectHash.ZeroOID.String()), string(changes))

				require.Less(t, referenceTransactionCalls, 2)
				if referenceTransactionCalls == 0 {
					require.Equal(t, state, hook.ReferenceTransactionPrepared)
				} else {
					require.Equal(t, state, hook.ReferenceTransactionCommitted)
				}
				referenceTransactionCalls++

				requirePayload(t, env)
				return nil
			},
			expectedRefDeletion: true,
		},
		{
			desc: "prereceive error",
			preReceive: func(t *testing.T, ctx context.Context, repo *gitalypb.Repository, pushOptions, env []string, stdin io.Reader, stdout, stderr io.Writer) error {
				_, err := io.Copy(stderr, strings.NewReader("prereceive failure"))
				require.NoError(t, err)
				return errors.New("ignored")
			},
			expectedErr: "prereceive failure",
		},
		{
			desc: "prereceive error from GitLab API response",
			preReceive: func(t *testing.T, ctx context.Context, repo *gitalypb.Repository, pushOptions, env []string, stdin io.Reader, stdout, stderr io.Writer) error {
				return hook.NotAllowedError{Message: "GitLab: file is locked"}
			},
			expectedErr: "GitLab: file is locked",
		},
		{
			desc: "update error",
			preReceive: func(t *testing.T, ctx context.Context, repo *gitalypb.Repository, pushOptions, env []string, stdin io.Reader, stdout, stderr io.Writer) error {
				return nil
			},
			update: func(t *testing.T, ctx context.Context, repo *gitalypb.Repository, ref, oldValue, newValue string, env []string, stdout, stderr io.Writer) error {
				_, err := io.Copy(stderr, strings.NewReader("update failure"))
				require.NoError(t, err)
				return errors.New("ignored")
			},
			expectedErr: "update failure",
		},
		{
			desc: "reference-transaction error",
			preReceive: func(t *testing.T, ctx context.Context, repo *gitalypb.Repository, pushOptions, env []string, stdin io.Reader, stdout, stderr io.Writer) error {
				return nil
			},
			update: func(t *testing.T, ctx context.Context, repo *gitalypb.Repository, ref, oldValue, newValue string, env []string, stdout, stderr io.Writer) error {
				return nil
			},
			referenceTransaction: func(t *testing.T, ctx context.Context, state hook.ReferenceTransactionState, env []string, stdin io.Reader) error {
				// The reference-transaction hook doesn't execute any custom hooks,
				// which is why it currently doesn't have any stdout/stderr.
				// Instead, errors are directly returned.
				return errors.New("reference-transaction failure")
			},
			expectedErr: "reference-transaction failure",
		},
		{
			desc: "post-receive custom hooks error is ignored",
			preReceive: func(t *testing.T, ctx context.Context, repo *gitalypb.Repository, pushOptions, env []string, stdin io.Reader, stdout, stderr io.Writer) error {
				return nil
			},
			update: func(t *testing.T, ctx context.Context, repo *gitalypb.Repository, ref, oldValue, newValue string, env []string, stdout, stderr io.Writer) error {
				return nil
			},
			referenceTransaction: func(t *testing.T, ctx context.Context, state hook.ReferenceTransactionState, env []string, stdin io.Reader) error {
				return nil
			},
			postReceive: func(t *testing.T, ctx context.Context, repo *gitalypb.Repository, pushOptions, env []string, stdin io.Reader, stdout, stderr io.Writer) error {
				_, err := io.Copy(stderr, strings.NewReader("post-receive failure"))
				require.NoError(t, err)
				return hook.NewCustomHookError(errors.New("ignored"))
			},
			expectedRefDeletion: true,
		},
		{
			desc: "post-receive non-custom hooks error returned",
			preReceive: func(t *testing.T, ctx context.Context, repo *gitalypb.Repository, pushOptions, env []string, stdin io.Reader, stdout, stderr io.Writer) error {
				return nil
			},
			update: func(t *testing.T, ctx context.Context, repo *gitalypb.Repository, ref, oldValue, newValue string, env []string, stdout, stderr io.Writer) error {
				return nil
			},
			referenceTransaction: func(t *testing.T, ctx context.Context, state hook.ReferenceTransactionState, env []string, stdin io.Reader) error {
				return nil
			},
			postReceive: func(t *testing.T, ctx context.Context, repo *gitalypb.Repository, pushOptions, env []string, stdin io.Reader, stdout, stderr io.Writer) error {
				_, err := io.Copy(stderr, strings.NewReader("post-receive failure"))
				require.NoError(t, err)
				return errors.New("uh oh")
			},
			expectedRefDeletion: true,
			expectedErr:         "running post-receive hooks: uh oh",
		},
	}

	for _, tc := range testCases {
		referenceTransactionCalls = 0
		t.Run(tc.desc, func(t *testing.T) {
			hookManager := hook.NewMockManager(t, tc.preReceive, tc.postReceive, tc.update, tc.referenceTransaction, hook.NewProcReceiveRegistry())

			gitCmdFactory := gittest.NewCommandFactory(t, cfg)
			updater := updateref.NewUpdaterWithHooks(cfg, testhelper.NewLogger(t), config.NewLocator(cfg), hookManager, gitCmdFactory, nil)

			err := updater.UpdateReference(ctx, repo, gittest.TestUser, nil, git.ReferenceName("refs/heads/main"), gittest.DefaultObjectHash.ZeroOID, commitID)
			if tc.expectedErr == "" {
				require.NoError(t, err)
			} else {
				require.Contains(t, err.Error(), tc.expectedErr)
			}

			if tc.expectedRefDeletion {
				contained, err := localrepo.NewTestRepo(t, cfg, repo).HasRevision(ctx, git.Revision("refs/heads/main"))
				require.NoError(t, err)
				require.False(t, contained, "branch should have been deleted")
				gittest.Exec(t, cfg, "-C", repoPath, "branch", "main", commitID.String())
			} else {
				ref, err := localrepo.NewTestRepo(t, cfg, repo).GetReference(ctx, "refs/heads/main")
				require.NoError(t, err)
				require.Equal(t, ref.Target, commitID.String())
			}
		})
	}
}

func TestUpdaterWithHooks_quarantine(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg := testcfg.Build(t)
	gitCmdFactory := gittest.NewCommandFactory(t, cfg)
	locator := config.NewLocator(cfg)

	repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
		SkipCreationViaService: true,
	})
	commitID := gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("main"))

	unquarantinedRepo := localrepo.NewTestRepo(t, cfg, repoProto)

	quarantine, err := quarantine.New(ctx, repoProto, testhelper.NewLogger(t), locator)
	require.NoError(t, err)
	quarantinedRepo := localrepo.NewTestRepo(t, cfg, quarantine.QuarantinedRepo())
	blobID, err := quarantinedRepo.WriteBlob(ctx, strings.NewReader("1834298812398123"), localrepo.WriteBlobConfig{})
	require.NoError(t, err)

	expectQuarantined := func(t *testing.T, env []string, quarantined bool) {
		t.Helper()

		if env != nil {
			payload, err := gitcmd.HooksPayloadFromEnv(env)
			require.NoError(t, err)
			if quarantined {
				testhelper.ProtoEqual(t, quarantine.QuarantinedRepo(), payload.Repo)
			} else {
				testhelper.ProtoEqual(t, repoProto, payload.Repo)
			}
		}

		exists, err := quarantinedRepo.HasRevision(ctx, blobID.Revision()+"^{blob}")
		require.NoError(t, err)
		require.Equal(t, quarantined, exists)

		exists, err = unquarantinedRepo.HasRevision(ctx, blobID.Revision()+"^{blob}")
		require.NoError(t, err)
		require.Equal(t, !quarantined, exists)
	}

	hookExecutions := make(map[string]int)
	hookManager := hook.NewMockManager(t,
		// The pre-receive hook is not expected to have the object in the normal repo.
		func(t *testing.T, ctx context.Context, repo *gitalypb.Repository, pushOptions, env []string, stdin io.Reader, stdout, stderr io.Writer) error {
			expectQuarantined(t, env, true)
			testhelper.ProtoEqual(t, quarantine.QuarantinedRepo(), repo)
			hookExecutions["prereceive"]++
			return nil
		},
		// But the post-receive hook shall get the unquarantined repository as input, with
		// objects already having been migrated into the target repo.
		func(t *testing.T, ctx context.Context, repo *gitalypb.Repository, pushOptions, env []string, stdin io.Reader, stdout, stderr io.Writer) error {
			expectQuarantined(t, env, false)
			testhelper.ProtoEqual(t, repoProto, repo)
			hookExecutions["postreceive"]++
			return nil
		},
		// The update hook gets executed after the pre-receive hook and will be executed for
		// each reference that we're updating. As it is called immediately before the ref
		// gets queued for update, objects must have already been migrated or otherwise
		// updating the refs will fail due to missing objects.
		func(t *testing.T, ctx context.Context, repo *gitalypb.Repository, ref, oldValue, newValue string, env []string, stdout, stderr io.Writer) error {
			expectQuarantined(t, env, false)
			testhelper.ProtoEqual(t, quarantine.QuarantinedRepo(), repo)
			hookExecutions["update"]++
			return nil
		},
		// The reference-transaction hook is called as we're queueing refs for update, so
		// the objects must be part of the main object database or otherwise the update will
		// fail due to missing objects.
		func(t *testing.T, ctx context.Context, state hook.ReferenceTransactionState, env []string, stdin io.Reader) error {
			expectQuarantined(t, env, false)
			switch state {
			case hook.ReferenceTransactionPrepared:
				hookExecutions["prepare"]++
			case hook.ReferenceTransactionCommitted:
				hookExecutions["commit"]++
			}
			return nil
		},
		hook.NewProcReceiveRegistry(),
	)

	require.NoError(t, updateref.NewUpdaterWithHooks(cfg, testhelper.NewLogger(t), locator, hookManager, gitCmdFactory, nil).UpdateReference(
		ctx,
		repoProto,
		&gitalypb.User{
			GlId:       "1234",
			GlUsername: "Username",
			Name:       []byte("Name"),
			Email:      []byte("mail@example.com"),
		},
		quarantine,
		git.ReferenceName("refs/heads/main"),
		gittest.DefaultObjectHash.ZeroOID,
		commitID,
	))

	require.Equal(t, map[string]int{
		"prereceive":  1,
		"postreceive": 1,
		"update":      1,
		"prepare":     1,
		"commit":      1,
	}, hookExecutions)

	contained, err := unquarantinedRepo.HasRevision(ctx, git.Revision("refs/heads/main"))
	require.NoError(t, err)
	require.False(t, contained, "branch should have been deleted")
}
