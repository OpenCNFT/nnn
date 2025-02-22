package hook

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v16/internal/featureflag"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/gitcmd"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/quarantine"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/storagemgr"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/transaction"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitlab"
	"gitlab.com/gitlab-org/gitaly/v16/internal/grpc/backchannel"
	"gitlab.com/gitlab-org/gitaly/v16/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v16/internal/transaction/txinfo"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
)

func TestPrereceive_customHooks(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg := testcfg.Build(t)

	repo, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
		SkipCreationViaService: true,
	})

	gitCmdFactory := gittest.NewCommandFactory(t, cfg)
	locator := config.NewLocator(cfg)

	txManager := transaction.NewTrackingManager()
	hookManager := NewManager(cfg, locator, testhelper.SharedLogger(t), gitCmdFactory, txManager, gitlab.NewMockClient(
		t, gitlab.MockAllowed, gitlab.MockPreReceive, gitlab.MockPostReceive,
	), NewTransactionRegistry(storagemgr.NewTransactionRegistry()), NewProcReceiveRegistry(), nil)

	receiveHooksPayload := &gitcmd.UserDetails{
		UserID:   "1234",
		Username: "user",
		Protocol: "web",
	}

	payload, err := gitcmd.NewHooksPayload(
		cfg,
		repo,
		gittest.DefaultObjectHash,
		nil,
		receiveHooksPayload,
		gitcmd.PreReceiveHook,
		featureflag.FromContext(ctx),
		storage.ExtractTransactionID(ctx),
	).Env()
	require.NoError(t, err)

	primaryPayload, err := gitcmd.NewHooksPayload(
		cfg,
		repo,
		gittest.DefaultObjectHash,
		&txinfo.Transaction{
			ID: 1234, Node: "primary", Primary: true,
		},
		receiveHooksPayload,
		gitcmd.PreReceiveHook,
		featureflag.FromContext(ctx),
		storage.ExtractTransactionID(ctx),
	).Env()
	require.NoError(t, err)

	secondaryPayload, err := gitcmd.NewHooksPayload(
		cfg,
		repo,
		gittest.DefaultObjectHash,
		&txinfo.Transaction{
			ID: 1234, Node: "secondary", Primary: false,
		},
		receiveHooksPayload,
		gitcmd.PreReceiveHook,
		featureflag.FromContext(ctx),
		storage.ExtractTransactionID(ctx),
	).Env()
	require.NoError(t, err)

	testCases := []struct {
		desc           string
		env            []string
		pushOptions    []string
		hook           string
		stdin          string
		expectedErr    string
		expectedStdout string
		expectedStderr string
		expectedVotes  []transaction.PhasedVote
	}{
		{
			desc:           "hook receives environment variables",
			env:            []string{payload},
			hook:           "#!/bin/sh\nenv | grep -v -e '^SHLVL=' -e '^_=' | sort\n",
			stdin:          "change\n",
			expectedStdout: strings.Join(getExpectedEnv(t, ctx, locator, gitCmdFactory, repo), "\n") + "\n",
			expectedVotes:  []transaction.PhasedVote{},
		},
		{
			desc:        "hook receives push options",
			env:         []string{payload},
			pushOptions: []string{"mr.create", "mr.merge_when_pipeline_succeeds"},
			hook:        "#!/bin/sh\nenv | grep '^GIT_PUSH_' | sort\n",
			stdin:       "change\n",
			expectedStdout: strings.Join([]string{
				"GIT_PUSH_OPTION_0=mr.create",
				"GIT_PUSH_OPTION_1=mr.merge_when_pipeline_succeeds",
				"GIT_PUSH_OPTION_COUNT=2",
			}, "\n") + "\n",
			expectedVotes: []transaction.PhasedVote{},
		},
		{
			desc:           "hook can write to stderr and stdout",
			env:            []string{payload},
			hook:           "#!/bin/sh\necho foo >&1 && echo bar >&2\n",
			stdin:          "change\n",
			expectedStdout: "foo\n",
			expectedStderr: "bar\n",
			expectedVotes:  []transaction.PhasedVote{},
		},
		{
			desc:           "hook receives standard input",
			env:            []string{payload},
			hook:           "#!/bin/sh\ncat\n",
			stdin:          "foo\n",
			expectedStdout: "foo\n",
			expectedVotes:  []transaction.PhasedVote{},
		},
		{
			desc:           "hook succeeds without consuming stdin",
			env:            []string{payload},
			hook:           "#!/bin/sh\necho foo\n",
			stdin:          "ignore me\n",
			expectedStdout: "foo\n",
			expectedVotes:  []transaction.PhasedVote{},
		},
		{
			desc:          "invalid hook results in error",
			env:           []string{payload},
			hook:          "",
			stdin:         "change\n",
			expectedErr:   "exec format error",
			expectedVotes: []transaction.PhasedVote{},
		},
		{
			desc:          "failing hook results in error",
			env:           []string{payload},
			hook:          "#!/bin/sh\nexit 123",
			stdin:         "change\n",
			expectedErr:   "exit status 123",
			expectedVotes: []transaction.PhasedVote{},
		},
		{
			desc:           "hook is executed on primary",
			env:            []string{primaryPayload},
			hook:           "#!/bin/sh\necho foo\n",
			stdin:          "change\n",
			expectedStdout: "foo\n",
			expectedVotes:  []transaction.PhasedVote{synchronizedVote("pre-receive")},
		},
		{
			desc:          "hook is not executed on secondary",
			env:           []string{secondaryPayload},
			hook:          "#!/bin/sh\necho foo\n",
			stdin:         "change\n",
			expectedVotes: []transaction.PhasedVote{synchronizedVote("pre-receive")},
		},
		{
			desc:          "missing changes cause error",
			env:           []string{payload},
			expectedErr:   "hook got no reference updates",
			expectedVotes: []transaction.PhasedVote{},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			txManager.Reset()

			gittest.WriteCustomHook(t, repoPath, "pre-receive", []byte(tc.hook))

			var stdout, stderr bytes.Buffer
			err = hookManager.PreReceiveHook(ctx, repo, tc.pushOptions, tc.env, strings.NewReader(tc.stdin), &stdout, &stderr)

			if tc.expectedErr != "" {
				require.Contains(t, err.Error(), tc.expectedErr)
			} else {
				require.NoError(t, err)
			}

			require.Equal(t, tc.expectedStdout, stdout.String())
			require.Equal(t, tc.expectedStderr, stderr.String())
			require.Equal(t, tc.expectedVotes, txManager.Votes())
		})
	}
}

func TestPrereceive_quarantine(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg := testcfg.Build(t)

	repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
		SkipCreationViaService: true,
	})

	quarantine, err := quarantine.New(ctx, repoProto, testhelper.SharedLogger(t), config.NewLocator(cfg))
	require.NoError(t, err)

	quarantinedRepo := localrepo.NewTestRepo(t, cfg, quarantine.QuarantinedRepo())
	blobID, err := quarantinedRepo.WriteBlob(ctx, strings.NewReader("allyourbasearebelongtous"), localrepo.WriteBlobConfig{})
	require.NoError(t, err)

	hookManager := NewManager(cfg, config.NewLocator(cfg), testhelper.SharedLogger(t), gittest.NewCommandFactory(t, cfg), nil, gitlab.NewMockClient(
		t, gitlab.MockAllowed, gitlab.MockPreReceive, gitlab.MockPostReceive,
	), NewTransactionRegistry(storagemgr.NewTransactionRegistry()), NewProcReceiveRegistry(), nil)

	//nolint:gitaly-linters
	gittest.WriteCustomHook(t, repoPath, "pre-receive", []byte(fmt.Sprintf(
		`#!/bin/sh
		git cat-file -p '%s' || true
	`, blobID.String())))

	for repo, isQuarantined := range map[*gitalypb.Repository]bool{
		quarantine.QuarantinedRepo(): true,
		repoProto:                    false,
	} {
		t.Run(fmt.Sprintf("quarantined: %v", isQuarantined), func(t *testing.T) {
			env, err := gitcmd.NewHooksPayload(
				cfg,
				repo,
				gittest.DefaultObjectHash,
				nil,
				&gitcmd.UserDetails{
					UserID:   "1234",
					Username: "user",
					Protocol: "web",
				},
				gitcmd.PreReceiveHook,
				featureflag.FromContext(ctx),
				storage.ExtractTransactionID(ctx),
			).Env()
			require.NoError(t, err)

			stdin := strings.NewReader(fmt.Sprintf("%s %s refs/heads/master",
				gittest.DefaultObjectHash.ZeroOID, gittest.DefaultObjectHash.ZeroOID))

			var stdout, stderr bytes.Buffer
			require.NoError(t, hookManager.PreReceiveHook(ctx, repo, nil,
				[]string{env}, stdin, &stdout, &stderr))

			if isQuarantined {
				require.Equal(t, "allyourbasearebelongtous", stdout.String())
				require.Empty(t, stderr.String())
			} else {
				require.Empty(t, stdout.String())
				require.Contains(t, stderr.String(), "Not a valid object name")
			}
		})
	}
}

type prereceiveAPIMock struct {
	allowed    func(context.Context, gitlab.AllowedParams) (bool, string, error)
	prereceive func(context.Context, string) (bool, error)
}

func (m *prereceiveAPIMock) Allowed(ctx context.Context, params gitlab.AllowedParams) (bool, string, error) {
	return m.allowed(ctx, params)
}

func (m *prereceiveAPIMock) PreReceive(ctx context.Context, glRepository string) (bool, error) {
	return m.prereceive(ctx, glRepository)
}

func (m *prereceiveAPIMock) Check(ctx context.Context) (*gitlab.CheckInfo, error) {
	return nil, errors.New("unexpected call")
}

func (m *prereceiveAPIMock) PostReceive(context.Context, string, string, string, ...string) (bool, []gitlab.PostReceiveMessage, error) {
	return true, nil, errors.New("unexpected call")
}

func TestPrereceive_gitlab(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg := testcfg.Build(t)

	repo, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
		SkipCreationViaService: true,
	})

	payload, err := gitcmd.NewHooksPayload(
		cfg,
		repo,
		gittest.DefaultObjectHash,
		nil,
		&gitcmd.UserDetails{
			UserID:   "1234",
			Username: "user",
			Protocol: "web",
		}, gitcmd.PreReceiveHook, nil, storage.ExtractTransactionID(ctx)).Env()
	require.NoError(t, err)

	standardEnv := []string{payload}
	pushOptions := []string{"mr.create", "mr.merge_when_pipeline_succeeds"}

	testCases := []struct {
		desc           string
		env            []string
		changes        string
		allowed        func(*testing.T, context.Context, gitlab.AllowedParams) (bool, string, error)
		prereceive     func(*testing.T, context.Context, string) (bool, error)
		expectHookCall bool
		expectedErr    error
	}{
		{
			desc:    "allowed change",
			env:     standardEnv,
			changes: "changes\n",
			allowed: func(t *testing.T, ctx context.Context, params gitlab.AllowedParams) (bool, string, error) {
				require.Equal(t, repoPath, params.RepoPath)
				require.Equal(t, repo.GetGlRepository(), params.GLRepository)
				require.Equal(t, "1234", params.GLID)
				require.Equal(t, "web", params.GLProtocol)
				require.Equal(t, "changes\n", params.Changes)
				require.Equal(t, repo.GetRelativePath(), params.RelativePath)
				require.Equal(t, pushOptions, params.PushOptions)
				return true, "", nil
			},
			prereceive: func(t *testing.T, ctx context.Context, glRepo string) (bool, error) {
				require.Equal(t, repo.GetGlRepository(), glRepo)
				return true, nil
			},
			expectHookCall: true,
		},
		{
			desc:    "disallowed change",
			env:     standardEnv,
			changes: "changes\n",
			allowed: func(t *testing.T, ctx context.Context, params gitlab.AllowedParams) (bool, string, error) {
				return false, "you shall not pass", nil
			},
			expectHookCall: false,
			expectedErr: NotAllowedError{
				Message:  "you shall not pass",
				Protocol: "web",
				UserID:   "1234",
				Changes:  []byte("changes\n"),
			},
		},
		{
			desc:    "allowed returns error",
			env:     standardEnv,
			changes: "changes\n",
			allowed: func(t *testing.T, ctx context.Context, params gitlab.AllowedParams) (bool, string, error) {
				return false, "", errors.New("oops")
			},
			expectHookCall: false,
			// This really is wrong, but we cannot fix it without adapting gitlab-shell
			// to return proper errors from its GitlabNetClient.
			expectedErr: NotAllowedError{
				Message:  "oops",
				Protocol: "web",
				UserID:   "1234",
				Changes:  []byte("changes\n"),
			},
		},
		{
			desc:    "prereceive rejects",
			env:     standardEnv,
			changes: "changes\n",
			allowed: func(t *testing.T, ctx context.Context, params gitlab.AllowedParams) (bool, string, error) {
				return true, "", nil
			},
			prereceive: func(t *testing.T, ctx context.Context, glRepo string) (bool, error) {
				return false, nil
			},
			expectHookCall: true,
			expectedErr:    errors.New(""),
		},
		{
			desc:    "prereceive errors",
			env:     standardEnv,
			changes: "changes\n",
			allowed: func(t *testing.T, ctx context.Context, params gitlab.AllowedParams) (bool, string, error) {
				return true, "", nil
			},
			prereceive: func(t *testing.T, ctx context.Context, glRepo string) (bool, error) {
				return false, errors.New("prereceive oops")
			},
			expectHookCall: true,
			expectedErr:    structerr.NewInternal("calling pre_receive endpoint: %w", errors.New("prereceive oops")),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			gitlabAPI := prereceiveAPIMock{
				allowed: func(ctx context.Context, params gitlab.AllowedParams) (bool, string, error) {
					return tc.allowed(t, ctx, params)
				},
				prereceive: func(ctx context.Context, glRepo string) (bool, error) {
					return tc.prereceive(t, ctx, glRepo)
				},
			}

			hookManager := NewManager(
				cfg,
				config.NewLocator(cfg),
				testhelper.SharedLogger(t),
				gittest.NewCommandFactory(t, cfg),
				transaction.NewManager(cfg, testhelper.SharedLogger(t), backchannel.NewRegistry()),
				&gitlabAPI,
				NewTransactionRegistry(storagemgr.NewTransactionRegistry()),
				NewProcReceiveRegistry(),
				nil,
			)

			gittest.WriteCustomHook(t, repoPath, "pre-receive", []byte("#!/bin/sh\necho called\n"))

			var stdout, stderr bytes.Buffer
			err = hookManager.PreReceiveHook(ctx, repo, pushOptions, tc.env, strings.NewReader(tc.changes), &stdout, &stderr)

			if tc.expectedErr != nil {
				require.Equal(t, tc.expectedErr, err)
			} else {
				require.NoError(t, err)
			}

			if tc.expectHookCall {
				require.Equal(t, "called\n", stdout.String())
			} else {
				require.Empty(t, stdout.String())
			}
			require.Empty(t, stderr.String())
		})
	}
}
