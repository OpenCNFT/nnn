package hook

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v16/internal/featureflag"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/gitcmd"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/storagemgr"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/transaction"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitlab"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v16/internal/transaction/txinfo"
	"gitlab.com/gitlab-org/gitaly/v16/internal/transaction/voting"
)

func TestHookManager_stopCalled(t *testing.T) {
	ctx := testhelper.Context(t)
	cfg := testcfg.Build(t)

	repo, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
		SkipCreationViaService: true,
	})

	expectedTx := txinfo.Transaction{
		ID: 1234, Node: "primary", Primary: true,
	}

	var mockTxMgr transaction.MockManager
	hookManager := NewManager(cfg, config.NewLocator(cfg), testhelper.SharedLogger(t), gittest.NewCommandFactory(t, cfg), &mockTxMgr, gitlab.NewMockClient(
		t, gitlab.MockAllowed, gitlab.MockPreReceive, gitlab.MockPostReceive,
	), NewTransactionRegistry(storagemgr.NewTransactionRegistry()), NewProcReceiveRegistry(), nil)

	hooksPayload, err := gitcmd.NewHooksPayload(
		cfg,
		repo,
		gittest.DefaultObjectHash,
		&expectedTx,
		&gitcmd.UserDetails{
			UserID:   "1234",
			Username: "user",
			Protocol: "web",
		},
		gitcmd.ReferenceTransactionHook,
		featureflag.FromContext(ctx),
		storage.ExtractTransactionID(ctx),
	).Env()
	require.NoError(t, err)

	hookPaths := make([]string, 3)
	for i, hook := range []string{"pre-receive", "update", "post-receive"} {
		hookPaths[i] = gittest.WriteCustomHook(t, repoPath, hook, []byte("#!/bin/sh\nexit 1\n"))
	}

	preReceiveFunc := func(t *testing.T) error {
		return hookManager.PreReceiveHook(ctx, repo, nil, []string{hooksPayload}, strings.NewReader("changes"), io.Discard, io.Discard)
	}
	updateFunc := func(t *testing.T) error {
		return hookManager.UpdateHook(ctx, repo, "ref", gittest.DefaultObjectHash.ZeroOID.String(), gittest.DefaultObjectHash.ZeroOID.String(), []string{hooksPayload}, io.Discard, io.Discard)
	}
	postReceiveFunc := func(t *testing.T) error {
		return hookManager.PostReceiveHook(ctx, repo, nil, []string{hooksPayload}, strings.NewReader("changes"), io.Discard, io.Discard)
	}

	for _, tc := range []struct {
		desc     string
		hookFunc func(*testing.T) error
		hookPath string
		stopErr  error
	}{
		{
			desc:     "pre-receive gets successfully stopped",
			hookFunc: preReceiveFunc,
			hookPath: hookPaths[0],
		},
		{
			desc:     "pre-receive with stop error does not clobber real error",
			hookFunc: preReceiveFunc,
			stopErr:  errors.New("stop error"),
			hookPath: hookPaths[0],
		},
		{
			desc:     "post-receive gets successfully stopped",
			hookFunc: postReceiveFunc,
			hookPath: hookPaths[2],
		},
		{
			desc:     "post-receive with stop error does not clobber real error",
			hookFunc: postReceiveFunc,
			stopErr:  errors.New("stop error"),
			hookPath: hookPaths[2],
		},
		{
			desc:     "update gets successfully stopped",
			hookFunc: updateFunc,
			hookPath: hookPaths[1],
		},
		{
			desc:     "update with stop error does not clobber real error",
			hookFunc: updateFunc,
			stopErr:  errors.New("stop error"),
			hookPath: hookPaths[1],
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			wasInvoked := false
			mockTxMgr.StopFn = func(ctx context.Context, tx txinfo.Transaction) error {
				require.Equal(t, expectedTx, tx)
				wasInvoked = true
				return tc.stopErr
			}

			err := tc.hookFunc(t)
			require.Equal(t, fmt.Sprintf("executing custom hooks: error executing \"%s\": exit status 1", tc.hookPath), err.Error())
			require.True(t, wasInvoked, "expected stop to have been invoked")
		})
	}
}

func TestHookManager_contextCancellationCancelsVote(t *testing.T) {
	ctx, cancel := context.WithCancel(testhelper.Context(t))
	cfg := testcfg.Build(t)

	repo, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
		SkipCreationViaService: true,
	})
	commitID := gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("master"))

	mockTxMgr := transaction.MockManager{
		VoteFn: func(ctx context.Context, _ txinfo.Transaction, _ voting.Vote, _ voting.Phase) error {
			<-ctx.Done()
			return fmt.Errorf("mock error: %w", ctx.Err())
		},
	}

	hookManager := NewManager(cfg, config.NewLocator(cfg), testhelper.SharedLogger(t), gittest.NewCommandFactory(t, cfg), &mockTxMgr, gitlab.NewMockClient(
		t, gitlab.MockAllowed, gitlab.MockPreReceive, gitlab.MockPostReceive,
	), NewTransactionRegistry(storagemgr.NewTransactionRegistry()), NewProcReceiveRegistry(), nil)

	hooksPayload, err := gitcmd.NewHooksPayload(
		cfg,
		repo,
		gittest.DefaultObjectHash,
		&txinfo.Transaction{
			ID: 1234, Node: "primary", Primary: true,
		},
		nil,
		gitcmd.ReferenceTransactionHook,
		nil,
		storage.ExtractTransactionID(ctx),
	).Env()
	require.NoError(t, err)

	changes := fmt.Sprintf("%s %s refs/heads/master", commitID, gittest.DefaultObjectHash.ZeroOID)

	cancel()

	err = hookManager.ReferenceTransactionHook(ctx, ReferenceTransactionPrepared, []string{hooksPayload}, strings.NewReader(changes))
	require.Equal(t, "error voting on transaction: mock error: context canceled", err.Error())
}

func TestIsForceDeletionsOnly(t *testing.T) {
	anyOID := strings.Repeat("1", gittest.DefaultObjectHash.EncodedLen())
	zeroOID := gittest.DefaultObjectHash.ZeroOID.String()

	forceDeletion := fmt.Sprintf("%s %s refs/heads/force-delete", zeroOID, zeroOID)
	forceUpdate := fmt.Sprintf("%s %s refs/heads/force-update", zeroOID, anyOID)
	deletion := fmt.Sprintf("%s %s refs/heads/delete", anyOID, zeroOID)

	for _, tc := range []struct {
		desc     string
		changes  string
		expected bool
	}{
		{
			desc:     "single force deletion",
			changes:  forceDeletion + "\n",
			expected: true,
		},
		{
			desc:     "single force deletion with missing newline",
			changes:  forceDeletion,
			expected: true,
		},
		{
			desc:     "multiple force deletions",
			changes:  strings.Join([]string{forceDeletion, forceDeletion}, "\n"),
			expected: true,
		},
		{
			desc:     "single non-force deletion",
			changes:  deletion + "\n",
			expected: false,
		},
		{
			desc:     "single force update",
			changes:  forceUpdate + "\n",
			expected: false,
		},
		{
			desc:     "mixed deletions and updates",
			changes:  strings.Join([]string{forceDeletion, forceUpdate}, "\n"),
			expected: false,
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			actual := isForceDeletionsOnly(gittest.DefaultObjectHash, strings.NewReader(tc.changes))
			require.Equal(t, tc.expected, actual)
		})
	}
}
