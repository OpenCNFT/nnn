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
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/pktline"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper/testcfg"
)

func TestProcReceiveHandler(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg := testcfg.Build(t)

	repo, _ := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
		SkipCreationViaService: true,
	})

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
		1,
	).Env()
	require.NoError(t, err)

	type setupData struct {
		env              []string
		ctx              context.Context
		stdin            string
		expectedErr      error
		expectedCloseErr error
		expectedStdout   string
		expectedStderr   string
		expectedUpdates  []ReferenceUpdate
		expectedAtomic   bool
		handlerSteps     func(handler ProcReceiveHandler) error
	}

	for _, tc := range []struct {
		desc  string
		setup func(t *testing.T, ctx context.Context) setupData
	}{
		{
			desc: "no payload",
			setup: func(t *testing.T, ctx context.Context) setupData {
				return setupData{
					env:         []string{},
					ctx:         ctx,
					expectedErr: fmt.Errorf("extracting hooks payload: %w", errors.New("no hooks payload found in environment")),
				}
			},
		},
		{
			desc: "invalid version",
			setup: func(t *testing.T, ctx context.Context) setupData {
				var stdin bytes.Buffer
				_, err := pktline.WriteString(&stdin, "version=2")
				require.NoError(t, err)

				return setupData{
					env:         []string{payload},
					ctx:         ctx,
					stdin:       stdin.String(),
					expectedErr: errors.New("unsupported version: version=2"),
				}
			},
		},
		{
			desc: "no features",
			setup: func(t *testing.T, ctx context.Context) setupData {
				var stdin bytes.Buffer
				_, err := pktline.WriteString(&stdin, "version=1\n")
				require.NoError(t, err)
				err = pktline.WriteFlush(&stdin)
				require.NoError(t, err)
				err = pktline.WriteFlush(&stdin)
				require.NoError(t, err)

				var stdout bytes.Buffer
				_, err = pktline.WriteString(&stdout, "version=1")
				require.NoError(t, err)
				err = pktline.WriteFlush(&stdout)
				require.NoError(t, err)
				err = pktline.WriteFlush(&stdout)
				require.NoError(t, err)

				return setupData{
					env:             []string{payload},
					ctx:             ctx,
					stdin:           stdin.String(),
					expectedStdout:  stdout.String(),
					expectedUpdates: []ReferenceUpdate{},
					handlerSteps: func(handler ProcReceiveHandler) error {
						require.False(t, handler.Atomic())
						require.Empty(t, handler.PushOptions())
						return handler.Close(nil)
					},
				}
			},
		},
		{
			desc: "single reference with atomic",
			setup: func(t *testing.T, ctx context.Context) setupData {
				var stdin bytes.Buffer
				_, err := pktline.WriteString(&stdin, "version=1\000push-options atomic")
				require.NoError(t, err)
				err = pktline.WriteFlush(&stdin)
				require.NoError(t, err)
				_, err = pktline.WriteString(&stdin, fmt.Sprintf("%s %s %s",
					gittest.DefaultObjectHash.ZeroOID, gittest.DefaultObjectHash.EmptyTreeOID, "refs/heads/main"))
				require.NoError(t, err)
				err = pktline.WriteFlush(&stdin)
				require.NoError(t, err)

				var stdout bytes.Buffer
				_, err = pktline.WriteString(&stdout, "version=1\000push-options atomic")
				require.NoError(t, err)
				err = pktline.WriteFlush(&stdout)
				require.NoError(t, err)
				_, err = pktline.WriteString(&stdout, "ok refs/heads/main")
				require.NoError(t, err)
				err = pktline.WriteFlush(&stdout)
				require.NoError(t, err)

				return setupData{
					env:            []string{payload},
					ctx:            ctx,
					stdin:          stdin.String(),
					expectedStdout: stdout.String(),
					expectedAtomic: true,
					expectedUpdates: []ReferenceUpdate{
						{
							Ref:    "refs/heads/main",
							OldOID: gittest.DefaultObjectHash.ZeroOID,
							NewOID: gittest.DefaultObjectHash.EmptyTreeOID,
						},
					},
					handlerSteps: func(handler ProcReceiveHandler) error {
						require.NoError(t, handler.AcceptUpdate("refs/heads/main"))
						return handler.Close(nil)
					},
				}
			},
		},
		{
			desc: "single reference without atomic",
			setup: func(t *testing.T, ctx context.Context) setupData {
				var stdin bytes.Buffer
				_, err := pktline.WriteString(&stdin, "version=1\000push-options")
				require.NoError(t, err)
				err = pktline.WriteFlush(&stdin)
				require.NoError(t, err)
				_, err = pktline.WriteString(&stdin, fmt.Sprintf("%s %s %s",
					gittest.DefaultObjectHash.ZeroOID, gittest.DefaultObjectHash.EmptyTreeOID, "refs/heads/main"))
				require.NoError(t, err)
				err = pktline.WriteFlush(&stdin)
				require.NoError(t, err)

				var stdout bytes.Buffer
				_, err = pktline.WriteString(&stdout, "version=1\000push-options")
				require.NoError(t, err)
				err = pktline.WriteFlush(&stdout)
				require.NoError(t, err)
				_, err = pktline.WriteString(&stdout, "ok refs/heads/main")
				require.NoError(t, err)
				err = pktline.WriteFlush(&stdout)
				require.NoError(t, err)

				return setupData{
					env:            []string{payload},
					ctx:            ctx,
					stdin:          stdin.String(),
					expectedStdout: stdout.String(),
					expectedUpdates: []ReferenceUpdate{
						{
							Ref:    "refs/heads/main",
							OldOID: gittest.DefaultObjectHash.ZeroOID,
							NewOID: gittest.DefaultObjectHash.EmptyTreeOID,
						},
					},
					handlerSteps: func(handler ProcReceiveHandler) error {
						require.NoError(t, handler.AcceptUpdate("refs/heads/main"))
						return handler.Close(nil)
					},
				}
			},
		},
		{
			desc: "single reference but close midway with error",
			setup: func(t *testing.T, ctx context.Context) setupData {
				var stdin bytes.Buffer
				_, err := pktline.WriteString(&stdin, "version=1\000push-options")
				require.NoError(t, err)
				err = pktline.WriteFlush(&stdin)
				require.NoError(t, err)
				_, err = pktline.WriteString(&stdin, fmt.Sprintf("%s %s %s",
					gittest.DefaultObjectHash.ZeroOID, gittest.DefaultObjectHash.EmptyTreeOID, "refs/heads/main"))
				require.NoError(t, err)
				err = pktline.WriteFlush(&stdin)
				require.NoError(t, err)

				var stdout bytes.Buffer
				_, err = pktline.WriteString(&stdout, "version=1\000push-options")
				require.NoError(t, err)
				err = pktline.WriteFlush(&stdout)
				require.NoError(t, err)

				return setupData{
					env:              []string{payload},
					ctx:              ctx,
					stdin:            stdin.String(),
					expectedStdout:   stdout.String(),
					expectedCloseErr: errors.New("season ticket on a one way ride"),
					expectedUpdates: []ReferenceUpdate{
						{
							Ref:    "refs/heads/main",
							OldOID: gittest.DefaultObjectHash.ZeroOID,
							NewOID: gittest.DefaultObjectHash.EmptyTreeOID,
						},
					},
					handlerSteps: func(handler ProcReceiveHandler) error {
						return handler.Close(errors.New("season ticket on a one way ride"))
					},
				}
			},
		},
		{
			desc: "multiple references",
			setup: func(t *testing.T, ctx context.Context) setupData {
				var stdin bytes.Buffer
				_, err := pktline.WriteString(&stdin, "version=1\000push-options")
				require.NoError(t, err)
				err = pktline.WriteFlush(&stdin)
				require.NoError(t, err)
				_, err = pktline.WriteString(&stdin, fmt.Sprintf("%s %s %s",
					gittest.DefaultObjectHash.ZeroOID, gittest.DefaultObjectHash.EmptyTreeOID, "refs/heads/main"))
				require.NoError(t, err)
				_, err = pktline.WriteString(&stdin, fmt.Sprintf("%s %s %s",
					gittest.DefaultObjectHash.ZeroOID, gittest.DefaultObjectHash.EmptyTreeOID, "refs/heads/branch"))
				require.NoError(t, err)
				err = pktline.WriteFlush(&stdin)
				require.NoError(t, err)

				var stdout bytes.Buffer
				_, err = pktline.WriteString(&stdout, "version=1\000push-options")
				require.NoError(t, err)
				err = pktline.WriteFlush(&stdout)
				require.NoError(t, err)
				_, err = pktline.WriteString(&stdout, "ok refs/heads/main")
				require.NoError(t, err)
				_, err = pktline.WriteString(&stdout, "ng refs/heads/branch for fun")
				require.NoError(t, err)
				err = pktline.WriteFlush(&stdout)
				require.NoError(t, err)

				return setupData{
					env:            []string{payload},
					ctx:            ctx,
					stdin:          stdin.String(),
					expectedStdout: stdout.String(),
					expectedUpdates: []ReferenceUpdate{
						{
							Ref:    "refs/heads/main",
							OldOID: gittest.DefaultObjectHash.ZeroOID,
							NewOID: gittest.DefaultObjectHash.EmptyTreeOID,
						},
						{
							Ref:    "refs/heads/branch",
							OldOID: gittest.DefaultObjectHash.ZeroOID,
							NewOID: gittest.DefaultObjectHash.EmptyTreeOID,
						},
					},
					handlerSteps: func(handler ProcReceiveHandler) error {
						require.NoError(t, handler.AcceptUpdate("refs/heads/main"))
						require.NoError(t, handler.RejectUpdate("refs/heads/branch", "for fun"))
						return handler.Close(nil)
					},
				}
			},
		},
		{
			desc: "push options",
			setup: func(t *testing.T, ctx context.Context) setupData {
				var stdin bytes.Buffer
				_, err := pktline.WriteString(&stdin, "version=1\000push-options")
				require.NoError(t, err)
				err = pktline.WriteFlush(&stdin)
				require.NoError(t, err)
				err = pktline.WriteFlush(&stdin)
				require.NoError(t, err)
				_, err = pktline.WriteString(&stdin, "push-option-1")
				require.NoError(t, err)
				_, err = pktline.WriteString(&stdin, "push-option-2")
				require.NoError(t, err)
				err = pktline.WriteFlush(&stdin)
				require.NoError(t, err)

				var stdout bytes.Buffer
				_, err = pktline.WriteString(&stdout, "version=1\000push-options")
				require.NoError(t, err)
				err = pktline.WriteFlush(&stdout)
				require.NoError(t, err)
				err = pktline.WriteFlush(&stdout)
				require.NoError(t, err)

				return setupData{
					env:             []string{payload},
					ctx:             ctx,
					stdin:           stdin.String(),
					expectedStdout:  stdout.String(),
					expectedUpdates: []ReferenceUpdate{},
					handlerSteps: func(handler ProcReceiveHandler) error {
						require.Equal(t, []string{"push-option-1", "push-option-2"}, handler.PushOptions())
						return handler.Close(nil)
					},
				}
			},
		},
		{
			desc: "handler writes to stderr",
			setup: func(t *testing.T, ctx context.Context) setupData {
				var stdin bytes.Buffer
				_, err := pktline.WriteString(&stdin, "version=1\000push-options")
				require.NoError(t, err)
				err = pktline.WriteFlush(&stdin)
				require.NoError(t, err)
				_, err = pktline.WriteString(&stdin, fmt.Sprintf("%s %s %s",
					gittest.DefaultObjectHash.ZeroOID, gittest.DefaultObjectHash.EmptyTreeOID, "refs/heads/main"))
				require.NoError(t, err)
				err = pktline.WriteFlush(&stdin)
				require.NoError(t, err)

				var stdout bytes.Buffer
				_, err = pktline.WriteString(&stdout, "version=1\000push-options")
				require.NoError(t, err)
				err = pktline.WriteFlush(&stdout)
				require.NoError(t, err)
				_, err = pktline.WriteString(&stdout, "ok refs/heads/main")
				require.NoError(t, err)
				err = pktline.WriteFlush(&stdout)
				require.NoError(t, err)

				return setupData{
					env:            []string{payload},
					ctx:            ctx,
					stdin:          stdin.String(),
					expectedStdout: stdout.String(),
					expectedStderr: "foo",
					expectedUpdates: []ReferenceUpdate{
						{
							Ref:    "refs/heads/main",
							OldOID: gittest.DefaultObjectHash.ZeroOID,
							NewOID: gittest.DefaultObjectHash.EmptyTreeOID,
						},
					},
					handlerSteps: func(handler ProcReceiveHandler) error {
						require.NoError(t, handler.AcceptUpdate("refs/heads/main"))
						_, err := handler.Write([]byte("foo"))
						require.NoError(t, err)
						return handler.Close(nil)
					},
				}
			},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			setup := tc.setup(t, ctx)

			var stdout, stderr bytes.Buffer
			handler, doneCh, err := NewProcReceiveHandler(setup.env, strings.NewReader(setup.stdin), &stdout, &stderr)
			if err != nil || setup.expectedErr != nil {
				require.Equal(t, setup.expectedErr, err)
				return
			}

			select {
			case <-doneCh:
				t.Fatal("done returned before handler called Close()")
			default:
			}

			require.NoError(t, setup.handlerSteps(handler))
			// When Close() is called, we must receive a confirmation.
			err = <-doneCh
			require.Equal(t, setup.expectedCloseErr, err)

			require.Equal(t, setup.expectedStdout, stdout.String())
			require.Equal(t, setup.expectedStderr, stderr.String())
		})
	}
}
