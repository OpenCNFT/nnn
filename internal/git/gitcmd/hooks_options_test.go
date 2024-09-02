package gitcmd_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v16/internal/command"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/gitcmd"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v16/internal/grpc/metadata"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper/testcfg"
	grpcmetadata "google.golang.org/grpc/metadata"
)

func TestWithRefHook(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg := testcfg.Build(t)

	repo, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
		SkipCreationViaService: true,
	})
	gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("refs/heads/master"))

	opt := gitcmd.WithRefTxHook(repo)
	subCmd := gitcmd.Command{Name: "update-ref", Args: []string{"refs/heads/master", gittest.DefaultObjectHash.ZeroOID.String()}}

	for _, tt := range []struct {
		name string
		fn   func() (*command.Command, error)
	}{
		{
			name: "NewCommand",
			fn: func() (*command.Command, error) {
				return gittest.NewCommandFactory(t, cfg, gitcmd.WithSkipHooks()).New(ctx, repo, subCmd, opt)
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			cmd, err := tt.fn()
			require.NoError(t, err)
			require.NoError(t, cmd.Wait())

			var actualEnvVars []string
			for _, env := range cmd.Env() {
				kv := strings.SplitN(env, "=", 2)
				require.Len(t, kv, 2)
				key, val := kv[0], kv[1]

				if strings.HasPrefix(key, "GL_") || strings.HasPrefix(key, "GITALY_") {
					require.NotEmptyf(t, strings.TrimSpace(val),
						"env var %s value should not be empty string", key)
					actualEnvVars = append(actualEnvVars, key)
				}
			}

			expectedEnv := []string{
				"GITALY_HOOKS_PAYLOAD",
				command.EnvLogConfiguration,
			}

			require.EqualValues(t, expectedEnv, actualEnvVars)
		})
	}
}

func TestWithPackObjectsHookEnv(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg := testcfg.Build(t)
	cfg.PackObjectsCache.Enabled = true

	repo, _ := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
		SkipCreationViaService: true,
	})

	userID := "user-123"
	username := "username"
	protocol := "protocol"
	remoteIP := "1.2.3.4"

	opt := gitcmd.WithPackObjectsHookEnv(repo, protocol)
	subCmd := gitcmd.Command{Name: "upload-pack", Args: []string{"a/b/c"}}

	ctx = grpcmetadata.AppendToOutgoingContext(ctx, "user_id", userID, "username", username, "remote_ip", remoteIP)
	ctx = metadata.OutgoingToIncoming(ctx)

	cmd, err := gittest.NewCommandFactory(t, cfg, gitcmd.WithSkipHooks()).New(ctx, repo, subCmd, opt)
	require.NoError(t, err)

	payload, err := gitcmd.HooksPayloadFromEnv(cmd.Env())
	require.NoError(t, err)

	require.Equal(t, userID, payload.UserDetails.UserID)
	require.Equal(t, username, payload.UserDetails.Username)
	require.Equal(t, protocol, payload.UserDetails.Protocol)
	require.Equal(t, remoteIP, payload.UserDetails.RemoteIP)
}
