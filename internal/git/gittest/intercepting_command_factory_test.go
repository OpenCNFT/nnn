package gittest

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/gitcmd"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper"
)

func TestInterceptingCommandFactory(t *testing.T) {
	cfg, repoProto, repoPath := setup(t)
	ctx := testhelper.Context(t)

	factory := NewInterceptingCommandFactory(t, ctx, cfg, func(execEnv gitcmd.ExecutionEnvironment) string {
		return fmt.Sprintf(
			`#!/usr/bin/env bash
			%q rev-parse --sq-quote 'Hello, world!'
		`, execEnv.BinaryPath)
	})
	expectedString := " 'Hello, world'\\!''\n"

	t.Run("New", func(t *testing.T) {
		var stdout bytes.Buffer
		cmd, err := factory.New(ctx, repoProto, gitcmd.Command{
			Name: "rev-parse",
			Args: []string{"something"},
		}, gitcmd.WithStdout(&stdout))
		require.NoError(t, err)
		require.NoError(t, cmd.Wait())
		require.Equal(t, expectedString, stdout.String())
	})

	t.Run("NewWithoutRepo", func(t *testing.T) {
		var stdout bytes.Buffer
		cmd, err := factory.NewWithoutRepo(ctx, gitcmd.Command{
			Name: "rev-parse",
			Args: []string{"something"},
			Flags: []gitcmd.Option{
				gitcmd.ValueFlag{Name: "-C", Value: repoPath},
			},
		}, gitcmd.WithStdout(&stdout))
		require.NoError(t, err)
		require.NoError(t, cmd.Wait())
		require.Equal(t, expectedString, stdout.String())
	})
}

func TestInterceptingCommandFactory_GitVersion(t *testing.T) {
	cfg, _, _ := setup(t)
	ctx := testhelper.Context(t)

	generateVersionScript := func(execEnv gitcmd.ExecutionEnvironment) string {
		return `#!/usr/bin/env bash
			echo "git version 1.2.3"
		`
	}

	// Obtain the real Git version so that we can compare that it matches what we expect.
	realFactory := NewCommandFactory(t, cfg)

	realVersion, err := realFactory.GitVersion(ctx)
	require.NoError(t, err)

	// Furthermore, we need to obtain the intercepted version here because we cannot construct
	// `git.Version` structs ourselves.
	fakeVersion, err := NewInterceptingCommandFactory(t, ctx, cfg, generateVersionScript, WithInterceptedVersion()).GitVersion(ctx)
	require.NoError(t, err)
	require.Equal(t, "1.2.3", fakeVersion.String())

	for _, tc := range []struct {
		desc            string
		opts            []InterceptingCommandFactoryOption
		expectedVersion git.Version
	}{
		{
			desc:            "without version interception",
			expectedVersion: realVersion,
		},
		{
			desc: "with version interception",
			opts: []InterceptingCommandFactoryOption{
				WithInterceptedVersion(),
			},
			expectedVersion: fakeVersion,
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			factory := NewInterceptingCommandFactory(t, ctx, cfg, generateVersionScript, tc.opts...)

			version, err := factory.GitVersion(ctx)
			require.NoError(t, err)
			require.Equal(t, tc.expectedVersion, version)

			// The real command factory should always return the real Git version.
			version, err = factory.realCommandFactory.GitVersion(ctx)
			require.NoError(t, err)
			require.Equal(t, realVersion, version)

			// On the other hand, the intercepting command factory should return
			// different versions depending on whether the version is intercepted or
			// not. This is required such that it correctly handles version checks in
			// case it calls `GitVersion()` on itself.
			version, err = factory.interceptingCommandFactory.GitVersion(ctx)
			require.NoError(t, err)
			require.Equal(t, tc.expectedVersion, version)
		})
	}
}
