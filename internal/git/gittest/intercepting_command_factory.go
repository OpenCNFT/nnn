package gittest

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"

	"gitlab.com/gitlab-org/gitaly/v16/internal/command"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/gitcmd"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper"
)

var _ gitcmd.CommandFactory = &InterceptingCommandFactory{}

// InterceptingCommandFactory is a gitcmd.CommandFactory which intercepts all executions of Git
// commands with a custom script.
type InterceptingCommandFactory struct {
	realCommandFactory         gitcmd.CommandFactory
	interceptingCommandFactory gitcmd.CommandFactory
	interceptVersion           bool
}

type interceptingCommandFactoryConfig struct {
	opts             []gitcmd.ExecCommandFactoryOption
	interceptVersion bool
}

// InterceptingCommandFactoryOption is an option that can be passed to
// NewInterceptingCommandFactory.
type InterceptingCommandFactoryOption func(*interceptingCommandFactoryConfig)

// WithRealCommandFactoryOptions is an option that allows the caller to pass options to the real
// git.ExecCommandFactory.
func WithRealCommandFactoryOptions(opts ...gitcmd.ExecCommandFactoryOption) InterceptingCommandFactoryOption {
	return func(cfg *interceptingCommandFactoryConfig) {
		cfg.opts = opts
	}
}

// WithInterceptedVersion will cause `GitVersion()` to use the intercepting Git command factory
// instead of the real Git command factory. This allows the caller to explicitly test version
// detection.
func WithInterceptedVersion() InterceptingCommandFactoryOption {
	return func(cfg *interceptingCommandFactoryConfig) {
		cfg.interceptVersion = true
	}
}

// NewInterceptingCommandFactory creates a new command factory which intercepts Git commands. The
// given configuration must point to the real Git executable. The function will be executed to
// generate the script and receives as input the Git execution environment pointing to the real Git
// binary.
func NewInterceptingCommandFactory(
	tb testing.TB,
	ctx context.Context,
	cfg config.Cfg,
	generateScript func(gitcmd.ExecutionEnvironment) string,
	opts ...InterceptingCommandFactoryOption,
) *InterceptingCommandFactory {
	var interceptingCommandFactoryCfg interceptingCommandFactoryConfig
	for _, opt := range opts {
		opt(&interceptingCommandFactoryCfg)
	}

	// We create two different command factories. The first one will set up the execution
	// environment such that we can deterministically resolve the Git binary path as well as
	// required environment variables for both bundled and non-bundled Git. The second one will
	// then use a separate config which overrides the Git binary path to point to a custom
	// script supplied by the user.
	gitCmdFactory := NewCommandFactory(tb, cfg, interceptingCommandFactoryCfg.opts...)
	execEnv := gitCmdFactory.GetExecutionEnvironment(ctx)

	scriptPath := filepath.Join(testhelper.TempDir(tb), "git")
	testhelper.WriteExecutable(tb, scriptPath, []byte(generateScript(execEnv)))

	// In case the user requested us to not intercept the Git version we need to write another
	// wrapper script. This wrapper script detects whether git-version(1) is executed and, if
	// so, instead executes the real Git binary. Otherwise, it executes the Git script as
	// provided by the user.
	//
	// This is required in addition to the interception of `GitVersion()` itself so that calls
	// to `interceptingCommandFactory.GitVersion()` also return the correct results whenever the
	// factory calls its own `GitVersion()` function.
	if !interceptingCommandFactoryCfg.interceptVersion {
		wrapperScriptPath := filepath.Join(testhelper.TempDir(tb), "git")
		testhelper.WriteExecutable(tb, wrapperScriptPath, []byte(fmt.Sprintf(
			`#!/usr/bin/env bash
			if test "$1" = "version"
			then
				exec %q "$@"
			fi
			exec %q "$@"
		`, execEnv.BinaryPath, scriptPath)))

		scriptPath = wrapperScriptPath
	}

	return &InterceptingCommandFactory{
		realCommandFactory:         gitCmdFactory,
		interceptingCommandFactory: NewCommandFactory(tb, cfg, gitcmd.WithGitBinaryPath(scriptPath)),
		interceptVersion:           interceptingCommandFactoryCfg.interceptVersion,
	}
}

// New creates a new Git command for the given repository using the intercepting script.
func (f *InterceptingCommandFactory) New(ctx context.Context, repo storage.Repository, sc gitcmd.Command, opts ...gitcmd.CmdOpt) (*command.Command, error) {
	return f.interceptingCommandFactory.New(ctx, repo, sc, append(
		opts, gitcmd.WithEnv(f.realCommandFactory.GetExecutionEnvironment(ctx).EnvironmentVariables...),
	)...)
}

// NewWithoutRepo creates a new Git command using the intercepting script.
func (f *InterceptingCommandFactory) NewWithoutRepo(ctx context.Context, sc gitcmd.Command, opts ...gitcmd.CmdOpt) (*command.Command, error) {
	return f.interceptingCommandFactory.NewWithoutRepo(ctx, sc, append(
		opts, gitcmd.WithEnv(f.realCommandFactory.GetExecutionEnvironment(ctx).EnvironmentVariables...),
	)...)
}

// GetExecutionEnvironment returns the execution environment of the intercetping command factory.
// The Git binary path will point to the intercepting script, while environment variables will
// point to the intercepted Git installation.
func (f *InterceptingCommandFactory) GetExecutionEnvironment(ctx context.Context) gitcmd.ExecutionEnvironment {
	execEnv := f.realCommandFactory.GetExecutionEnvironment(ctx)
	execEnv.BinaryPath = f.interceptingCommandFactory.GetExecutionEnvironment(ctx).BinaryPath
	return execEnv
}

// HooksPath returns the path where hooks are stored. This returns the actual hooks path of the real
// Git command factory.
func (f *InterceptingCommandFactory) HooksPath(ctx context.Context) string {
	return f.realCommandFactory.HooksPath(ctx)
}

// GitVersion returns the Git version as returned by the intercepted command.
func (f *InterceptingCommandFactory) GitVersion(ctx context.Context) (git.Version, error) {
	if f.interceptVersion {
		return f.interceptingCommandFactory.GitVersion(ctx)
	}
	return f.realCommandFactory.GitVersion(ctx)
}
