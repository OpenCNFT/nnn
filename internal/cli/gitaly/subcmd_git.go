package gitaly

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/urfave/cli/v2"
	"gitlab.com/gitlab-org/gitaly/v16"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git"
	"gitlab.com/gitlab-org/gitaly/v16/internal/log"
)

func gitalyConfigFlag() *cli.StringFlag {
	return &cli.StringFlag{
		Name:     flagConfig,
		Usage:    "path to Gitaly configuration",
		Aliases:  []string{"c"},
		Required: true,
	}
}

func newGitCommand() *cli.Command {
	return &cli.Command{
		Name:  "git",
		Usage: "execute Git commands using Gitaly's embedded Git",
		UsageText: `gitaly git -c <gitaly-config-path> -- [git-command] [args...]

Example: gitaly git -c <gitaly-config-path> -- status`,
		Description: `=== WARNING ===
Do not execute commands in Gitaly's storages
without understanding the implications of doing so.
Modifying Gitaly's state may lead to violating Gitaly's
invariants, and lead to unavailability or data loss.
===============`,
		Action:          gitAction,
		HideHelpCommand: true,
		Flags: []cli.Flag{
			gitalyConfigFlag(),
		},
	}
}

func gitAction(ctx *cli.Context) (returnErr error) {
	logger := log.ConfigureCommand()

	cfg, err := loadConfig(ctx.String(flagConfig))
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}

	runtimeDir, err := os.MkdirTemp("", "gitaly-git-*")
	if err != nil {
		return fmt.Errorf("creating runtime dir: %w", err)
	}

	defer func() {
		if err := os.RemoveAll(runtimeDir); err != nil {
			returnErr = errors.Join(returnErr, fmt.Errorf("removing runtime dir: %w", err))
		}
	}()

	cfg.RuntimeDir = runtimeDir

	if err := gitaly.UnpackAuxiliaryBinaries(cfg.RuntimeDir, func(binaryName string) bool {
		return strings.HasPrefix(binaryName, "gitaly-git")
	}); err != nil {
		return fmt.Errorf("unpack auxiliary binaries: %w", err)
	}

	gitCmdFactory, cleanup, err := git.NewExecCommandFactory(cfg, logger)
	if err != nil {
		return fmt.Errorf("creating Git command factory: %w", err)
	}
	defer cleanup()

	gitBinaryPath := gitCmdFactory.GetExecutionEnvironment(ctx.Context).BinaryPath

	cmd := exec.Command(gitBinaryPath, ctx.Args().Slice()...)
	cmd.Stdin = ctx.App.Reader
	cmd.Stdout = ctx.App.Writer
	cmd.Stderr = ctx.App.ErrWriter

	// Disable automatic garbage collection and maintenance
	gitConfig := []git.ConfigPair{
		{Key: "gc.auto", Value: "0"},
		{Key: "maintenance.auto", Value: "0"},
	}

	cmd.Env = os.Environ()

	cmd.Env = append(cmd.Env,
		fmt.Sprintf("GIT_EXEC_PATH=%s", filepath.Dir(gitBinaryPath)))
	cmd.Env = append(cmd.Env, git.ConfigPairsToGitEnvironment(gitConfig)...)

	err = cmd.Run()
	if err != nil {
		var exitError *exec.ExitError
		if errors.As(err, &exitError) {
			return cli.Exit("", exitError.ExitCode())
		}
		return fmt.Errorf("executing git command: %w", err)
	}

	return nil
}
