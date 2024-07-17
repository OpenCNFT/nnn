package gitaly

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"

	"github.com/urfave/cli/v2"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git"
	"gitlab.com/gitlab-org/gitaly/v16/internal/log"
)

func newGitCommand() *cli.Command {
	return &cli.Command{
		Name:  "git",
		Usage: "execute Git commands using Gitaly's embedded Git",
		UsageText: `gitaly git [git-command] [args...]

Example: gitaly git status`,
		Description:     "Execute Git commands using the same Git execution environment as Gitaly.",
		Action:          gitAction,
		HideHelpCommand: true,
		ArgsUsage:       "[git-command] [args...]",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:     flagConfig,
				Usage:    "path to Gitaly configuration",
				Aliases:  []string{"c"},
				Required: true,
			},
		},
	}
}

func gitAction(ctx *cli.Context) error {
	logger := log.ConfigureCommand()

	if ctx.NArg() < 1 {
		if err := cli.ShowSubcommandHelp(ctx); err != nil {
			return err
		}
		return cli.Exit("error: Git command required", 1)
	}

	cfg, err := loadConfig(ctx.String(flagConfig))
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}

	gitCmdFactory, cleanup, err := git.NewExecCommandFactory(cfg, logger)
	if err != nil {
		return fmt.Errorf("creating Git command factory: %w", err)
	}
	defer cleanup()

	gitBinaryPath := gitCmdFactory.GetExecutionEnvironment(ctx.Context).BinaryPath

	cmd := exec.Command(gitBinaryPath, ctx.Args().Slice()...)
	cmd.Stdin = os.Stdin
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmd.Env = append(os.Environ(),
		fmt.Sprintf("GIT_EXEC_PATH=%s", filepath.Dir(gitBinaryPath)),
		fmt.Sprintf("PATH=%s:%s", filepath.Dir(gitBinaryPath), os.Getenv("PATH")),
	)

	err = cmd.Run()
	if err != nil {
		if exitError, ok := err.(*exec.ExitError); ok {
			return cli.Exit("", exitError.ExitCode())
		}
		return fmt.Errorf("executing git command: %w", err)
	}

	return nil
}
