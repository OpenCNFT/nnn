package gitaly

import (
	"context"
	"errors"
	"fmt"
	"os"

	"github.com/urfave/cli/v2"
	"gitlab.com/gitlab-org/gitaly/v16"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/gitcmd"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/config/prometheus"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/hook"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/storagemgr"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitlab"
	"gitlab.com/gitlab-org/gitaly/v16/internal/log"
)

func newCheckCommand() *cli.Command {
	return &cli.Command{
		Name:  "check",
		Usage: "verify internal API is accessible",
		UsageText: `gitaly check <gitaly_config_file>

Example: gitaly check gitaly.config.toml`,
		Description:     "Check that the internal Gitaly API is accessible.",
		ArgsUsage:       "<configfile>",
		Action:          checkAction,
		HideHelpCommand: true,
	}
}

func checkAction(ctx *cli.Context) (returnedErr error) {
	logger := log.ConfigureCommand()

	if ctx.NArg() != 1 || ctx.Args().First() == "" {
		if err := cli.ShowSubcommandHelp(ctx); err != nil {
			return err
		}

		return cli.Exit("error: invalid argument(s)", 2)
	}

	configPath := ctx.Args().First()
	cfg, err := loadConfig(configPath)
	if err != nil {
		return fmt.Errorf("loading configuration %q: %w", configPath, err)
	}

	if cfg.RuntimeDir != "" {
		if err := config.PruneOldGitalyProcessDirectories(logger, cfg.RuntimeDir); err != nil {
			return fmt.Errorf("prune runtime directories: %w", err)
		}
	}

	runtimeDir, err := config.SetupRuntimeDirectory(cfg, os.Getpid())
	if err != nil {
		return fmt.Errorf("setup runtime directory: %w", err)
	}
	cfg.RuntimeDir = runtimeDir

	defer func() {
		if rmErr := os.RemoveAll(cfg.RuntimeDir); rmErr != nil {
			returnedErr = errors.Join(err, rmErr)
		}
	}()

	// Since this subcommand invokes a Git command, we need to unpack the bundled Git binaries
	// from the Gitaly binary.
	if err := gitaly.UnpackAuxiliaryBinaries(cfg.RuntimeDir, func(string) bool {
		return true
	}); err != nil {
		return fmt.Errorf("unpack auxiliary binaries: %w", err)
	}

	fmt.Fprint(ctx.App.Writer, "Checking GitLab API access: ")
	info, err := checkAPI(cfg, logger)
	if err != nil {
		fmt.Fprintln(ctx.App.Writer, "FAILED")
		return err
	}

	fmt.Fprintln(ctx.App.Writer, "OK")
	fmt.Fprintf(ctx.App.Writer, "GitLab version: %s\n", info.Version)
	fmt.Fprintf(ctx.App.Writer, "GitLab revision: %s\n", info.Revision)
	fmt.Fprintf(ctx.App.Writer, "GitLab Api version: %s\n", info.APIVersion)
	fmt.Fprintf(ctx.App.Writer, "Redis reachable for GitLab: %t\n", info.RedisReachable)
	fmt.Fprintln(ctx.App.Writer, "OK")

	return returnedErr
}

func checkAPI(cfg config.Cfg, logger log.Logger) (*gitlab.CheckInfo, error) {
	gitlabAPI, err := gitlab.NewHTTPClient(logger, cfg.Gitlab, cfg.TLS, prometheus.Config{})
	if err != nil {
		return nil, err
	}

	gitCmdFactory, cleanup, err := gitcmd.NewExecCommandFactory(cfg, logger)
	if err != nil {
		return nil, err
	}
	defer cleanup()

	return hook.NewManager(
		cfg,
		config.NewLocator(cfg),
		logger,
		gitCmdFactory,
		nil,
		gitlabAPI,
		hook.NewTransactionRegistry(storagemgr.NewTransactionRegistry()),
		hook.NewProcReceiveRegistry(),
		nil,
	).Check(context.Background())
}
