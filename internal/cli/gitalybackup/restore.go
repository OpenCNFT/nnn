package gitalybackup

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"runtime"

	cli "github.com/urfave/cli/v2"
	"gitlab.com/gitlab-org/gitaly/v16/internal/backup"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v16/internal/grpc/client"
	"gitlab.com/gitlab-org/gitaly/v16/internal/log"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
)

type restoreRequest struct {
	serverRepository
	AlwaysCreate bool `json:"always_create"`
}

type restoreSubcommand struct {
	backupPath            string
	parallel              int
	parallelStorage       int
	layout                string
	removeAllRepositories []string
	backupID              string
	serverSide            bool
}

func (cmd *restoreSubcommand) flags(ctx *cli.Context) {
	cmd.backupPath = ctx.String("path")
	cmd.parallel = ctx.Int("parallel")
	cmd.parallelStorage = ctx.Int("parallel-storage")
	cmd.layout = ctx.String("layout")
	cmd.removeAllRepositories = ctx.StringSlice("remove-all-repositories")
	cmd.backupID = ctx.String("id")
	cmd.serverSide = ctx.Bool("server-side")
}

func restoreFlags() []cli.Flag {
	return []cli.Flag{
		&cli.StringFlag{
			Name:  "path",
			Usage: "repository backup path",
		},
		&cli.IntFlag{
			Name:  "parallel",
			Usage: "maximum number of parallel backups",
			Value: runtime.NumCPU(),
		},
		&cli.IntFlag{
			Name:  "parallel-storage",
			Usage: "maximum number of parallel backups per storage. Note: actual parallelism when combined with `-parallel` depends on the order the repositories are received.",
			Value: 2,
		},
		&cli.StringFlag{
			Name:  "layout",
			Usage: "how backup files are located. Either pointer or legacy.",
			Value: "pointer",
		},
		&cli.StringSliceFlag{
			Name:  "remove-all-repositories",
			Usage: "comma-separated list of storage names to have all repositories removed from before restoring.",
		},
		&cli.StringFlag{
			Name:  "id",
			Usage: "ID of full backup to restore. If not specified, the latest backup is restored.",
		},
		&cli.BoolFlag{
			Name:  "server-side",
			Usage: "use server-side backups. Note: The feature is not ready for production use.",
			Value: false,
		},
	}
}

func newRestoreCommand() *cli.Command {
	return &cli.Command{
		Name:   "restore",
		Usage:  "Restore backup file",
		Action: restoreAction,
		Flags:  restoreFlags(),
	}
}

func restoreAction(cctx *cli.Context) error {
	logger, err := log.Configure(cctx.App.Writer, "json", "")
	if err != nil {
		fmt.Printf("configuring logger failed: %v", err)
		return err
	}

	ctx, err := storage.InjectGitalyServersEnv(cctx.Context)
	if err != nil {
		logger.Error(err.Error())
		return err
	}

	subcmd := restoreSubcommand{}
	subcmd.flags(cctx)

	if err := subcmd.run(ctx, logger, cctx.App.Reader); err != nil {
		logger.Error(err.Error())
		return err
	}
	return nil
}

func (cmd *restoreSubcommand) run(ctx context.Context, logger log.Logger, stdin io.Reader) error {
	pool := client.NewPool(client.WithDialOptions(client.UnaryInterceptor(), client.StreamInterceptor()))
	defer func() {
		_ = pool.Close()
	}()

	var manager backup.Strategy
	if cmd.serverSide {
		if cmd.backupPath != "" {
			return fmt.Errorf("restore: path cannot be used with server-side backups")
		}

		manager = backup.NewServerSideAdapter(pool)
	} else {
		sink, err := backup.ResolveSink(ctx, cmd.backupPath)
		if err != nil {
			return fmt.Errorf("restore: resolve sink: %w", err)
		}
		locator, err := backup.ResolveLocator(cmd.layout, sink)
		if err != nil {
			return fmt.Errorf("restore: resolve locator: %w", err)
		}
		manager = backup.NewManager(sink, locator, pool)
	}

	for _, storageName := range cmd.removeAllRepositories {
		err := manager.RemoveAllRepositories(ctx, &backup.RemoveAllRepositoriesRequest{
			StorageName: storageName,
		})
		if err != nil {
			// Treat RemoveAll failures as soft failures until we can determine
			// how often it fails.
			logger.WithError(err).WithField("storage_name", storageName).Warn("failed to remove all repositories")
		}
	}

	// Defaults to no concurrency.
	parallel := 1
	if cmd.parallel > 0 {
		parallel = cmd.parallel
	}
	parallelStorage := 1
	if cmd.parallelStorage > 0 {
		parallelStorage = cmd.parallelStorage
	}
	pipeline := backup.NewParallelPipeline(logger, parallel, parallelStorage)

	decoder := json.NewDecoder(stdin)
	for {
		var req restoreRequest
		if err := decoder.Decode(&req); errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			return fmt.Errorf("restore: %w", err)
		}

		repo := gitalypb.Repository{
			StorageName:   req.StorageName,
			RelativePath:  req.RelativePath,
			GlProjectPath: req.GlProjectPath,
		}
		pipeline.Handle(ctx, backup.NewRestoreCommand(manager, backup.RestoreRequest{
			Server:           req.ServerInfo,
			Repository:       &repo,
			VanityRepository: &repo,
			AlwaysCreate:     req.AlwaysCreate,
			BackupID:         cmd.backupID,
		}))
	}

	if err := pipeline.Done(); err != nil {
		return fmt.Errorf("restore: %w", err)
	}
	return nil
}
