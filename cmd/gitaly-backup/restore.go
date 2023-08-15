package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"runtime"
	"strings"

	"github.com/sirupsen/logrus"
	"gitlab.com/gitlab-org/gitaly/v16/client"
	"gitlab.com/gitlab-org/gitaly/v16/internal/backup"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	internalclient "gitlab.com/gitlab-org/gitaly/v16/internal/grpc/client"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
)

type restoreRequest struct {
	storage.ServerInfo
	StorageName   string `json:"storage_name"`
	RelativePath  string `json:"relative_path"`
	GlProjectPath string `json:"gl_project_path"`
	AlwaysCreate  bool   `json:"always_create"`
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

func (cmd *restoreSubcommand) Flags(fs *flag.FlagSet) {
	fs.StringVar(&cmd.backupPath, "path", "", "repository backup path")
	fs.IntVar(&cmd.parallel, "parallel", runtime.NumCPU(), "maximum number of parallel restores")
	fs.IntVar(&cmd.parallelStorage, "parallel-storage", 2, "maximum number of parallel restores per storage. Note: actual parallelism when combined with `-parallel` depends on the order the repositories are received.")
	fs.StringVar(&cmd.layout, "layout", "pointer", "how backup files are located. Either pointer or legacy.")
	fs.Func("remove-all-repositories", "comma-separated list of storage names to have all repositories removed from before restoring.", func(removeAll string) error {
		cmd.removeAllRepositories = strings.Split(removeAll, ",")
		return nil
	})
	fs.StringVar(&cmd.backupID, "id", "", "ID of full backup to restore. If not specified, the latest backup is restored.")
	fs.BoolVar(&cmd.serverSide, "server-side", false, "use server-side backups. Note: The feature is not ready for production use.")
}

func (cmd *restoreSubcommand) Run(ctx context.Context, logger logrus.FieldLogger, stdin io.Reader, stdout io.Writer) error {
	pool := client.NewPool(internalclient.UnaryInterceptor(), internalclient.StreamInterceptor())
	defer pool.Close()

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
			logger.WithError(err).Warnf("failed to remove all repositories from %s", storageName)
		}
	}

	var pipeline backup.Pipeline
	pipeline = backup.NewLoggingPipeline(logger)
	if cmd.parallel > 0 || cmd.parallelStorage > 0 {
		pipeline = backup.NewParallelPipeline(pipeline, cmd.parallel, cmd.parallelStorage)
	}

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
