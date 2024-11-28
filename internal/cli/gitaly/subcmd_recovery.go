package gitaly

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/urfave/cli/v2"
	"gitlab.com/gitlab-org/gitaly/v16"
	"gitlab.com/gitlab-org/gitaly/v16/internal/backup"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/catfile"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/gitcmd"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/housekeeping"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/keyvalue"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/keyvalue/databasemgr"
	nodeimpl "gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/node"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/storagemgr"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/storagemgr/partition"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/storagemgr/partition/migration"
	"gitlab.com/gitlab-org/gitaly/v16/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v16/internal/log"
)

const (
	flagPartition = "partition"
)

func newRecoveryCommand() *cli.Command {
	return &cli.Command{
		Name:      "recovery",
		Usage:     "manage partitions offline",
		UsageText: "gitaly recovery --config <gitaly_config_file> command [command options]",
		Flags: []cli.Flag{
			gitalyConfigFlag(),
		},
		Subcommands: []*cli.Command{
			{
				Name:  "status",
				Usage: "shows the status of a partition",
				UsageText: `gitaly recovery --config <gitaly_config_file> status [command options]

Example: gitaly recovery --config gitaly.config.toml status --storage default --partition 2`,
				Action: recoveryStatusAction,
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:  flagStorage,
						Usage: "storage containing the partition",
					},
					&cli.StringFlag{
						Name:  flagPartition,
						Usage: "partition ID",
					},
				},
			},
		},
	}
}

func recoveryStatusAction(ctx *cli.Context) (returnErr error) {
	logger := log.ConfigureCommand()

	cfg, err := loadConfig(ctx.String(flagConfig))
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}

	runtimeDir, err := os.MkdirTemp("", "gitaly-recovery-*")
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

	dbMgr, err := databasemgr.NewDBManager(
		ctx.Context,
		cfg.Storages,
		keyvalue.NewBadgerStore,
		helper.NewTimerTickerFactory(time.Minute),
		logger,
	)
	if err != nil {
		return fmt.Errorf("new db manager: %w", err)
	}
	defer dbMgr.Close()

	locator := config.NewLocator(cfg)

	gitCmdFactory, cleanup, err := gitcmd.NewExecCommandFactory(cfg, logger)
	if err != nil {
		return fmt.Errorf("creating Git command factory: %w", err)
	}
	defer cleanup()

	catfileCache := catfile.NewCache(cfg)
	defer catfileCache.Stop()

	housekeepingMetrics := housekeeping.NewMetrics(cfg.Prometheus)
	partitionMetrics := partition.NewMetrics(housekeepingMetrics)
	storageMetrics := storagemgr.NewMetrics(cfg.Prometheus)

	node, err := nodeimpl.NewManager(
		cfg.Storages,
		storagemgr.NewFactory(
			logger,
			dbMgr,
			migration.NewFactory(
				partition.NewFactory(
					gitCmdFactory,
					localrepo.NewFactory(logger, locator, gitCmdFactory, catfileCache),
					partitionMetrics,
					nil,
				),
			),
			1,
			storageMetrics,
		),
	)
	if err != nil {
		return fmt.Errorf("new node: %w", err)
	}
	defer node.Close()

	storageName := ctx.String(flagStorage)
	if storageName == "" {
		if len(cfg.Storages) != 1 {
			return fmt.Errorf("multiple storages configured: use --storage to specify the one you want")
		}

		storageName = cfg.Storages[0].Name
	}

	nodeStorage, err := node.GetStorage(storageName)
	if err != nil {
		return fmt.Errorf("get storage: %w", err)
	}

	var partitionID storage.PartitionID
	if err := parsePartitionID(&partitionID, ctx.String(flagPartition)); err != nil {
		return fmt.Errorf("parse partition ID: %w", err)
	}

	if partitionID == 0 {
		return fmt.Errorf("invalid partition ID %s", partitionID)
	}

	partition, err := nodeStorage.GetPartition(ctx.Context, partitionID)
	if err != nil {
		return fmt.Errorf("get partition: %w", err)
	}
	defer partition.Close()

	txn, err := partition.Begin(ctx.Context, storage.BeginOptions{
		RelativePaths: []string{},
	})
	if err != nil {
		return fmt.Errorf("begin: %w", err)
	}
	defer func() {
		err := txn.Rollback(ctx.Context)
		returnErr = errors.Join(returnErr, err)
	}()

	appliedLSN := txn.SnapshotLSN()
	relativePaths := txn.PartitionRelativePaths()

	fmt.Fprintf(ctx.App.Writer, "Partition ID: %s\n", partitionID.String())
	fmt.Fprintf(ctx.App.Writer, "Applied LSN: %s\n", appliedLSN.String())

	if len(relativePaths) > 0 {
		fmt.Fprintf(ctx.App.Writer, "Relative paths:\n")
		for _, relativePath := range relativePaths {
			fmt.Fprintf(ctx.App.Writer, " - %s\n", relativePath)
		}
	}

	if cfg.Backup.WALGoCloudURL == "" {
		return fmt.Errorf("write-ahead log backup is not configured")
	}

	sink, err := backup.ResolveSink(ctx.Context, cfg.Backup.WALGoCloudURL)
	if err != nil {
		return fmt.Errorf("resolve sink: %w", err)
	}
	logEntryStore := backup.NewLogEntryStore(sink)
	entries := logEntryStore.Query(backup.PartitionInfo{
		PartitionID: partitionID,
		StorageName: storageName,
	}, appliedLSN+1)

	fmt.Fprintf(ctx.App.Writer, "Available backup entries:\n")
	for entries.Next(ctx.Context) {
		fmt.Fprintf(ctx.App.Writer, " - %s\n", entries.LSN())
	}

	if err := entries.Err(); err != nil {
		return fmt.Errorf("query log entry store: %w", err)
	}

	return nil
}

func parsePartitionID(id *storage.PartitionID, value string) error {
	parsedID, err := strconv.ParseUint(value, 10, 64)
	if err != nil {
		return fmt.Errorf("parse partition ID: %w", err)
	}

	*id = storage.PartitionID(parsedID)

	return nil
}
