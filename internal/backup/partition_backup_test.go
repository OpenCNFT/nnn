package backup_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v16/internal/backup"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/service/setup"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v16/internal/grpc/client"
	"gitlab.com/gitlab-org/gitaly/v16/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper/testserver"
)

func TestPartitionBackup_Create(t *testing.T) {
	if testhelper.IsPraefectEnabled() {
		t.Skip(`Praefect currently doesn't support routing the PARTITION scoped RPC messages.`)
	}

	for _, tc := range []struct {
		desc             string
		storageName      string
		timeout          time.Duration
		cancelContext    bool
		expectedLastLog  string
		expectedArchives []string
		expectedErr      error
	}{
		{
			desc:        "success",
			storageName: "default",
			timeout:     time.Minute * 1,
			expectedArchives: []string{
				"2", // the partition id of the first repository
				"3", // the partition id of the second repository
			},
			expectedLastLog: "archived 1 partitions",
		},
		{
			desc:        "error",
			storageName: "non-existent",
			timeout:     time.Minute * 1,
			expectedErr: fmt.Errorf("list partitions: rpc error: code = InvalidArgument desc = %w", testhelper.WithInterceptedMetadata(
				structerr.NewInvalidArgument("get storage: storage name not found"), "storage_name", "non-existent",
			)),
			expectedLastLog: "getting partitions",
		},
		{
			desc:            "backup process exceeds timeout",
			storageName:     "default",
			timeout:         time.Millisecond * 1,
			expectedErr:     fmt.Errorf("backup partition: rpc error: code = DeadlineExceeded desc = context deadline exceeded"),
			expectedLastLog: "successfully archived 0 partitions and failed at partition id 2",
		},
		{
			desc:            "context cancelled during backup",
			storageName:     "default",
			timeout:         time.Minute * 1,
			cancelContext:   true,
			expectedLastLog: "backup operation terminated",
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			ctx, cancel := context.WithCancel(testhelper.Context(t))
			defer cancel()

			logger := testhelper.SharedLogger(t)
			loggerHook := testhelper.AddLoggerHook(logger)

			backupRoot := testhelper.TempDir(t)
			backupSink, err := backup.ResolveSink(ctx, backupRoot)
			require.NoError(t, err)

			cfg := testcfg.Build(t)
			cfg.SocketPath = testserver.RunGitalyServer(t, cfg, setup.RegisterAll,
				testserver.WithBackupSink(backupSink),
			)

			// Creating repositories will assign them to partitions.
			gittest.CreateRepository(t, ctx, cfg)
			gittest.CreateRepository(t, ctx, cfg)

			pool := client.NewPool()
			defer testhelper.MustClose(t, pool)

			// Setting the pagination limit to 1, so that we can confirm that pagination works.
			manager := backup.NewPartititonBackupManager(pool).WithPaginationLimit(1).WithBackupTimeout(tc.timeout)

			if tc.cancelContext {
				go func() {
					time.Sleep(time.Millisecond * 1)
					cancel()
				}()
			}

			err = manager.Create(
				ctx,
				storage.ServerInfo{
					Address: cfg.SocketPath,
				},
				tc.storageName,
				logger,
			)

			// The test relies on the interceptor being configured in the test server. If WAL is not enabled, the interceptor won't be configured,
			// and as a result the transaction won't be initialized.
			if !testhelper.IsWALEnabled() &&
				(tc.expectedErr == nil || tc.expectedErr.Error() != structerr.NewFailedPrecondition("backup partition: server-side backups are not configured").Error()) {
				tc.expectedErr = structerr.NewInternal("list partitions: rpc error: code = Internal desc = transactions not enabled")
			}
			if tc.expectedErr != nil {
				require.Error(t, tc.expectedErr, err)
				return
			}

			require.NoError(t, err)

			logEntries := loggerHook.AllEntries()
			lastEntry := logEntries[len(logEntries)-1]
			require.Contains(t, lastEntry.Message, tc.expectedLastLog)

			for _, expectedArchive := range tc.expectedArchives {
				tarPath := filepath.Join(backupRoot, cfg.Storages[0].Name, expectedArchive, storage.LSN(1).String()) + ".tar"
				tar, err := os.Open(tarPath)
				require.NoError(t, err)
				testhelper.MustClose(t, tar)
			}

			// When trying to create duplicate backup, we should simply skip instead of returning an error.
			require.NoError(t, manager.Create(
				testhelper.Context(t),
				storage.ServerInfo{
					Address: cfg.SocketPath,
				},
				tc.storageName,
				logger,
			))
		})
	}
}
