package partition_test

import (
	"context"
	"io/fs"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v16/internal/archive"
	"gitlab.com/gitlab-org/gitaly/v16/internal/backup"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v16/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper/testserver"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
)

func TestBackupPartition(t *testing.T) {
	if testhelper.IsPraefectEnabled() {
		t.Skip(`Praefect currently doesn't support routing the PARTITION scoped RPC messages.`)
	}

	type setupData struct {
		cfg         config.Cfg
		ptnClient   gitalypb.PartitionServiceClient
		repoClient  gitalypb.RepositoryServiceClient
		storageName string
		partitionID string
	}

	for _, tc := range []struct {
		desc        string
		setup       func(t *testing.T, ctx context.Context, backupSink backup.Sink) setupData
		expectedErr error
	}{
		{
			desc: "success",
			setup: func(t *testing.T, ctx context.Context, backupSink backup.Sink) setupData {
				cfg, ptnClient, repoClient := setupServices(t,
					testserver.WithBackupSink(backupSink),
				)

				return setupData{
					cfg:         cfg,
					ptnClient:   ptnClient,
					repoClient:  repoClient,
					storageName: "default",
					partitionID: "2",
				}
			},
		},
		{
			desc: "invalid storage",
			setup: func(t *testing.T, ctx context.Context, backupSink backup.Sink) setupData {
				cfg, ptnClient, repoClient := setupServices(t,
					testserver.WithBackupSink(backupSink),
				)

				return setupData{
					cfg:         cfg,
					ptnClient:   ptnClient,
					repoClient:  repoClient,
					storageName: "non-existent",
					partitionID: "2",
				}
			},
			expectedErr: testhelper.WithInterceptedMetadata(
				structerr.NewInvalidArgument("get storage: storage name not found"), "storage_name", "non-existent",
			),
		},
		{
			desc: "no backup sink",
			setup: func(t *testing.T, ctx context.Context, backupSink backup.Sink) setupData {
				cfg, ptnClient, repoClient := setupServices(t)

				return setupData{
					cfg:         cfg,
					ptnClient:   ptnClient,
					repoClient:  repoClient,
					storageName: "default",
					partitionID: "2",
				}
			},
			expectedErr: structerr.NewFailedPrecondition("backup partition: server-side backups are not configured"),
		},
	} {
		tc := tc
		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			ctx := testhelper.Context(t)

			backupRoot := testhelper.TempDir(t)
			backupSink, err := backup.ResolveSink(ctx, backupRoot)
			require.NoError(t, err)

			data := tc.setup(t, ctx, backupSink)

			repo, _ := gittest.CreateRepository(t, ctx, data.cfg)

			forkRepository := &gitalypb.Repository{
				StorageName:  repo.StorageName,
				RelativePath: gittest.NewRepositoryName(t),
			}

			// Inject the Gitaly's address information in the context. CreateFork uses this to
			// fetch from the source repository.
			ctx = testhelper.MergeOutgoingMetadata(ctx, testcfg.GitalyServersMetadataFromCfg(t, data.cfg))

			createForkResponse, err := data.repoClient.CreateFork(ctx, &gitalypb.CreateForkRequest{
				Repository:       forkRepository,
				SourceRepository: repo,
			})
			require.NoError(t, err)
			testhelper.ProtoEqual(t, &gitalypb.CreateForkResponse{}, createForkResponse)

			resp, err := data.ptnClient.BackupPartition(ctx, &gitalypb.BackupPartitionRequest{
				StorageName: data.storageName,
				PartitionId: data.partitionID,
			})
			// The test relies on the interceptor being configured in the test server. If WAL is not enabled, the interceptor won't be configured,
			// and as a result the transaction won't be initialized.
			if !testhelper.IsWALEnabled() &&
				(tc.expectedErr == nil || tc.expectedErr.Error() != structerr.NewFailedPrecondition("backup partition: server-side backups are not configured").Error()) {
				tc.expectedErr = structerr.NewInternal("backup partition: transaction not initialized")
			}
			if tc.expectedErr != nil {
				testhelper.RequireGrpcError(t, tc.expectedErr, err)
				return
			}

			require.NoError(t, err)
			testhelper.ProtoEqual(t, &gitalypb.BackupPartitionResponse{}, resp)

			lsn := storage.LSN(2)
			tarPath := filepath.Join(backupRoot, data.storageName, data.partitionID, lsn.String()) + ".tar"
			tar, err := os.Open(tarPath)
			require.NoError(t, err)
			defer testhelper.MustClose(t, tar)

			testhelper.ContainsTarState(t, tar, testhelper.DirectoryState{
				".":                         {Mode: archive.TarFileMode | archive.ExecuteMode | fs.ModeDir},
				repo.RelativePath:           {Mode: archive.TarFileMode | archive.ExecuteMode | fs.ModeDir},
				forkRepository.RelativePath: {Mode: archive.TarFileMode | archive.ExecuteMode | fs.ModeDir},
			})
		})
	}
}

func TestBackupPartition_BackupExists(t *testing.T) {
	if testhelper.IsPraefectEnabled() {
		t.Skip(`Praefect currently doesn't support routing the PARTITION scoped RPC messages.`)
	}

	ctx := testhelper.Context(t)

	backupRoot := testhelper.TempDir(t)
	backupSink, err := backup.ResolveSink(ctx, backupRoot)
	require.NoError(t, err)

	_, ptnClient, _ := setupServices(t,
		testserver.WithBackupSink(backupSink),
	)

	_, err = ptnClient.BackupPartition(ctx, &gitalypb.BackupPartitionRequest{
		StorageName: "default",
		PartitionId: "1",
	})

	if testhelper.IsWALEnabled() {
		require.NoError(t, err)
	} else {
		// The test relies on the interceptor being configured in the test server. If WAL is not enabled, the interceptor won't be configured,
		// and as a result the transaction won't be initialized.
		testhelper.RequireGrpcError(t, structerr.NewInternal("backup partition: transaction not initialized"), err)
		return
	}

	// Calling the same backup again should fail as it already exists
	_, err = ptnClient.BackupPartition(ctx, &gitalypb.BackupPartitionRequest{
		StorageName: "default",
		PartitionId: "1",
	})

	testhelper.RequireGrpcError(t, structerr.NewAlreadyExists("there is an up-to-date backup for the given partition"), err)
}
