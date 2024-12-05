package migration

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/storagemgr"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper"
)

func TestCombinedMigrations(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		desc             string
		mainPartition    storagemgr.Partition
		dryRunPartition  storagemgr.Partition
		requiredErr      error
		requiredLogMsg   string
		requiredLogError error
	}{
		{
			desc: "both partitions raise no error",
			mainPartition: mockPartition{
				beginFn: func(context.Context, storage.BeginOptions) (storage.Transaction, error) {
					return mockTransaction{}, nil
				},
			},
			dryRunPartition: mockPartition{
				beginFn: func(context.Context, storage.BeginOptions) (storage.Transaction, error) {
					return mockTransaction{}, nil
				},
			},
			requiredErr: nil,
		},
		{
			desc: "main partition raises an error",
			mainPartition: mockPartition{
				beginFn: func(context.Context, storage.BeginOptions) (storage.Transaction, error) {
					return mockTransaction{}, errors.New("main partition error")
				},
			},
			dryRunPartition: mockPartition{
				beginFn: func(context.Context, storage.BeginOptions) (storage.Transaction, error) {
					return mockTransaction{}, nil
				},
			},
			requiredErr: errors.New("main partition error"),
		},
		{
			desc: "dry-run partition raises an error",
			mainPartition: mockPartition{
				beginFn: func(context.Context, storage.BeginOptions) (storage.Transaction, error) {
					return mockTransaction{}, nil
				},
			},
			dryRunPartition: mockPartition{
				beginFn: func(context.Context, storage.BeginOptions) (storage.Transaction, error) {
					return mockTransaction{}, errors.New("dryrun partition error")
				},
			},
			requiredErr:      nil,
			requiredLogMsg:   "failed to begin migration dry-run",
			requiredLogError: errors.New("dryrun partition error"),
		},
		{
			desc: "both partition raise errors",
			mainPartition: mockPartition{
				beginFn: func(context.Context, storage.BeginOptions) (storage.Transaction, error) {
					return mockTransaction{}, errors.New("main partition error")
				},
			},
			dryRunPartition: mockPartition{
				beginFn: func(context.Context, storage.BeginOptions) (storage.Transaction, error) {
					return mockTransaction{}, errors.New("dryrun partition error")
				},
			},
			requiredErr:      errors.New("main partition error"),
			requiredLogMsg:   "failed to begin migration dry-run",
			requiredLogError: errors.New("dryrun partition error"),
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			ctx := testhelper.Context(t)
			logger := testhelper.NewLogger(t)
			loggerHook := testhelper.AddLoggerHook(logger)

			partition := combinedMigrationPartition{
				Partition: tc.mainPartition,
				logger:    logger,
				dryRun:    tc.dryRunPartition,
			}

			_, err := partition.Begin(ctx, storage.BeginOptions{})
			require.Equal(t, tc.requiredErr, err)

			// We need to wait for the goroutines to finish so we can read the logs.
			partition.Close()

			if entry := loggerHook.LastEntry(); entry != nil {
				require.Equal(t, tc.requiredLogMsg, entry.Message)
				require.Equal(t, tc.requiredLogError, entry.Data["error"])
			}
		})
	}
}
