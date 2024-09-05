package storagemgr

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/keyvalue"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/keyvalue/databasemgr"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/mode"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/storagemgr/snapshot"
	"gitlab.com/gitlab-org/gitaly/v16/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v16/internal/log"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
)

type mockPartitionFactory struct {
	new func(
		logger log.Logger,
		partitionID storage.PartitionID,
		db keyvalue.Transactioner,
		storageName string,
		storagePath string,
		absoluteStateDir string,
		stagingDir string,
		logConsumer storage.LogConsumer,
	) Partition
}

// newStubPartitionFactory returns a partition factory that doesn't do anything and calls
// on its methods succeed.
func newStubPartitionFactory() PartitionFactory {
	return mockPartitionFactory{
		new: func(
			logger log.Logger,
			partitionID storage.PartitionID,
			db keyvalue.Transactioner,
			storageName string,
			storagePath string,
			absoluteStateDir string,
			stagingDir string,
			logConsumer storage.LogConsumer,
		) Partition {
			closing := make(chan struct{})
			isClosing := func() bool {
				select {
				case <-closing:
					return true
				default:
					return false
				}
			}

			// This stub emulates what a real partition implementation.
			return mockPartition{
				run: func() error {
					// Run returns after the partition has been closed.
					<-closing
					return nil
				},
				begin: func(ctx context.Context, opts storage.BeginOptions) (storage.Transaction, error) {
					// Transactions fail to begin if context is done.
					if err := ctx.Err(); err != nil {
						return nil, ctx.Err()
					}

					return mockTransaction{
						commit: func(ctx context.Context) error {
							// Commits fail if partition is closed.
							if isClosing() {
								return storage.ErrTransactionProcessingStopped
							}

							// Commits fail if context is done.
							return ctx.Err()
						},
						rollback: func() error { return nil },
					}, nil
				},
				close: func() {
					// Closing is idempotent.
					if !isClosing() {
						close(closing)
					}
				},
				isClosing: isClosing,
			}
		},
	}
}

func (m mockPartitionFactory) New(
	logger log.Logger,
	partitionID storage.PartitionID,
	db keyvalue.Transactioner,
	storageName string,
	storagePath string,
	absoluteStateDir string,
	stagingDir string,
	logConsumer storage.LogConsumer,
) Partition {
	return m.new(
		logger,
		partitionID,
		db,
		storageName,
		storagePath,
		absoluteStateDir,
		stagingDir,
		logConsumer,
	)
}

type mockPartition struct {
	begin     func(context.Context, storage.BeginOptions) (storage.Transaction, error)
	run       func() error
	close     func()
	isClosing func() bool
	storage.LogManager
}

func (m mockPartition) Begin(ctx context.Context, opts storage.BeginOptions) (storage.Transaction, error) {
	return m.begin(ctx, opts)
}

func (m mockPartition) Run() error {
	return m.run()
}

func (m mockPartition) Close() {
	m.close()
}

func (m mockPartition) IsClosing() bool {
	return m.isClosing()
}

type mockTransaction struct {
	storage.Transaction
	commit   func(context.Context) error
	rollback func() error
}

func (m mockTransaction) Commit(ctx context.Context) error {
	return m.commit(ctx)
}

func (m mockTransaction) Rollback() error { return m.rollback() }

func requirePartitionOpen(t *testing.T, storageMgr *StorageManager, ptnID storage.PartitionID, expectOpen bool) {
	t.Helper()

	storageMgr.mu.Lock()
	defer storageMgr.mu.Unlock()
	_, ptnOpen := storageMgr.partitions[ptnID]
	require.Equal(t, expectOpen, ptnOpen)
}

// blockOnPartitionClosing checks if any partitions are currently in the process of
// closing. If some are, the function waits for the closing process to complete before
// continuing. This is required in order to accurately validate partition state.
func blockOnPartitionClosing(t *testing.T, mgr *StorageManager, waitForFullClose bool) {
	t.Helper()

	var waitFor []chan struct{}
	mgr.mu.Lock()
	for _, ptn := range mgr.partitions {
		// The closePartition step closes the transaction manager directly without calling close
		// on the partition, so we check the manager directly here as well.
		if ptn.isClosing() || ptn.partition.IsClosing() {
			waiter := ptn.partitionClosed
			if waitForFullClose {
				waiter = ptn.closed
			}

			waitFor = append(waitFor, waiter)
		}
	}
	mgr.mu.Unlock()

	for _, closed := range waitFor {
		<-closed
	}
}

func TestStorageManager(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)

	// steps defines execution steps in a test. Each test case can define multiple steps to exercise
	// more complex behavior.
	type steps []any

	// begin calls Begin on the TransactionManager to start a new transaction.
	type begin struct {
		// transactionID is the identifier given to the transaction created. This is used to identify
		// the transaction in later steps.
		transactionID int
		// ctx is the context used when `Begin()` gets invoked.
		ctx context.Context
		// repo is the repository that the transaction belongs to.
		repo storage.Repository
		// partitionID is the partition that the transaction belongs to.
		// Overwritten by the repo partition ID if repo is set.
		partitionID storage.PartitionID
		// alternateRelativePath is the relative path of the alternate repository.
		alternateRelativePath string
		// readOnly indicates if the transaction is read-only.
		readOnly bool
		// expectedState contains the partitions and their pending transaction count at
		// the end of the step.
		expectedState map[storage.PartitionID]uint
		// expectedError is the error expected to be returned when beginning the transaction.
		expectedError error
	}

	// commit calls Commit on a transaction.
	type commit struct {
		// transactionID identifies the transaction to commit.
		transactionID int
		// ctx is the context used when `Commit()` gets invoked.
		ctx context.Context
		// expectedState contains the partitions and their pending transaction count at
		// the end of the step.
		expectedState map[storage.PartitionID]uint
		// expectedError is the error that is expected to be returned when committing the transaction.
		expectedError error
	}

	// rollback calls Rollback on a transaction.
	type rollback struct {
		// transactionID identifies the transaction to rollback.
		transactionID int
		// expectedState contains the partitions and their pending transaction count at
		// the end of the step.
		expectedState map[storage.PartitionID]uint
		// expectedError is the error that is expected to be returned when rolling back the transaction.
		expectedError error
	}

	// closePartition closes the transaction manager for the specified repository. This is done to
	// simulate failures.
	type closePartition struct {
		// transactionID identifies the transaction manager associated with the transaction to stop.
		transactionID int
	}

	// finalizeTransaction runs the transaction finalizer for the specified repository. This is used
	// to simulate finalizers executing after a transaction manager has been stopped.
	type finalizeTransaction struct {
		// transactionID identifies the transaction to finalize.
		transactionID int
	}

	// closeManager closes the partition manager. This is done to simulate errors for transactions
	// being processed without a running partition manager.
	type closeManager struct{}

	// assertMetrics is a step used to assert the current state of metrics.
	type assertMetrics struct {
		partitionsStartedTotal uint64
		partitionsStoppedTotal uint64
	}

	// checkExpectedState validates that the partition manager contains the correct partitions and
	// associated transaction count at the point of execution.
	checkExpectedState := func(t *testing.T, cfg config.Cfg, mgr *StorageManager, expectedState map[storage.PartitionID]uint) {
		t.Helper()

		actualState := map[storage.PartitionID]uint{}
		for ptnID, partition := range mgr.partitions {
			actualState[ptnID] = partition.pendingTransactionCount
		}

		if expectedState == nil {
			expectedState = map[storage.PartitionID]uint{}
		}

		require.Equal(t, expectedState, actualState)
	}

	setupRepository := func(t *testing.T, cfg config.Cfg, storage config.Storage) storage.Repository {
		t.Helper()

		repo, _ := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
			Storage:                storage,
			SkipCreationViaService: true,
		})

		return repo
	}

	// transactionData holds relevant data for each transaction created during a testcase.
	type transactionData struct {
		txn        *finalizableTransaction
		storageMgr *StorageManager
		ptn        *partition
	}

	type setupData struct {
		steps            steps
		partitionFactory PartitionFactory
	}

	for _, tc := range []struct {
		desc  string
		setup func(t *testing.T, cfg config.Cfg) setupData
	}{
		{
			desc: "transaction committed for single repository",
			setup: func(t *testing.T, cfg config.Cfg) setupData {
				repo := setupRepository(t, cfg, cfg.Storages[0])

				return setupData{
					steps: steps{
						begin{
							repo: repo,
							expectedState: map[storage.PartitionID]uint{
								2: 1,
							},
						},
						commit{},
					},
				}
			},
		},
		{
			desc: "two transactions committed for single repository sequentially",
			setup: func(t *testing.T, cfg config.Cfg) setupData {
				repo := setupRepository(t, cfg, cfg.Storages[0])

				return setupData{
					steps: steps{
						begin{
							transactionID: 1,
							repo:          repo,
							expectedState: map[storage.PartitionID]uint{
								2: 1,
							},
						},
						commit{
							transactionID: 1,
						},
						begin{
							transactionID: 2,
							repo:          repo,
							expectedState: map[storage.PartitionID]uint{
								2: 1,
							},
						},
						commit{
							transactionID: 2,
						},
					},
				}
			},
		},
		{
			desc: "two transactions committed for single repository in parallel",
			setup: func(t *testing.T, cfg config.Cfg) setupData {
				repo := setupRepository(t, cfg, cfg.Storages[0])

				return setupData{
					steps: steps{
						begin{
							transactionID: 1,
							repo:          repo,
							expectedState: map[storage.PartitionID]uint{
								2: 1,
							},
						},
						begin{
							transactionID: 2,
							repo:          repo,
							expectedState: map[storage.PartitionID]uint{
								2: 2,
							},
						},
						commit{
							transactionID: 1,
							expectedState: map[storage.PartitionID]uint{
								2: 1,
							},
						},
						commit{
							transactionID: 2,
						},
					},
				}
			},
		},
		{
			desc: "transaction committed for multiple repositories",
			setup: func(t *testing.T, cfg config.Cfg) setupData {
				repoA := setupRepository(t, cfg, cfg.Storages[0])
				repoB := setupRepository(t, cfg, cfg.Storages[0])
				repoC := setupRepository(t, cfg, cfg.Storages[0])

				return setupData{
					steps: steps{
						begin{
							transactionID: 1,
							repo:          repoA,
							expectedState: map[storage.PartitionID]uint{
								2: 1,
							},
						},
						begin{
							transactionID: 2,
							repo:          repoB,
							expectedState: map[storage.PartitionID]uint{
								2: 1,
								3: 1,
							},
						},
						begin{
							transactionID: 3,
							repo:          repoC,
							expectedState: map[storage.PartitionID]uint{
								2: 1,
								3: 1,
								4: 1,
							},
						},
						commit{
							transactionID: 1,
							expectedState: map[storage.PartitionID]uint{
								3: 1,
								4: 1,
							},
						},
						commit{
							transactionID: 2,
							expectedState: map[storage.PartitionID]uint{
								4: 1,
							},
						},
						commit{
							transactionID: 3,
						},
					},
				}
			},
		},
		{
			desc: "transaction rolled back for single repository",
			setup: func(t *testing.T, cfg config.Cfg) setupData {
				repo := setupRepository(t, cfg, cfg.Storages[0])

				return setupData{
					steps: steps{
						begin{
							repo: repo,
							expectedState: map[storage.PartitionID]uint{
								2: 1,
							},
						},
						rollback{},
					},
				}
			},
		},
		{
			desc: "starting transaction failed due to cancelled context",
			setup: func(t *testing.T, cfg config.Cfg) setupData {
				repo := setupRepository(t, cfg, cfg.Storages[0])

				stepCtx, cancel := context.WithCancel(ctx)
				cancel()

				return setupData{
					steps: steps{
						begin{
							ctx:           stepCtx,
							repo:          repo,
							expectedError: context.Canceled,
						},
					},
				}
			},
		},
		{
			desc: "committing transaction failed due to cancelled context",
			setup: func(t *testing.T, cfg config.Cfg) setupData {
				repo := setupRepository(t, cfg, cfg.Storages[0])

				stepCtx, cancel := context.WithCancel(ctx)
				cancel()

				return setupData{
					steps: steps{
						begin{
							repo: repo,
							expectedState: map[storage.PartitionID]uint{
								2: 1,
							},
						},
						commit{
							ctx:           stepCtx,
							expectedError: context.Canceled,
						},
					},
				}
			},
		},
		{
			desc: "committing transaction failed due to stopped transaction manager",
			setup: func(t *testing.T, cfg config.Cfg) setupData {
				repo := setupRepository(t, cfg, cfg.Storages[0])

				return setupData{
					steps: steps{
						begin{
							repo: repo,
							expectedState: map[storage.PartitionID]uint{
								2: 1,
							},
						},
						closePartition{},
						commit{
							expectedError: storage.ErrTransactionProcessingStopped,
						},
					},
				}
			},
		},
		{
			desc: "transaction from previous transaction manager finalized after new manager started",
			setup: func(t *testing.T, cfg config.Cfg) setupData {
				repo := setupRepository(t, cfg, cfg.Storages[0])

				return setupData{
					steps: steps{
						begin{
							transactionID: 1,
							repo:          repo,
							expectedState: map[storage.PartitionID]uint{
								2: 1,
							},
						},
						closePartition{
							transactionID: 1,
						},
						begin{
							transactionID: 2,
							repo:          repo,
							expectedState: map[storage.PartitionID]uint{
								2: 1,
							},
						},
						finalizeTransaction{
							transactionID: 1,
						},
						commit{
							transactionID: 2,
						},
					},
				}
			},
		},
		{
			desc: "transaction started after partition manager stopped",
			setup: func(t *testing.T, cfg config.Cfg) setupData {
				repo := setupRepository(t, cfg, cfg.Storages[0])

				return setupData{
					steps: steps{
						closeManager{},
						begin{
							repo:          repo,
							expectedError: ErrPartitionManagerClosed,
						},
					},
				}
			},
		},
		{
			desc: "multiple transactions started after partition manager stopped",
			setup: func(t *testing.T, cfg config.Cfg) setupData {
				repo := setupRepository(t, cfg, cfg.Storages[0])

				return setupData{
					steps: steps{
						closeManager{},
						begin{
							transactionID: 1,
							repo:          repo,
							expectedError: ErrPartitionManagerClosed,
						},
						begin{
							transactionID: 2,
							repo:          repo,
							expectedError: ErrPartitionManagerClosed,
						},
					},
				}
			},
		},
		{
			desc: "relative paths are cleaned",
			setup: func(t *testing.T, cfg config.Cfg) setupData {
				repo := setupRepository(t, cfg, cfg.Storages[0])

				return setupData{
					steps: steps{
						begin{
							transactionID: 1,
							repo:          repo,
							expectedState: map[storage.PartitionID]uint{
								2: 1,
							},
						},
						begin{
							transactionID: 2,
							repo: &gitalypb.Repository{
								StorageName:  repo.GetStorageName(),
								RelativePath: filepath.Join(repo.GetRelativePath(), "child-dir", ".."),
							},
							expectedState: map[storage.PartitionID]uint{
								2: 2,
							},
						},
					},
				}
			},
		},
		{
			desc: "transaction finalized only once",
			setup: func(t *testing.T, cfg config.Cfg) setupData {
				repo := setupRepository(t, cfg, cfg.Storages[0])

				return setupData{
					steps: steps{
						begin{
							transactionID: 1,
							repo:          repo,
							expectedState: map[storage.PartitionID]uint{
								2: 1,
							},
						},
						begin{
							transactionID: 2,
							repo: &gitalypb.Repository{
								StorageName:  repo.GetStorageName(),
								RelativePath: repo.GetRelativePath(),
							},
							expectedState: map[storage.PartitionID]uint{
								2: 2,
							},
						},
						rollback{
							transactionID: 2,
							expectedState: map[storage.PartitionID]uint{
								2: 1,
							},
						},
						rollback{
							transactionID: 2,
							expectedState: map[storage.PartitionID]uint{
								2: 1,
							},
						},
					},
				}
			},
		},
		{
			desc: "repository and alternate target the same partition",
			setup: func(t *testing.T, cfg config.Cfg) setupData {
				repo := setupRepository(t, cfg, cfg.Storages[0])
				alternateRepo := setupRepository(t, cfg, cfg.Storages[0])

				return setupData{
					steps: steps{
						begin{
							transactionID:         1,
							repo:                  repo,
							alternateRelativePath: alternateRepo.GetRelativePath(),
							expectedState: map[storage.PartitionID]uint{
								2: 1,
							},
						},
						begin{
							transactionID: 2,
							repo:          repo,
							expectedState: map[storage.PartitionID]uint{
								2: 2,
							},
						},
						begin{
							transactionID: 3,
							repo:          alternateRepo,
							expectedState: map[storage.PartitionID]uint{
								2: 3,
							},
						},
						rollback{
							transactionID: 1,
							expectedState: map[storage.PartitionID]uint{
								2: 2,
							},
						},
						rollback{
							transactionID: 2,
							expectedState: map[storage.PartitionID]uint{
								2: 1,
							},
						},
						rollback{
							transactionID: 3,
						},
					},
				}
			},
		},
		{
			desc: "beginning transaction on repositories in different partitions fails",
			setup: func(t *testing.T, cfg config.Cfg) setupData {
				repo1 := setupRepository(t, cfg, cfg.Storages[0])
				repo2 := setupRepository(t, cfg, cfg.Storages[0])

				return setupData{
					steps: steps{
						begin{
							transactionID: 1,
							repo:          repo1,
							expectedState: map[storage.PartitionID]uint{
								2: 1,
							},
						},
						begin{
							transactionID: 2,
							repo:          repo2,
							expectedState: map[storage.PartitionID]uint{
								2: 1,
								3: 1,
							},
						},
						begin{
							transactionID:         3,
							repo:                  repo1,
							alternateRelativePath: repo2.GetRelativePath(),
							expectedState: map[storage.PartitionID]uint{
								2: 1,
								3: 1,
							},
							expectedError: fmt.Errorf("get partition: %w", ErrRepositoriesAreInDifferentPartitions),
						},
					},
				}
			},
		},
		{
			desc: "transaction committed for partition",
			setup: func(t *testing.T, cfg config.Cfg) setupData {
				return setupData{
					steps: steps{
						begin{
							partitionID: 2,
							readOnly:    true,
							expectedState: map[storage.PartitionID]uint{
								2: 1,
							},
						},
						commit{},
					},
				}
			},
		},
		{
			desc: "clears read-only directory in staging directory",
			setup: func(t *testing.T, cfg config.Cfg) setupData {
				internalDir := internalDirectoryPath(cfg.Storages[0].Path)
				stagingDir := stagingDirectoryPath(internalDir)

				// Create a staging directory as it would exist if Gitaly terminated uncleanly.
				require.NoError(t, os.MkdirAll(stagingDir, mode.Directory))

				// Create a read-only directory that contains some other files to emulate a shared snapshot
				// with read-only directories in it.
				readOnlyDir := filepath.Join(stagingDir, "read-only-dir")
				require.NoError(t, os.Mkdir(readOnlyDir, mode.Directory))
				require.NoError(t, os.WriteFile(filepath.Join(readOnlyDir, "file-to-remove"), nil, mode.File))
				require.NoError(t, storage.SetDirectoryMode(readOnlyDir, snapshot.ModeReadOnlyDirectory))

				// We don't have any steps in the test as we're just asserting that StorageManager initializes
				// correctly and removes read-only directories in staging directory.
				return setupData{}
			},
		},
		{
			desc: "transaction committed for partition with relative path filter",
			setup: func(t *testing.T, cfg config.Cfg) setupData {
				repo := setupRepository(t, cfg, cfg.Storages[0])

				return setupData{
					steps: steps{
						begin{
							partitionID: 2,
							repo:        repo,
							expectedState: map[storage.PartitionID]uint{
								2: 1,
							},
						},
						commit{},
					},
				}
			},
		},
		{
			desc: "beginning transaction on partition with relative path filter on different partition fails",
			setup: func(t *testing.T, cfg config.Cfg) setupData {
				repo := setupRepository(t, cfg, cfg.Storages[0])

				return setupData{
					steps: steps{
						begin{
							partitionID:   100,
							repo:          repo,
							expectedError: fmt.Errorf("partition ID does not match repository partition"),
						},
					},
				}
			},
		},
		{
			desc: "records metrics correctly",
			setup: func(t *testing.T, cfg config.Cfg) setupData {
				return setupData{
					steps: steps{
						begin{
							transactionID: 1,
							partitionID:   2,
							readOnly:      true,
							expectedState: map[storage.PartitionID]uint{
								2: 1,
							},
						},
						assertMetrics{
							partitionsStartedTotal: 1,
						},
						begin{
							transactionID: 2,
							partitionID:   2,
							readOnly:      true,
							expectedState: map[storage.PartitionID]uint{
								2: 2,
							},
						},
						commit{
							transactionID: 1,
							expectedState: map[storage.PartitionID]uint{
								2: 1,
							},
						},
						assertMetrics{
							partitionsStartedTotal: 1,
						},
						commit{transactionID: 2},
						assertMetrics{
							partitionsStartedTotal: 1,
							partitionsStoppedTotal: 1,
						},
						begin{
							transactionID: 3,
							partitionID:   2,
							readOnly:      true,
							expectedState: map[storage.PartitionID]uint{
								2: 1,
							},
						},
						commit{transactionID: 3},
						assertMetrics{
							partitionsStartedTotal: 2,
							partitionsStoppedTotal: 2,
						},
					},
				}
			},
		},
	} {
		tc := tc
		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			cfg := testcfg.Build(t)
			logger := testhelper.SharedLogger(t)

			setup := tc.setup(t, cfg)

			// Create some existing content in the staging directory so we can assert it gets removed and
			// recreated.
			for _, storage := range cfg.Storages {
				require.NoError(t,
					os.MkdirAll(
						filepath.Join(stagingDirectoryPath(internalDirectoryPath(storage.Path)), "existing-content"),
						mode.Directory,
					),
				)
			}

			dbMgr, err := databasemgr.NewDBManager(cfg.Storages, keyvalue.NewBadgerStore, helper.NewNullTickerFactory(), logger)
			require.NoError(t, err)
			defer dbMgr.Close()

			partitionFactory := setup.partitionFactory
			if partitionFactory == nil {
				partitionFactory = newStubPartitionFactory()
			}

			metrics := NewMetrics(cfg.Prometheus)
			storageMgr, err := NewStorageManager(
				logger,
				cfg.Storages[0].Name,
				cfg.Storages[0].Path,
				dbMgr,
				partitionFactory,
				nil,
				metrics,
			)
			require.NoError(t, err)

			defer func() {
				storageMgr.Close()
				dbMgr.Close()
				for _, storage := range cfg.Storages {
					// Assert all staging directories have been emptied at the end.
					testhelper.RequireDirectoryState(t, internalDirectoryPath(storage.Path), "staging", testhelper.DirectoryState{
						"/staging": {Mode: mode.Directory},
					})
				}
			}()

			for _, storage := range cfg.Storages {
				// Assert the existing content in the staging directory was removed.
				testhelper.RequireDirectoryState(t, internalDirectoryPath(storage.Path), "staging", testhelper.DirectoryState{
					"/staging": {Mode: mode.Directory},
				})
			}

			// openTransactionData holds references to all transactions and its associated partition
			// created during the testcase.
			openTransactionData := map[int]*transactionData{}

			var storageManagerStopped bool
			for _, step := range setup.steps {
				switch step := step.(type) {
				case begin:
					require.NotContains(t, openTransactionData, step.transactionID, "test error: transaction id reused in begin")

					beginCtx := ctx
					if step.ctx != nil {
						beginCtx = step.ctx
					}

					var relativePath string
					if step.repo != nil {
						relativePath = step.repo.GetRelativePath()
					}
					txn, err := storageMgr.Begin(beginCtx, step.partitionID, TransactionOptions{
						RelativePath:          relativePath,
						AlternateRelativePath: step.alternateRelativePath,
						ReadOnly:              step.readOnly,
					})
					require.Equal(t, step.expectedError, err)

					blockOnPartitionClosing(t, storageMgr, true)
					checkExpectedState(t, cfg, storageMgr, step.expectedState)

					if err != nil {
						continue
					}

					storageMgr.mu.Lock()

					ptnID := step.partitionID
					if step.repo != nil {
						var err error
						ptnID, err = storageMgr.partitionAssigner.getPartitionID(ctx, relativePath, "", false)
						require.NoError(t, err)
					}

					ptn := storageMgr.partitions[ptnID]
					storageMgr.mu.Unlock()

					openTransactionData[step.transactionID] = &transactionData{
						txn:        txn,
						storageMgr: storageMgr,
						ptn:        ptn,
					}
				case commit:
					require.Contains(t, openTransactionData, step.transactionID, "test error: transaction committed before being started")

					data := openTransactionData[step.transactionID]

					commitCtx := ctx
					if step.ctx != nil {
						commitCtx = step.ctx
					}

					require.ErrorIs(t, data.txn.Commit(commitCtx), step.expectedError)

					blockOnPartitionClosing(t, storageMgr, true)
					checkExpectedState(t, cfg, storageMgr, step.expectedState)
				case rollback:
					require.Contains(t, openTransactionData, step.transactionID, "test error: transaction rolled back before being started")

					data := openTransactionData[step.transactionID]
					require.ErrorIs(t, data.txn.Rollback(), step.expectedError)

					blockOnPartitionClosing(t, storageMgr, true)
					checkExpectedState(t, cfg, storageMgr, step.expectedState)
				case closePartition:
					require.Contains(t, openTransactionData, step.transactionID, "test error: transaction manager stopped before being started")

					data := openTransactionData[step.transactionID]
					// Close the Partition instance directly. Closing through the partition wrapper would change
					// the state used to sync which should only be changed when the closing is initiated through
					// the normal means.
					data.ptn.partition.Close()

					blockOnPartitionClosing(t, storageMgr, false)
				case finalizeTransaction:
					require.Contains(t, openTransactionData, step.transactionID, "test error: transaction finalized before being started")

					data := openTransactionData[step.transactionID]

					data.storageMgr.finalizeTransaction(data.ptn)
				case closeManager:
					require.False(t, storageManagerStopped, "test error: storage manager already stopped")
					storageManagerStopped = true

					storageMgr.Close()
				case assertMetrics:
					testhelper.RequirePromMetrics(t, metrics, fmt.Sprintf(`
# HELP gitaly_partitions_started_total Number of partitions started.
# TYPE gitaly_partitions_started_total counter
gitaly_partitions_started_total{storage="default"} %d
# HELP gitaly_partitions_stopped_total Number of partitions stopped.
# TYPE gitaly_partitions_stopped_total counter
gitaly_partitions_stopped_total{storage="default"} %d
					`,
						step.partitionsStartedTotal,
						step.partitionsStoppedTotal,
					))
				}
			}
		})
	}
}

func TestStorageManager_concurrentClose(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)

	cfg := testcfg.Build(t)
	logger := testhelper.SharedLogger(t)

	dbMgr, err := databasemgr.NewDBManager(cfg.Storages, keyvalue.NewBadgerStore, helper.NewNullTickerFactory(), logger)
	require.NoError(t, err)
	defer dbMgr.Close()

	storageName := cfg.Storages[0].Name
	storageMgr, err := NewStorageManager(logger, storageName, cfg.Storages[0].Path, dbMgr, newStubPartitionFactory(), nil, NewMetrics(cfg.Prometheus))
	require.NoError(t, err)
	defer storageMgr.Close()

	tx, err := storageMgr.Begin(ctx, 0, TransactionOptions{
		RelativePath: "relative-path",
		AllowPartitionAssignmentWithoutRepository: true,
	})
	require.NoError(t, err)

	start := make(chan struct{})

	var wg sync.WaitGroup
	wg.Add(3)

	// The last active transaction finishing will close partition.
	go func() {
		defer wg.Done()
		<-start
		assert.NoError(t, tx.Rollback())
	}()

	// StorageManager may be closed if the server is shutting down.
	go func() {
		defer wg.Done()
		<-start
		storageMgr.Close()
	}()

	// The Partition may return if it errors out.
	txMgr := storageMgr.partitions[2].partition
	go func() {
		defer wg.Done()
		<-start
		txMgr.Close()
	}()

	close(start)

	wg.Wait()
}

func TestStorageManager_callLogManager(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)

	cfg := testcfg.Build(t)
	logger := testhelper.SharedLogger(t)

	dbMgr, err := databasemgr.NewDBManager(cfg.Storages, keyvalue.NewBadgerStore, helper.NewNullTickerFactory(), logger)
	require.NoError(t, err)

	storageName := cfg.Storages[0].Name
	storageMgr, err := NewStorageManager(logger, storageName, cfg.Storages[0].Path, dbMgr, newStubPartitionFactory(), nil, NewMetrics(cfg.Prometheus))
	require.NoError(t, err)

	defer func() {
		storageMgr.Close()
		dbMgr.Close()
	}()

	ptnID := storage.PartitionID(1)
	requirePartitionOpen(t, storageMgr, ptnID, false)

	require.NoError(t, storageMgr.CallLogManager(ctx, ptnID, func(lm storage.LogManager) {
		requirePartitionOpen(t, storageMgr, ptnID, true)
		tm, ok := lm.(Partition)
		require.True(t, ok)

		require.False(t, tm.IsClosing())
	}))

	blockOnPartitionClosing(t, storageMgr, true)
	requirePartitionOpen(t, storageMgr, ptnID, false)
}
