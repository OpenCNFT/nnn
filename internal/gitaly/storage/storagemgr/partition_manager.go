package storagemgr

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sync"

	"github.com/dgraph-io/badger/v4"
	"github.com/prometheus/client_golang/prometheus"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/gitcmd"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/config"
	gitalycfgprom "gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/config/prometheus"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/keyvalue"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/keyvalue/databasemgr"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/mode"
	"gitlab.com/gitlab-org/gitaly/v16/internal/log"
	"gitlab.com/gitlab-org/gitaly/v16/internal/safe"
	"gitlab.com/gitlab-org/gitaly/v16/internal/structerr"
)

// ErrPartitionManagerClosed is returned when the PartitionManager stops processing transactions.
var ErrPartitionManagerClosed = errors.New("partition manager closed")

// Partition is the interface of a Partition as used by PartitionManager.
type Partition interface {
	Begin(context.Context, storage.BeginOptions) (storage.Transaction, error)
	Run() error
	Close()
	IsClosing() bool
}

// PartitionFactory is factory type that can create new partitions.
type PartitionFactory interface {
	// New returns a new Partition instance.
	New(
		logger log.Logger,
		partitionID storage.PartitionID,
		db keyvalue.Transactioner,
		storageName string,
		storagePath string,
		absoluteStateDir string,
		stagingDir string,
		metrics TransactionManagerMetrics,
		logConsumer LogConsumer,
	) Partition
}

type partitionFactory struct {
	cmdFactory  gitcmd.CommandFactory
	repoFactory localrepo.Factory
}

func (f partitionFactory) New(
	logger log.Logger,
	partitionID storage.PartitionID,
	db keyvalue.Transactioner,
	storageName string,
	storagePath string,
	absoluteStateDir string,
	stagingDir string,
	metrics TransactionManagerMetrics,
	logConsumer LogConsumer,
) Partition {
	// ScopeByStorage takes in context to pass it to the locator. This may be useful in the
	// RPC handlers to rewrite the storage in the future but never here. Requiring a context
	// here is more of a structural issue in the code, and is not useful.
	repoFactory, err := f.repoFactory.ScopeByStorage(context.Background(), storageName)
	if err != nil {
		// ScopeByStorage will only error if accessing a non existent storage. This can't
		// be the case when Factory is used as the storage is already verified.
		// This is a layering issue in the code, and not a realistic error scenario. We
		// thus panic out rather than make the error part of the interface.
		panic(fmt.Errorf("building a partition for a non-existent storage: %q", storageName))
	}

	return NewTransactionManager(
		partitionID,
		logger,
		db,
		storageName,
		storagePath,
		absoluteStateDir,
		stagingDir,
		f.cmdFactory,
		repoFactory,
		metrics,
		logConsumer,
	)
}

// NewPartitionFactory returns a new PartitionFactory.
func NewPartitionFactory(cmdFactory gitcmd.CommandFactory, repoFactory localrepo.Factory) PartitionFactory {
	return partitionFactory{
		cmdFactory:  cmdFactory,
		repoFactory: repoFactory,
	}
}

// PartitionManager is responsible for managing the lifecycle of each TransactionManager.
type PartitionManager struct {
	// storages are the storages configured in this Gitaly server. The map is keyed by the storage name.
	storages map[string]*storageManager
	// partitionFactory is a factory to create Partitions. This shouldn't ever be changed
	// during normal operation, but can be used to adjust the partition's behaviour in tests.
	partitionFactory PartitionFactory
	// consumer consumes the WAL from the partitions.
	consumer LogConsumer
	// consumerCleanup closes the LogConsumer.
	consumerCleanup func()
	// metrics accounts for all metrics of transaction operations. It will be
	// passed down to each partition and is shared between them. The
	// metrics must be registered to be collected by prometheus collector.
	metrics *metrics
}

type storageManagerMetrics struct {
	partitionsStarted prometheus.Counter
	partitionsStopped prometheus.Counter
}

// storageManager represents a single storage.
type storageManager struct {
	// mu synchronizes access to the fields of storageManager.
	mu sync.Mutex
	// logger handles all logging for storageManager.
	logger log.Logger
	// name is the name of the storage.
	name string
	// path is the absolute path to the storage's root.
	path string
	// stagingDirectory is the directory where all of the partition staging directories
	// should be created.
	stagingDirectory string
	// closed tracks whether the storageManager has been closed. If it is closed,
	// no new transactions are allowed to begin.
	closed bool
	// db is the handle to the key-value store used for storing the storage's database state.
	database keyvalue.Store
	// partitionAssigner manages partition assignments of repositories.
	partitionAssigner *partitionAssigner
	// partitions contains all the active partitions. Each repository can have up to one partition.
	partitions map[storage.PartitionID]*partition
	// activePartitions keeps track of active partitions.
	activePartitions sync.WaitGroup

	// metrics are the metrics gathered from the storage manager.
	metrics storageManagerMetrics
}

func (sm *storageManager) close() {
	sm.mu.Lock()
	// Mark the storage as closed so no new transactions can begin anymore. This
	// also means no more partitions are spawned.
	sm.closed = true
	for _, ptn := range sm.partitions {
		// Close all partitions.
		ptn.close()
	}
	sm.mu.Unlock()

	// Wait for all partitions to finish.
	sm.activePartitions.Wait()

	if err := sm.partitionAssigner.Close(); err != nil {
		sm.logger.WithError(err).Error("failed closing partition assigner")
	}
}

// finalizeTransaction decrements the partition's pending transaction count and closes it if there are no more
// transactions pending.
func (sm *storageManager) finalizeTransaction(ptn *partition) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	ptn.pendingTransactionCount--
	if ptn.pendingTransactionCount == 0 {
		ptn.close()
	}
}

// finalizableTransaction wraps a transaction to track the number of in-flight transactions for a Partition.
type finalizableTransaction struct {
	// finalize is called when the transaction is either committed or rolled back.
	finalize func()
	// Transaction is the underlying transaction.
	storage.Transaction
}

// Commit commits the transaction and runs the finalizer.
func (tx *finalizableTransaction) Commit(ctx context.Context) error {
	defer tx.finalize()
	return tx.Transaction.Commit(ctx)
}

// Rollback rolls back the transaction and runs the finalizer.
func (tx *finalizableTransaction) Rollback() error {
	defer tx.finalize()
	return tx.Transaction.Rollback()
}

// newFinalizableTransaction returns a wrapped transaction that executes finalizeTransaction when the transaction
// is committed or rolled back.
func (sm *storageManager) newFinalizableTransaction(ptn *partition, tx storage.Transaction) *finalizableTransaction {
	var finalizeOnce sync.Once
	return &finalizableTransaction{
		finalize: func() {
			finalizeOnce.Do(func() {
				sm.finalizeTransaction(ptn)
			})
		},
		Transaction: tx,
	}
}

// partition contains the transaction manager and tracks the number of in-flight transactions for the partition.
type partition struct {
	// closing is closed when the partition has no longer any active transactions.
	closing chan struct{}
	// closed is closed when the partitions goroutine has finished.
	closed chan struct{}
	// partitionClosed is closed to signal when the partition.Run has returned.
	// Clients stumbling on the partition when it is closing wait on this channel to know when the previous
	// partition instance has closed and it is safe to start another one.
	partitionClosed chan struct{}
	// partition manages all transactions for the partition.
	partition Partition
	// pendingTransactionCount holds the current number of in flight transactions being processed by the manager.
	pendingTransactionCount uint
}

// close closes the partition's transaction manager.
func (ptn *partition) close() {
	// The partition may be closed either due to PartitionManager itself being closed,
	// or due it having no more active transactions. Both of these can happen, in which
	// case both of them would attempt to close the channel. Check first whether the
	// channel has already been closed.
	if ptn.isClosing() {
		return
	}

	close(ptn.closing)
	ptn.partition.Close()
}

// isClosing returns whether partition is closing.
func (ptn *partition) isClosing() bool {
	select {
	case <-ptn.closing:
		return true
	default:
		return false
	}
}

func clearStagingDirectory(stagingDir string) error {
	// Shared snapshots don't have write permissions on them to prevent accidental writes
	// into them. If Gitaly terminated uncleanly and didn't clean up all of the shared snapshots,
	// their directories would still be missing the write permission and fail the below
	// RemoveAll call.
	//
	// Restore the write permission in the staging directory so read-only shared snapshots don't
	// fail the deletion. The staging directory may also not exist so ignore the error.
	if err := storage.SetDirectoryMode(stagingDir, mode.Directory); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("set directory mode: %w", err)
	}

	if err := os.RemoveAll(stagingDir); err != nil {
		return fmt.Errorf("remove all: %w", err)
	}

	return nil
}

// NewPartitionManager returns a new PartitionManager.
func NewPartitionManager(
	ctx context.Context,
	configuredStorages []config.Storage,
	logger log.Logger,
	dbMgr *databasemgr.DBManager,
	promCfg gitalycfgprom.Config,
	partitionFactory PartitionFactory,
	consumerFactory LogConsumerFactory,
) (*PartitionManager, error) {
	metrics := newMetrics(promCfg)

	storages := make(map[string]*storageManager, len(configuredStorages))
	for _, configuredStorage := range configuredStorages {
		internalDir := internalDirectoryPath(configuredStorage.Path)
		stagingDir := stagingDirectoryPath(internalDir)
		// Remove a possible already existing staging directory as it may contain stale files
		// if the previous process didn't shutdown gracefully.
		if err := clearStagingDirectory(stagingDir); err != nil {
			return nil, fmt.Errorf("failed clearing storage's staging directory: %w", err)
		}

		if err := os.MkdirAll(stagingDir, mode.Directory); err != nil {
			return nil, fmt.Errorf("create storage's staging directory: %w", err)
		}

		storageLogger := logger.WithField("storage", configuredStorage.Name)
		db, err := dbMgr.GetDB(configuredStorage.Name)
		if err != nil {
			return nil, err
		}

		pa, err := newPartitionAssigner(db, configuredStorage.Path)
		if err != nil {
			return nil, fmt.Errorf("new partition assigner: %w", err)
		}

		storages[configuredStorage.Name] = &storageManager{
			logger:            storageLogger,
			name:              configuredStorage.Name,
			path:              configuredStorage.Path,
			stagingDirectory:  stagingDir,
			database:          db,
			partitionAssigner: pa,
			partitions:        map[storage.PartitionID]*partition{},
			metrics:           metrics.storageManagerMetrics(configuredStorage.Name),
		}
	}

	pm := &PartitionManager{
		storages:         storages,
		metrics:          metrics,
		partitionFactory: partitionFactory,
	}

	if consumerFactory != nil {
		pm.consumer, pm.consumerCleanup = consumerFactory(pm)
	}

	return pm, nil
}

func keyPrefixPartition(ptnID storage.PartitionID) []byte {
	return []byte(fmt.Sprintf("p/%s/", ptnID.MarshalBinary()))
}

// internalDirectoryPath returns the full path of Gitaly's internal data directory for the storage.
func internalDirectoryPath(storagePath string) string {
	return filepath.Join(storagePath, config.GitalyDataPrefix)
}

func stagingDirectoryPath(storagePath string) string {
	return filepath.Join(storagePath, "staging")
}

// TransactionOptions are used to pass transaction options into Begin.
type TransactionOptions struct {
	// ReadOnly indicates whether this is a read-only transaction. Read-only transactions are not
	// configured with a quarantine directory and do not commit a log entry.
	ReadOnly bool
	// RelativePath specifies which repository in the partition will be the target.
	RelativePath string
	// AlternateRelativePath specifies a repository to include in the transaction's snapshot as well.
	AlternateRelativePath string
	// AllowPartitionAssignmentWithoutRepository determines whether a partition assignment should be
	// written out even if repository does not exist.
	AllowPartitionAssignmentWithoutRepository bool
	// ForceExclusiveSnapshot forces the transactions to use an exclusive snapshot. This is a temporary
	// workaround for some RPCs that do not work well with shared read-only snapshots yet.
	ForceExclusiveSnapshot bool
	// KVOnly is an option that starts only a key-value transaction against the partition when no relative
	// path is provided.
	KVOnly bool
}

// Begin gets the Partition for the specified repository and starts a transaction. If a
// Partition is not already running, a new one is created and used. The partition tracks
// the number of pending transactions and this counter gets incremented when Begin is invoked.
//
// Specifying storageName and partitionID will begin a transaction targeting an
// entire partition. If the partitionID is zero, then the partition is detected
// from opts.RelativePath.
func (pm *PartitionManager) Begin(ctx context.Context, storageName string, partitionID storage.PartitionID, opts TransactionOptions) (*finalizableTransaction, error) {
	storageMgr, ok := pm.storages[storageName]
	if !ok {
		return nil, structerr.NewNotFound("unknown storage: %q", storageName)
	}

	var relativePaths []string

	if opts.KVOnly {
		// Don't snapshot any repositories and only start a KV transaction.
		relativePaths = []string{}
	} else if opts.RelativePath != "" {
		var err error
		opts.RelativePath, err = storage.ValidateRelativePath(storageMgr.path, opts.RelativePath)
		if err != nil {
			return nil, structerr.NewInvalidArgument("validate relative path: %w", err)
		}

		repoPartitionID, err := storageMgr.partitionAssigner.getPartitionID(ctx, opts.RelativePath, opts.AlternateRelativePath, opts.AllowPartitionAssignmentWithoutRepository)
		if err != nil {
			if errors.Is(err, badger.ErrDBClosed) {
				// The database is closed when PartitionManager is closing. Return a more
				// descriptive error of what happened.
				return nil, ErrPartitionManagerClosed
			}

			return nil, fmt.Errorf("get partition: %w", err)
		}

		if partitionID != invalidPartitionID && repoPartitionID != partitionID {
			return nil, errors.New("partition ID does not match repository partition")
		}

		relativePaths = []string{opts.RelativePath}
		partitionID = repoPartitionID
	}

	relativeStateDir := deriveStateDirectory(partitionID)
	absoluteStateDir := filepath.Join(storageMgr.path, relativeStateDir)
	if err := os.MkdirAll(filepath.Dir(absoluteStateDir), mode.Directory); err != nil {
		return nil, fmt.Errorf("create state directory hierarchy: %w", err)
	}

	if err := safe.NewSyncer().SyncHierarchy(storageMgr.path, filepath.Dir(relativeStateDir)); err != nil {
		return nil, fmt.Errorf("sync state directory hierarchy: %w", err)
	}

	ptn, err := pm.startPartition(ctx, storageMgr, partitionID)
	if err != nil {
		return nil, err
	}

	if opts.AlternateRelativePath != "" {
		relativePaths = append(relativePaths, opts.AlternateRelativePath)
	}

	transaction, err := ptn.partition.Begin(ctx, storage.BeginOptions{
		Write:                  !opts.ReadOnly,
		RelativePaths:          relativePaths,
		ForceExclusiveSnapshot: opts.ForceExclusiveSnapshot,
	})
	if err != nil {
		// The pending transaction count needs to be decremented since the transaction is no longer
		// inflight. A transaction failing does not necessarily mean the transaction manager has
		// stopped running. Consequently, if there are no other pending transactions the partition
		// should be closed.
		storageMgr.finalizeTransaction(ptn)

		return nil, err
	}

	return storageMgr.newFinalizableTransaction(ptn, transaction), nil
}

// CallLogManager executes the provided function against the Partition for the specified partition, starting it if necessary.
func (pm *PartitionManager) CallLogManager(ctx context.Context, storageName string, partitionID storage.PartitionID, fn func(lm LogManager)) error {
	storageMgr, ok := pm.storages[storageName]
	if !ok {
		return structerr.NewNotFound("unknown storage: %q", storageName)
	}

	ptn, err := pm.startPartition(ctx, storageMgr, partitionID)
	if err != nil {
		return err
	}

	defer storageMgr.finalizeTransaction(ptn)

	logManager, ok := ptn.partition.(LogManager)
	if !ok {
		return fmt.Errorf("expected LogManager, got %T", logManager)
	}

	fn(logManager)

	return nil
}

// startPartition starts the TransactionManager for a partition.
func (pm *PartitionManager) startPartition(ctx context.Context, storageMgr *storageManager, partitionID storage.PartitionID) (*partition, error) {
	relativeStateDir := deriveStateDirectory(partitionID)
	absoluteStateDir := filepath.Join(storageMgr.path, relativeStateDir)
	if err := os.MkdirAll(filepath.Dir(absoluteStateDir), mode.Directory); err != nil {
		return nil, fmt.Errorf("create state directory hierarchy: %w", err)
	}

	if err := safe.NewSyncer().SyncHierarchy(storageMgr.path, filepath.Dir(relativeStateDir)); err != nil {
		return nil, fmt.Errorf("sync state directory hierarchy: %w", err)
	}

	for {
		storageMgr.mu.Lock()
		if storageMgr.closed {
			storageMgr.mu.Unlock()
			return nil, ErrPartitionManagerClosed
		}

		ptn, ok := storageMgr.partitions[partitionID]
		if !ok {
			ptn = &partition{
				closing:         make(chan struct{}),
				closed:          make(chan struct{}),
				partitionClosed: make(chan struct{}),
			}

			stagingDir, err := os.MkdirTemp(storageMgr.stagingDirectory, "")
			if err != nil {
				storageMgr.mu.Unlock()
				return nil, fmt.Errorf("create staging directory: %w", err)
			}

			logger := storageMgr.logger.WithField("partition_id", partitionID)

			mgr := pm.partitionFactory.New(
				logger,
				partitionID,
				keyvalue.NewPrefixedTransactioner(storageMgr.database, keyPrefixPartition(partitionID)),
				storageMgr.name,
				storageMgr.path,
				absoluteStateDir,
				stagingDir,
				NewTransactionManagerMetrics(
					pm.metrics.housekeeping,
					pm.metrics.snapshot.Scope(storageMgr.name),
				),
				pm.consumer,
			)

			ptn.partition = mgr

			storageMgr.partitions[partitionID] = ptn

			storageMgr.metrics.partitionsStarted.Inc()
			storageMgr.activePartitions.Add(1)
			go func() {
				if err := mgr.Run(); err != nil {
					logger.WithError(err).WithField("partition_state_directory", relativeStateDir).Error("partition failed")
				}

				// In the event that Partition stops running, a new Partition instance will
				// need to be started in order to continue processing transactions. The partition instance
				// is deleted allowing the next transaction for the repository to create a new partition
				// instance.
				storageMgr.mu.Lock()
				delete(storageMgr.partitions, partitionID)
				storageMgr.mu.Unlock()

				close(ptn.partitionClosed)

				// If the Partition returned due to an error, it could be that there are still
				// in-flight transactions operating on their staged state. Removing the staging directory
				// while they are active can lead to unexpected errors. Wait with the removal until they've
				// all finished, and only then remove the staging directory.
				//
				// All transactions must eventually finish, so we don't wait on a context cancellation here.
				<-ptn.closing

				if err := os.RemoveAll(stagingDir); err != nil {
					logger.WithError(err).Error("failed removing partition's staging directory")
				}

				storageMgr.metrics.partitionsStopped.Inc()
				close(ptn.closed)
				storageMgr.activePartitions.Done()
			}()
		}

		if ptn.isClosing() {
			// If the partition is in the process of shutting down, the partition should not be
			// used. The lock is released while waiting for the partition to complete closing as to
			// not block other partitions from processing transactions. Once closing is complete, a
			// new attempt is made to get a valid partition.
			storageMgr.mu.Unlock()
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-ptn.partitionClosed:
			}

			continue
		}

		ptn.pendingTransactionCount++
		storageMgr.mu.Unlock()

		return ptn, nil
	}
}

// deriveStateDirectory hashes the partition ID and returns the state
// directory where state related to the partition should be stored.
func deriveStateDirectory(id storage.PartitionID) string {
	hasher := sha256.New()
	hasher.Write([]byte(id.String()))
	hash := hex.EncodeToString(hasher.Sum(nil))

	return filepath.Join(
		config.GitalyDataPrefix,
		"partitions",
		// These two levels balance the state directories into smaller
		// subdirectories to keep the directory sizes reasonable.
		hash[0:2],
		hash[2:4],
		id.String(),
	)
}

// Close closes transaction processing for all storages and waits for closing completion.
func (pm *PartitionManager) Close() {
	var activeStorages sync.WaitGroup
	for _, storageMgr := range pm.storages {
		activeStorages.Add(1)
		storageMgr := storageMgr
		go func() {
			storageMgr.close()
			activeStorages.Done()
		}()
	}

	if pm.consumerCleanup != nil {
		pm.consumerCleanup()
	}

	activeStorages.Wait()
}

// Describe is used to describe Prometheus metrics.
func (pm *PartitionManager) Describe(metrics chan<- *prometheus.Desc) {
	prometheus.DescribeByCollect(pm, metrics)
}

// Collect is used to collect Prometheus metrics.
func (pm *PartitionManager) Collect(metrics chan<- prometheus.Metric) {
	pm.metrics.Collect(metrics)
}
