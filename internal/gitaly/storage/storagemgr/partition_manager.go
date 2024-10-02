package storagemgr

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sync"

	"github.com/dgraph-io/badger/v4"
	"github.com/prometheus/client_golang/prometheus"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/config"
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

// Partition extends the typical Partition interface with methods needed by PartitionManager.
type Partition interface {
	storage.Partition
	Run() error
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
	) Partition
}

type storageManagerMetrics struct {
	partitionsStarted prometheus.Counter
	partitionsStopped prometheus.Counter
}

// StorageManager represents a single storage.
type StorageManager struct {
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
	// partitionFactory is a factory to create Partitions.
	partitionFactory PartitionFactory

	// metrics are the metrics gathered from the storage manager.
	metrics storageManagerMetrics
}

// NewStorageManager instantiates a new StorageManager.
func NewStorageManager(
	logger log.Logger,
	name string,
	path string,
	dbMgr *databasemgr.DBManager,
	partitionFactory PartitionFactory,
	metrics *Metrics,
) (*StorageManager, error) {
	internalDir := internalDirectoryPath(path)
	stagingDir := stagingDirectoryPath(internalDir)
	// Remove a possible already existing staging directory as it may contain stale files
	// if the previous process didn't shutdown gracefully.
	if err := clearStagingDirectory(stagingDir); err != nil {
		return nil, fmt.Errorf("failed clearing storage's staging directory: %w", err)
	}

	if err := os.MkdirAll(stagingDir, mode.Directory); err != nil {
		return nil, fmt.Errorf("create storage's staging directory: %w", err)
	}

	storageLogger := logger.WithField("storage", name)
	db, err := dbMgr.GetDB(name)
	if err != nil {
		return nil, err
	}

	pa, err := newPartitionAssigner(db, path)
	if err != nil {
		return nil, fmt.Errorf("new partition assigner: %w", err)
	}

	return &StorageManager{
		logger:            storageLogger,
		name:              name,
		path:              path,
		stagingDirectory:  stagingDir,
		database:          db,
		partitionAssigner: pa,
		partitions:        map[storage.PartitionID]*partition{},
		partitionFactory:  partitionFactory,
		metrics:           metrics.storageManagerMetrics(name),
	}, nil
}

// Close closes the manager for further access and waits for all partitions to stop.
func (sm *StorageManager) Close() {
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
func (tx *finalizableTransaction) Rollback(ctx context.Context) error {
	defer tx.finalize()
	return tx.Transaction.Rollback(ctx)
}

// newFinalizableTransaction returns a wrapped transaction that executes finalize when the transaction
// is committed or rollbacked.
func newFinalizableTransaction(tx storage.Transaction, finalize func()) *finalizableTransaction {
	return &finalizableTransaction{
		finalize:    finalize,
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
	// referenceCount holds the current number of references held to the partition.
	referenceCount uint
	// Partition is the wrapped partition handle.
	Partition
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
	ptn.Close()
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

func keyPrefixPartition(ptnID storage.PartitionID) []byte {
	return []byte(fmt.Sprintf("%s%s/", prefixPartition, ptnID.MarshalBinary()))
}

// internalDirectoryPath returns the full path of Gitaly's internal data directory for the storage.
func internalDirectoryPath(storagePath string) string {
	return filepath.Join(storagePath, config.GitalyDataPrefix)
}

func stagingDirectoryPath(storagePath string) string {
	return filepath.Join(storagePath, "staging")
}

// Begin gets the Partition for the specified repository and starts a transaction. If a
// Partition is not already running, a new one is created and used. The partition tracks
// the number of pending transactions and this counter gets incremented when Begin is invoked.
func (sm *StorageManager) Begin(ctx context.Context, opts storage.TransactionOptions) (_ storage.Transaction, returnedErr error) {
	if opts.RelativePath == "" {
		return nil, fmt.Errorf("target relative path unset")
	}

	relativePath, err := storage.ValidateRelativePath(sm.path, opts.RelativePath)
	if err != nil {
		return nil, structerr.NewInvalidArgument("validate relative path: %w", err)
	}

	partitionID, err := sm.partitionAssigner.getPartitionID(ctx, relativePath, opts.AlternateRelativePath, opts.AllowPartitionAssignmentWithoutRepository)
	if err != nil {
		if errors.Is(err, badger.ErrDBClosed) {
			// The database is closed when PartitionManager is closing. Return a more
			// descriptive error of what happened.
			return nil, ErrPartitionManagerClosed
		}

		return nil, fmt.Errorf("get partition: %w", err)
	}

	ptn, err := sm.startPartition(ctx, partitionID)
	if err != nil {
		return nil, err
	}

	defer func() {
		if returnedErr != nil {
			// Close the partition handle on error as the caller wouldn't do so anymore by
			// committing/rollbacking the transaction.
			ptn.Close()
		}
	}()

	relativePaths := []string{relativePath}
	if opts.AlternateRelativePath != "" {
		relativePaths = append(relativePaths, opts.AlternateRelativePath)
	}

	transaction, err := ptn.Begin(ctx, storage.BeginOptions{
		Write:                  !opts.ReadOnly,
		RelativePaths:          relativePaths,
		ForceExclusiveSnapshot: opts.ForceExclusiveSnapshot,
	})
	if err != nil {
		return nil, err
	}

	return newFinalizableTransaction(transaction, ptn.Close), nil
}

// partitionHandle is a handle to a partition. It wraps the close method of a partition with reference
// counting and only closes the partition if there are no other remaining references to it.
type partitionHandle struct {
	*partition
	sm   *StorageManager
	once sync.Once
}

// newPartitionHandle creates a new handle to the partition. `sm.mu.Lock()` must be held while calling this.
func newPartitionHandle(sm *StorageManager, ptn *partition) *partitionHandle {
	ptn.referenceCount++
	return &partitionHandle{sm: sm, partition: ptn}
}

// Close decrements the partition's reference count and closes it if there are no more references to it.
func (p *partitionHandle) Close() {
	p.once.Do(func() {
		p.sm.mu.Lock()
		defer p.sm.mu.Unlock()

		p.partition.referenceCount--
		if p.partition.referenceCount == 0 {
			p.partition.close()
		}
	})
}

// GetPartition returns a new handle to a partition.
func (sm *StorageManager) GetPartition(ctx context.Context, partitionID storage.PartitionID) (storage.Partition, error) {
	return sm.startPartition(ctx, partitionID)
}

// startPartition starts a partition.
func (sm *StorageManager) startPartition(ctx context.Context, partitionID storage.PartitionID) (*partitionHandle, error) {
	relativeStateDir := deriveStateDirectory(partitionID)
	absoluteStateDir := filepath.Join(sm.path, relativeStateDir)
	if err := os.MkdirAll(filepath.Dir(absoluteStateDir), mode.Directory); err != nil {
		return nil, fmt.Errorf("create state directory hierarchy: %w", err)
	}

	if err := safe.NewSyncer().SyncHierarchy(sm.path, filepath.Dir(relativeStateDir)); err != nil {
		return nil, fmt.Errorf("sync state directory hierarchy: %w", err)
	}

	for {
		sm.mu.Lock()
		if sm.closed {
			sm.mu.Unlock()
			return nil, ErrPartitionManagerClosed
		}

		ptn, ok := sm.partitions[partitionID]
		if !ok {
			ptn = &partition{
				closing:         make(chan struct{}),
				closed:          make(chan struct{}),
				partitionClosed: make(chan struct{}),
			}

			stagingDir, err := os.MkdirTemp(sm.stagingDirectory, "")
			if err != nil {
				sm.mu.Unlock()
				return nil, fmt.Errorf("create staging directory: %w", err)
			}

			logger := sm.logger.WithField("partition_id", partitionID)

			mgr := sm.partitionFactory.New(
				logger,
				partitionID,
				keyvalue.NewPrefixedTransactioner(sm.database, keyPrefixPartition(partitionID)),
				sm.name,
				sm.path,
				absoluteStateDir,
				stagingDir,
			)

			ptn.Partition = mgr

			sm.partitions[partitionID] = ptn

			sm.metrics.partitionsStarted.Inc()
			sm.activePartitions.Add(1)
			go func() {
				if err := mgr.Run(); err != nil {
					logger.WithError(err).WithField("partition_state_directory", relativeStateDir).Error("partition failed")
				}

				// In the event that Partition stops running, a new Partition instance will
				// need to be started in order to continue processing transactions. The partition instance
				// is deleted allowing the next transaction for the repository to create a new partition
				// instance.
				sm.mu.Lock()
				delete(sm.partitions, partitionID)
				sm.mu.Unlock()

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

				sm.metrics.partitionsStopped.Inc()
				close(ptn.closed)
				sm.activePartitions.Done()
			}()
		}

		if ptn.isClosing() {
			// If the partition is in the process of shutting down, the partition should not be
			// used. The lock is released while waiting for the partition to complete closing as to
			// not block other partitions from processing transactions. Once closing is complete, a
			// new attempt is made to get a valid partition.
			sm.mu.Unlock()
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-ptn.partitionClosed:
			}

			continue
		}

		defer sm.mu.Unlock()

		return newPartitionHandle(sm, ptn), nil
	}
}

// GetAssignedPartitionID returns the ID of the partition the relative path has been assigned to.
func (sm *StorageManager) GetAssignedPartitionID(relativePath string) (storage.PartitionID, error) {
	return sm.partitionAssigner.partitionAssignmentTable.getPartitionID(relativePath)
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

// ListPartitions returns a partition iterator for listing the partitions.
func (sm *StorageManager) ListPartitions(partitionID storage.PartitionID) (storage.PartitionIterator, error) {
	txn := sm.database.NewTransaction(false)
	iter := txn.NewIterator(keyvalue.IteratorOptions{
		Prefix: []byte(prefixPartition),
	})

	pi := &partitionIterator{
		txn:  txn,
		it:   iter,
		seek: keyPrefixPartition(partitionID),
	}

	return pi, nil
}

type partitionIterator struct {
	txn     keyvalue.Transaction
	it      keyvalue.Iterator
	current storage.PartitionID
	seek    []byte
	err     error
}

// Next advances the iterator to the next valid partition ID.
// It returns true if a new, valid partition ID was found, and false otherwise.
// The method ensures that:
//  1. If a seek value is set, it seeks to that position first.
//  2. It skips over any duplicate or lesser partition IDs.
//  3. It stops when it finds a partition ID greater than the last one,
//     or when it reaches the end of the iterator.
//
// If an error occurs during extraction of the partition ID, it returns false
// and the error can be retrieved using the Err() method.
func (pi *partitionIterator) Next() bool {
	if pi.seek != nil {
		pi.it.Seek(pi.seek)
		pi.seek = nil
	} else {
		pi.it.Next()
	}

	for ; pi.it.Valid(); pi.it.Next() {
		last := pi.current

		pi.current, pi.err = pi.extractPartitionID()
		if pi.err != nil {
			return false
		}

		if pi.current > last {
			return true
		}
	}

	return false
}

// GetPartitionID returns the current partition ID of the iterator.
func (pi *partitionIterator) GetPartitionID() storage.PartitionID {
	return pi.current
}

// Err returns the error of the iterator.
func (pi *partitionIterator) Err() error {
	return pi.err
}

// Close closes the iterator and discards the underlying transaction
func (pi *partitionIterator) Close() {
	pi.it.Close()
	pi.txn.Discard()
}

// extractPartitionID returns the partition ID by extracting it from the key.
// If key structure is different than expected, it returns error.
func (pi *partitionIterator) extractPartitionID() (storage.PartitionID, error) {
	var partitionID storage.PartitionID

	key := pi.it.Item().Key()
	unprefixedKey, hasPrefix := bytes.CutPrefix(key, []byte(prefixPartition))
	if !hasPrefix || len(unprefixedKey) < binary.Size(partitionID) {
		return invalidPartitionID, fmt.Errorf("invalid partition key format: %q", key)
	}

	partitionID.UnmarshalBinary(unprefixedKey[:binary.Size(partitionID)])

	return partitionID, nil
}
