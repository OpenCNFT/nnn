package partition

import (
	"bufio"
	"bytes"
	"container/list"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"runtime/trace"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/dgraph-io/badger/v4"
	"github.com/prometheus/client_golang/prometheus"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/gitcmd"
	housekeepingcfg "gitlab.com/gitlab-org/gitaly/v16/internal/git/housekeeping/config"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/stats"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/updateref"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/repoutil"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/gitstorage"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/keyvalue"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/mode"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/storagemgr/partition/conflict"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/storagemgr/partition/fsrecorder"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/storagemgr/snapshot"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/wal"
	"gitlab.com/gitlab-org/gitaly/v16/internal/log"
	"gitlab.com/gitlab-org/gitaly/v16/internal/safe"
	"gitlab.com/gitlab-org/gitaly/v16/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v16/internal/tracing"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
	"gitlab.com/gitlab-org/labkit/correlation"
	"golang.org/x/sync/errgroup"
	"google.golang.org/protobuf/proto"
)

var (
	// errConcurrentPackedRefsWrite is returned when a transaction conflicts due to a concurrent
	// write into packed-refs file.
	errConcurrentPackedRefsWrite = structerr.NewAborted("concurrent packed-refs write")
	// errConcurrentReferencePacking is returned when attempting to commit reference deletions but
	// references have been concurrently packed.
	errConcurrentReferencePacking = structerr.NewAborted("references packed concurrently")
	// ErrRepositoryAlreadyExists is attempting to create a repository that already exists.
	ErrRepositoryAlreadyExists = structerr.NewAlreadyExists("repository already exists")
	// errInitializationFailed is returned when the TransactionManager failed to initialize successfully.
	errInitializationFailed = errors.New("initializing transaction processing failed")
	// errCommittedEntryGone is returned when the log entry of a LSN is gone from database while it's still
	// accessed by other transactions.
	errCommittedEntryGone = errors.New("in-used committed entry is gone")
	// errNotDirectory is returned when the repository's path doesn't point to a directory
	errNotDirectory = errors.New("repository's path didn't point to a directory")
	// errRelativePathNotSet is returned when a transaction is begun without providing a relative path
	// of the target repository.
	errRelativePathNotSet = errors.New("relative path not set")
	// errAlternateAlreadyLinked is returned when attempting to set an alternate on a repository that
	// already has one.
	errAlternateAlreadyLinked = errors.New("repository already has an alternate")
	// errConflictRepositoryDeletion is returned when an operation conflicts with repository deletion in another
	// transaction.
	errConflictRepositoryDeletion = errors.New("detected an update conflicting with repository deletion")
	// errPackRefsConflictRefDeletion is returned when there is a committed ref deletion before pack-refs
	// task is committed. The transaction should be aborted.
	errPackRefsConflictRefDeletion = errors.New("detected a conflict with reference deletion when committing packed-refs")
	// errHousekeepingConflictOtherUpdates is returned when the transaction includes housekeeping alongside
	// with other updates.
	errHousekeepingConflictOtherUpdates = errors.New("housekeeping in the same transaction with other updates")
	// errHousekeepingConflictConcurrent is returned when there are another concurrent housekeeping task.
	errHousekeepingConflictConcurrent = errors.New("conflict with another concurrent housekeeping task")
	// errRepackConflictPrunedObject is returned when the repacking task pruned an object that is still used by other
	// concurrent transactions.
	errRepackConflictPrunedObject = errors.New("pruned object used by other updates")
	// errRepackNotSupportedStrategy is returned when the manager runs the repacking task using unsupported strategy.
	errRepackNotSupportedStrategy = errors.New("strategy not supported")
	// errConcurrentAlternateUnlink is a repack attempts to commit against a repository that was concurrenty unlinked
	// from an alternate
	errConcurrentAlternateUnlink = errors.New("concurrent alternate unlinking with repack")

	// Below errors are used to error out in cases when updates have been staged in a read-only transaction.
	errReadOnlyRepositoryDeletion = errors.New("repository deletion staged in a read-only transaction")
	errReadOnlyHousekeeping       = errors.New("housekeeping in a read-only transaction")
	errReadOnlyKeyValue           = errors.New("key-value writes in a read-only transaction")

	errRepositoryDeletionOtherOperations = errors.New("other operations staged with repository deletion")

	// errWritableAllRepository is returned when a transaction is started with
	// no relative path filter specified and is not read-only. Transactions do
	// not currently support writing to multiple repositories and so a writable
	// transaction without a specified target relative path would be ambiguous.
	errWritableAllRepository = errors.New("cannot start writable all repository transaction")

	// keyAppliedLSN is the database key storing a partition's last applied log entry's LSN.
	keyAppliedLSN = []byte("applied_lsn")
)

const relativePathKeyPrefix = "r/"

// relativePathKey generates the database key for storing relative paths in a partition.
func relativePathKey(relativePath string) []byte {
	return []byte(relativePathKeyPrefix + relativePath)
}

// InvalidReferenceFormatError is returned when a reference name was invalid.
type InvalidReferenceFormatError struct {
	// ReferenceName is the reference with invalid format.
	ReferenceName git.ReferenceName
}

// Error returns the formatted error string.
func (err InvalidReferenceFormatError) Error() string {
	return fmt.Sprintf("invalid reference format: %q", err.ReferenceName)
}

// newConflictingKeyValueOperationError returns an error that is raised when a transaction
// attempts to commit a key-value operation that conflicted with other concurrently committed transactions.
func newConflictingKeyValueOperationError(key string) error {
	return structerr.NewAborted("conflicting key-value operations").WithMetadata("key", key)
}

// repositoryCreation models a repository creation in a transaction.
type repositoryCreation struct {
	// objectHash defines the object format the repository is created with.
	objectHash git.ObjectHash
}

type transactionState int

const (
	// transactionStateOpen indicates the transaction is open, and hasn't been committed or rolled back yet.
	transactionStateOpen = transactionState(iota)
	// transactionStateRollback indicates the transaction has been rolled back.
	transactionStateRollback
	// transactionStateCommit indicates the transaction has already been committed.
	transactionStateCommit
)

// Transaction is a unit-of-work that contains reference changes to perform on the repository.
type Transaction struct {
	// write denotes whether or not this transaction is a write transaction.
	write bool
	// repositoryExists indicates whether the target repository existed when this transaction began.
	repositoryExists bool
	// metrics stores metric reporters inherited from the manager.
	metrics ManagerMetrics

	// state records whether the transaction is still open. Transaction is open until either Commit()
	// or Rollback() is called on it.
	state transactionState
	// stateLatch guards the transaction against concurrent commit and rollback operations. Transactions
	// are not generally safe for concurrent use. As the transaction may need to be committed in the
	// post-receive hook, there's potential for a race. If the RPC times out, it could be that the
	// PostReceiveHook RPC's goroutine attempts to commit a transaction at the same time as the parent
	// RPC's goroutine attempts to abort it. stateLatch guards against this race.
	stateLatch sync.Mutex

	// commit commits the Transaction through the TransactionManager.
	commit func(context.Context, *Transaction) error
	// result is where the outcome of the transaction is sent to by TransactionManager once it
	// has been determined.
	result chan error
	// admitted is set when the transaction was admitted for processing in the TransactionManager.
	// Transaction queues in admissionQueue to be committed, and is considered admitted once it has
	// been dequeued by TransactionManager.Run(). Once the transaction is admitted, its ownership moves
	// from the client goroutine to the TransactionManager.Run() goroutine, and the client goroutine must
	// not do any modifications to the state of the transaction anymore to avoid races.
	admitted bool
	// finish cleans up the transaction releasing the resources associated with it. It must be called
	// once the transaction is done with.
	finish func(admitted bool) error
	// finished is closed when the transaction has been finished. This enables waiting on transactions
	// to finish where needed.
	finished chan struct{}

	// relativePath is the relative path of the repository this transaction is targeting.
	relativePath string
	// stagingDirectory is the directory where the transaction stages its files prior
	// to them being logged. It is cleaned up when the transaction finishes.
	stagingDirectory string
	// quarantineDirectory is the directory within the stagingDirectory where the new objects of the
	// transaction are quarantined.
	quarantineDirectory string
	// packPrefix contains the prefix (`pack-<digest>`) of the transaction's pack if the transaction
	// had objects to log.
	packPrefix string
	// snapshotRepository is a snapshot of the target repository with a possible quarantine applied
	// if this is a read-write transaction.
	snapshotRepository *localrepo.Repo

	// snapshotLSN is the log sequence number which this transaction is reading the repository's
	// state at.
	snapshotLSN storage.LSN
	// snapshot is the transaction's snapshot of the partition file system state. It's used to rewrite
	// relative paths to point to the snapshot instead of the actual repositories.
	snapshot snapshot.FileSystem
	// db is the transaction's snapshot of the partition's key-value state. The keyvalue.Transaction is
	// discarded when the transaction finishes. The recorded writes are write-ahead logged and applied
	// to the partition from the WAL.
	db keyvalue.Transaction
	// recordingReadWriter is a ReadWriter operating on db that also records operations performed. This
	// is used to record the operations performed so they can be conflict checked and write-ahead logged.
	recordingReadWriter keyvalue.RecordingReadWriter
	// stagingRepository is a repository that is used to stage the transaction. If there are quarantined
	// objects, it has the quarantine applied so the objects are available for verification and packing.
	// Generally the staging repository is the actual repository instance. If the repository doesn't exist
	// yet, the staging repository is a temporary repository that is deleted once the transaction has been
	// finished.
	stagingRepository *localrepo.Repo
	// stagingSnapshot is the snapshot used for staging the transaction, and where the staging repository
	// exists.
	stagingSnapshot snapshot.FileSystem

	// walEntry is the log entry where the transaction stages its state for committing.
	walEntry               *wal.Entry
	initialReferenceValues map[git.ReferenceName]git.Reference
	referenceUpdates       []git.ReferenceUpdates
	customHooksUpdated     bool
	repositoryCreation     *repositoryCreation
	deleteRepository       bool
	includedObjects        map[git.ObjectID]struct{}
	runHousekeeping        *runHousekeeping
	alternateUpdated       bool

	// objectDependencies are the object IDs this transaction depends on in
	// the repository. The dependencies are used to guard against invalid packs
	// being committed which don't contain all necessary objects. The write could
	// either be missing objects, or a concurrent prune could have removed the
	// dependencies.
	objectDependencies map[git.ObjectID]struct{}
}

// Begin opens a new transaction. The caller must call either Commit or Rollback to release
// the resources tied to the transaction. The returned Transaction is not safe for concurrent use.
//
// The returned Transaction's read snapshot includes all writes that were committed prior to the
// Begin call. Begin blocks until the committed writes have been applied to the repository.
func (mgr *TransactionManager) Begin(ctx context.Context, opts storage.BeginOptions) (_ storage.Transaction, returnedErr error) {
	defer trace.StartRegion(ctx, "begin").End()
	defer prometheus.NewTimer(mgr.metrics.beginDuration(opts.Write)).ObserveDuration()
	transactionDurationTimer := prometheus.NewTimer(mgr.metrics.transactionDuration(opts.Write))

	trace.Log(ctx, "correlation_id", correlation.ExtractFromContext(ctx))
	trace.Log(ctx, "storage_name", mgr.storageName)
	trace.Log(ctx, "partition_id", mgr.partitionID.String())
	trace.Log(ctx, "write", strconv.FormatBool(opts.Write))
	trace.Log(ctx, "relative_path_filter_set", strconv.FormatBool(opts.RelativePaths != nil))
	trace.Log(ctx, "relative_path_filter", strings.Join(opts.RelativePaths, ";"))
	trace.Log(ctx, "force_exclusive_snapshot", strconv.FormatBool(opts.ForceExclusiveSnapshot))

	// Wait until the manager has been initialized so the notification channels
	// and the LSNs are loaded.
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-mgr.initialized:
		if !mgr.initializationSuccessful {
			return nil, errInitializationFailed
		}
	}

	var relativePath string
	if len(opts.RelativePaths) > 0 {
		// Set the first repository as the tracked repository
		relativePath = opts.RelativePaths[0]
	}

	if opts.RelativePaths == nil && opts.Write {
		return nil, errWritableAllRepository
	}

	span, _ := tracing.StartSpanIfHasParent(ctx, "transaction.Begin", nil)
	span.SetTag("write", opts.Write)
	span.SetTag("relativePath", relativePath)
	defer span.Finish()

	mgr.mutex.Lock()

	txn := &Transaction{
		write:        opts.Write,
		commit:       mgr.commit,
		snapshotLSN:  mgr.appendedLSN,
		finished:     make(chan struct{}),
		relativePath: relativePath,
		metrics:      mgr.metrics,
	}

	mgr.snapshotLocks[txn.snapshotLSN].activeSnapshotters.Add(1)
	defer mgr.snapshotLocks[txn.snapshotLSN].activeSnapshotters.Done()
	readReady := mgr.snapshotLocks[txn.snapshotLSN].applied

	var entry *committedEntry
	if txn.write {
		entry = mgr.updateCommittedEntry(txn.snapshotLSN)
	}

	mgr.mutex.Unlock()

	span.SetTag("snapshotLSN", txn.snapshotLSN)

	txn.finish = func(admitted bool) error {
		defer trace.StartRegion(ctx, "finish transaction").End()
		defer close(txn.finished)
		defer transactionDurationTimer.ObserveDuration()

		defer func() {
			if txn.db != nil {
				txn.db.Discard()
			}

			if txn.write {
				var removedAnyEntry bool

				mgr.mutex.Lock()
				removedAnyEntry = mgr.cleanCommittedEntry(entry)
				mgr.mutex.Unlock()

				// Signal the manager this transaction finishes. The purpose of this signaling is to wake it up
				// and clean up stale entries in the database. The manager scans and removes leading empty
				// entries. We signal only if the transaction modifies the in-memory committed entry.
				// This signal queue is buffered. If the queue is full, the manager hasn't woken up. The
				// next scan will cover the work of the prior one. So, no need to let the transaction wait.
				// ┌─ 1st signal        ┌─ The manager scans til here
				// □ □ □ □ □ □ □ □ □ □ ■ ■ ⧅ ⧅ ⧅ ⧅ ⧅ ⧅ ■ ■ ⧅ ⧅ ⧅ ⧅ ■
				//        └─ 2nd signal
				if removedAnyEntry {
					select {
					case mgr.completedQueue <- struct{}{}:
					default:
					}
				}
			}
		}()

		cleanTemporaryState := func() error {
			defer trace.StartRegion(ctx, "cleanTemporaryState").End()

			var cleanupErr error
			if txn.snapshot != nil {
				if err := txn.snapshot.Close(); err != nil {
					cleanupErr = errors.Join(cleanupErr, fmt.Errorf("close snapshot: %w", err))
				}
			}

			if txn.stagingSnapshot != nil {
				if err := txn.stagingSnapshot.Close(); err != nil {
					cleanupErr = errors.Join(cleanupErr, fmt.Errorf("close staging snapshot: %w", err))
				}
			}

			if txn.stagingDirectory != "" {
				if err := os.RemoveAll(txn.stagingDirectory); err != nil {
					cleanupErr = errors.Join(cleanupErr, fmt.Errorf("remove staging directory: %w", err))
				}
			}

			return cleanupErr
		}

		if admitted {
			// If the transcation was admitted, `.Run()` is responsible for cleaning the transaction up.
			// Cleaning up the snapshots can take a relatively long time if the snapshots are large, or if
			// the file system is busy. To avoid blocking transaction processing, we us a pool of background
			// workers to clean up the transaction snapshots.
			//
			// The number of background workers is limited to exert backpressure on write transactions if
			// we can't clean up after them fast enough.
			mgr.cleanupWorkers.Go(func() error {
				if err := cleanTemporaryState(); err != nil {
					mgr.cleanupWorkerFailedOnce.Do(func() { close(mgr.cleanupWorkerFailed) })
					return fmt.Errorf("clean temporary state async: %w", err)
				}

				return nil
			})

			return nil
		}

		if err := cleanTemporaryState(); err != nil {
			return fmt.Errorf("clean temporary state sync: %w", err)
		}

		return nil
	}

	defer func() {
		if returnedErr != nil {
			if err := txn.finish(false); err != nil {
				mgr.logger.WithError(err).ErrorContext(ctx, "failed finishing unsuccessful transaction begin")
			}
		}
	}()

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-mgr.ctx.Done():
		return nil, storage.ErrTransactionProcessingStopped
	case <-readReady:
		txn.db = mgr.db.NewTransaction(txn.write)
		txn.recordingReadWriter = keyvalue.NewRecordingReadWriter(txn.db)

		relativePaths := opts.RelativePaths
		if relativePaths == nil {
			relativePaths = txn.PartitionRelativePaths()
		}

		var err error
		txn.stagingDirectory, err = os.MkdirTemp(mgr.stagingDirectory, "")
		if err != nil {
			return nil, fmt.Errorf("mkdir temp: %w", err)
		}

		if txn.snapshot, err = mgr.snapshotManager.GetSnapshot(ctx,
			relativePaths,
			txn.write || opts.ForceExclusiveSnapshot,
		); err != nil {
			return nil, fmt.Errorf("get snapshot: %w", err)
		}

		if txn.write {
			// Create a directory to store all staging files.
			if err := os.Mkdir(txn.walFilesPath(), mode.Directory); err != nil {
				return nil, fmt.Errorf("create wal files directory: %w", err)
			}

			txn.walEntry = wal.NewEntry(txn.walFilesPath())
		}

		if txn.repositoryTarget() {
			txn.repositoryExists, err = mgr.doesRepositoryExist(ctx, txn.snapshot.RelativePath(txn.relativePath))
			if err != nil {
				return nil, fmt.Errorf("does repository exist: %w", err)
			}

			txn.snapshotRepository = mgr.repositoryFactory.Build(txn.snapshot.RelativePath(txn.relativePath))
			if txn.write {
				if txn.repositoryExists {
					// Record the original packed-refs file. We'll use the inode number of the file in the conflict checks to determine
					// whether the transaction modified the packed-refs file. We record the file by hard linking it to prevent its inode from
					// being recycled should it be deleted while the transactions is running.
					if err := os.Link(
						mgr.getAbsolutePath(txn.snapshot.RelativePath(txn.relativePath), "packed-refs"),
						txn.originalPackedRefsFilePath(),
					); err != nil && !errors.Is(err, fs.ErrNotExist) {
						return nil, fmt.Errorf("record original packed-refs: %w", err)
					}

					txn.quarantineDirectory = filepath.Join(txn.stagingDirectory, "quarantine")
					if err := os.MkdirAll(filepath.Join(txn.quarantineDirectory, "pack"), mode.Directory); err != nil {
						return nil, fmt.Errorf("create quarantine directory: %w", err)
					}

					txn.snapshotRepository, err = txn.snapshotRepository.Quarantine(ctx, txn.quarantineDirectory)
					if err != nil {
						return nil, fmt.Errorf("quarantine: %w", err)
					}
				} else {
					// The repository does not exist, and this is a write. This should thus create the repository. As the repository's final state
					// is still being logged in TransactionManager, we already log here the creation of any missing parent directories of
					// the repository. When the transaction commits, we don't know if they existed or not, so we can't record this later.
					//
					// If the repository is at the root of the storage, there's no parent directories to create.
					if parentDir := filepath.Dir(txn.relativePath); parentDir != "." {
						if err := fsrecorder.NewFS(txn.snapshot.Root(), txn.walEntry).MkdirAll(parentDir); err != nil {
							return nil, fmt.Errorf("create parent directories: %w", err)
						}
					}

					txn.quarantineDirectory = filepath.Join(mgr.storagePath, txn.snapshot.RelativePath(txn.relativePath), "objects")
				}
			}
		}

		return txn, nil
	}
}

// repositoryTarget returns true if the transaction targets a repository.
func (txn *Transaction) repositoryTarget() bool {
	return txn.relativePath != ""
}

// PartitionRelativePaths returns all known repository relative paths for the
// transactions partition.
func (txn *Transaction) PartitionRelativePaths() []string {
	it := txn.KV().NewIterator(keyvalue.IteratorOptions{
		Prefix: []byte(relativePathKeyPrefix),
	})
	defer it.Close()

	var relativePaths []string
	for it.Rewind(); it.Valid(); it.Next() {
		key := it.Item().Key()
		relativePath := bytes.TrimPrefix(key, []byte(relativePathKeyPrefix))
		relativePaths = append(relativePaths, string(relativePath))
	}

	return relativePaths
}

// originalPackedRefsFilePath returns the path of the original `packed-refs` file that records the state of the
// `packed-refs` file as it was when the transaction started.
func (txn *Transaction) originalPackedRefsFilePath() string {
	return filepath.Join(txn.stagingDirectory, "packed-refs.original")
}

// RewriteRepository returns a copy of the repository that has been set up to correctly access
// the repository in the transaction's snapshot.
func (txn *Transaction) RewriteRepository(repo *gitalypb.Repository) *gitalypb.Repository {
	rewritten := proto.Clone(repo).(*gitalypb.Repository)
	rewritten.RelativePath = txn.snapshot.RelativePath(repo.GetRelativePath())

	if repo.GetRelativePath() == txn.relativePath {
		rewritten.GitObjectDirectory = txn.snapshotRepository.GetGitObjectDirectory()
		rewritten.GitAlternateObjectDirectories = txn.snapshotRepository.GetGitAlternateObjectDirectories()
	}

	return rewritten
}

// OriginalRepository returns the repository as it was before rewriting it to point to the snapshot.
func (txn *Transaction) OriginalRepository(repo *gitalypb.Repository) *gitalypb.Repository {
	original := proto.Clone(repo).(*gitalypb.Repository)
	original.RelativePath = strings.TrimPrefix(repo.GetRelativePath(), txn.snapshot.Prefix()+string(os.PathSeparator))
	original.GitObjectDirectory = ""
	original.GitAlternateObjectDirectories = nil
	return original
}

func (txn *Transaction) updateState(newState transactionState) error {
	txn.stateLatch.Lock()
	defer txn.stateLatch.Unlock()

	switch txn.state {
	case transactionStateOpen:
		txn.state = newState
		return nil
	case transactionStateRollback:
		return storage.ErrTransactionAlreadyRollbacked
	case transactionStateCommit:
		return storage.ErrTransactionAlreadyCommitted
	default:
		return fmt.Errorf("unknown transaction state: %q", txn.state)
	}
}

// Commit performs the changes. If no error is returned, the transaction was successful and the changes
// have been performed. If an error was returned, the transaction may or may not be persisted.
func (txn *Transaction) Commit(ctx context.Context) (returnedErr error) {
	defer trace.StartRegion(ctx, "commit").End()

	if err := txn.updateState(transactionStateCommit); err != nil {
		return err
	}

	defer prometheus.NewTimer(txn.metrics.commitDuration(txn.write)).ObserveDuration()

	defer func() {
		if err := txn.finishUnadmitted(); err != nil && returnedErr == nil {
			returnedErr = err
		}
	}()

	if !txn.write {
		// These errors are only for reporting programming mistakes where updates have been
		// accidentally staged in a read-only transaction. The changes would not be anyway
		// performed as read-only transactions are not committed through the manager.
		switch {
		case txn.deleteRepository:
			return errReadOnlyRepositoryDeletion
		case txn.runHousekeeping != nil:
			return errReadOnlyHousekeeping
		case len(txn.recordingReadWriter.WriteSet()) > 0:
			return errReadOnlyKeyValue
		default:
			return nil
		}
	}

	if txn.runHousekeeping != nil && (txn.referenceUpdates != nil ||
		txn.customHooksUpdated ||
		txn.deleteRepository ||
		txn.includedObjects != nil) {
		return errHousekeepingConflictOtherUpdates
	}

	return txn.commit(ctx, txn)
}

// Rollback releases resources associated with the transaction without performing any changes.
func (txn *Transaction) Rollback(ctx context.Context) error {
	defer trace.StartRegion(ctx, "rollback").End()

	if err := txn.updateState(transactionStateRollback); err != nil {
		return err
	}

	defer prometheus.NewTimer(txn.metrics.rollbackDuration(txn.write)).ObserveDuration()

	return txn.finishUnadmitted()
}

// finishUnadmitted cleans up after the transaction if it wasn't yet admitted. If the transaction was admitted,
// the Transaction is being processed by TransactionManager. The clean up responsibility moves there as well
// to avoid races.
func (txn *Transaction) finishUnadmitted() error {
	if txn.admitted {
		return nil
	}

	return txn.finish(false)
}

// SnapshotLSN returns the LSN of the Transaction's read snapshot.
func (txn *Transaction) SnapshotLSN() storage.LSN {
	return txn.snapshotLSN
}

// Root returns the path to the read snapshot.
func (txn *Transaction) Root() string {
	return txn.snapshot.Root()
}

// RecordInitialReferenceValues records the initial values of the references for the next UpdateReferences call. If oid is
// not a zero OID, it's used as the initial value. If oid is a zero value, the reference's actual value is resolved.
//
// The reference's first recorded value is used as its old OID in the update. RecordInitialReferenceValues can be used to
// record the value without staging an update in the transaction. This is useful for example generally recording the initial
// value in the 'prepare' phase of the reference transaction hook before any changes are made without staging any updates
// before the 'committed' phase is reached. The recorded initial values are only used for the next UpdateReferences call.
func (txn *Transaction) RecordInitialReferenceValues(ctx context.Context, initialValues map[git.ReferenceName]git.Reference) error {
	txn.initialReferenceValues = make(map[git.ReferenceName]git.Reference, len(initialValues))

	objectHash, err := txn.snapshotRepository.ObjectHash(ctx)
	if err != nil {
		return fmt.Errorf("object hash: %w", err)
	}

	for name, reference := range initialValues {
		if !reference.IsSymbolic {

			oid := git.ObjectID(reference.Target)

			if objectHash.IsZeroOID(oid) {
				// If this is a zero OID, resolve the value to see if this is a force update or the
				// reference doesn't exist.
				if current, err := txn.snapshotRepository.ResolveRevision(ctx, name.Revision()); err != nil {
					if !errors.Is(err, git.ErrReferenceNotFound) {
						return fmt.Errorf("resolve revision: %w", err)
					}

					// The reference doesn't exist, leave the value as zero oid.
				} else {
					oid = current
				}
			}

			txn.initialReferenceValues[name] = git.NewReference(name, oid)
		} else {
			txn.initialReferenceValues[name] = reference
		}
	}

	return nil
}

// UpdateReferences updates the given references as part of the transaction. Each call is treated as
// a different reference transaction. This allows for performing directory-file conflict inducing
// changes in a transaction. For example:
//
// - First call  - delete 'refs/heads/parent'
// - Second call - create 'refs/heads/parent/child'
//
// If a reference is updated multiple times during a transaction, its first recorded old OID used as
// the old OID when verifying the reference update, and the last recorded new OID is used as the new
// OID in the final commit. This means updates like 'oid-1 -> oid-2 -> oid-3' will ultimately be
// committed as 'oid-1 -> oid-3'. The old OIDs of the intermediate states are not verified when
// committing the write to the actual repository and are discarded from the final committed log
// entry.
func (txn *Transaction) UpdateReferences(ctx context.Context, updates git.ReferenceUpdates) error {
	u := git.ReferenceUpdates{}

	for reference, update := range updates {
		// Transactions should only stage references with valid names as otherwise Git would already
		// fail when they try to stage them against their snapshot. `update-ref` happily accepts references
		// outside of `refs` directory so such references could theoretically arrive here. We thus sanity
		// check that all references modified are within the refs directory.
		//
		// HEAD is a special case and refers to a default branch update.
		if !strings.HasPrefix(reference.String(), "refs/") && reference != "HEAD" {
			return InvalidReferenceFormatError{ReferenceName: reference}
		}

		oldOID := update.OldOID
		oldTarget := update.OldTarget

		if initialValue, ok := txn.initialReferenceValues[reference]; ok {
			if !initialValue.IsSymbolic {
				oldOID = git.ObjectID(initialValue.Target)
			} else {
				oldTarget = git.ReferenceName(initialValue.Target)
			}
		}

		if oldOID == update.NewOID && oldTarget == update.NewTarget {
			// This was a no-op.
			continue
		}

		for _, updates := range txn.referenceUpdates {
			if txUpdate, ok := updates[reference]; ok {
				if txUpdate.NewOID != "" {
					oldOID = txUpdate.NewOID
				}

				if txUpdate.NewTarget != "" {
					oldTarget = txUpdate.NewTarget
				}
			}
		}

		u[reference] = git.ReferenceUpdate{
			OldOID:    oldOID,
			NewOID:    update.NewOID,
			OldTarget: oldTarget,
			NewTarget: update.NewTarget,
		}
	}

	txn.initialReferenceValues = nil

	if len(u) == 0 {
		return nil
	}

	txn.referenceUpdates = append(txn.referenceUpdates, u)

	return nil
}

// DeleteRepository deletes the repository when the transaction is committed.
func (txn *Transaction) DeleteRepository() {
	txn.deleteRepository = true
}

// MarkCustomHooksUpdated sets a hint to the transaction manager that custom hooks have been updated as part
// of the transaction. This leads to the manager identifying changes and staging them for commit.
func (txn *Transaction) MarkCustomHooksUpdated() {
	txn.customHooksUpdated = true
}

// PackRefs sets pack-refs housekeeping task as a part of the transaction. The transaction can only runs other
// housekeeping tasks in the same transaction. No other updates are allowed.
func (txn *Transaction) PackRefs() {
	if txn.runHousekeeping == nil {
		txn.runHousekeeping = &runHousekeeping{}
	}
	txn.runHousekeeping.packRefs = &runPackRefs{
		PrunedRefs: map[git.ReferenceName]struct{}{},
	}
}

// Repack sets repacking housekeeping task as a part of the transaction.
func (txn *Transaction) Repack(config housekeepingcfg.RepackObjectsConfig) {
	if txn.runHousekeeping == nil {
		txn.runHousekeeping = &runHousekeeping{}
	}
	txn.runHousekeeping.repack = &runRepack{
		config: config,
	}
}

// WriteCommitGraphs enables the commit graph to be rewritten as part of the transaction.
func (txn *Transaction) WriteCommitGraphs(config housekeepingcfg.WriteCommitGraphConfig) {
	if txn.runHousekeeping == nil {
		txn.runHousekeeping = &runHousekeeping{}
	}
	txn.runHousekeeping.writeCommitGraphs = &writeCommitGraphs{
		config: config,
	}
}

// IncludeObject includes the given object and its dependencies in the transaction's logged pack file even
// if the object is unreachable from the references.
func (txn *Transaction) IncludeObject(oid git.ObjectID) {
	if txn.includedObjects == nil {
		txn.includedObjects = map[git.ObjectID]struct{}{}
	}

	txn.includedObjects[oid] = struct{}{}
}

// KV returns a handle to the key-value store snapshot of the transaction.
func (txn *Transaction) KV() keyvalue.ReadWriter {
	return keyvalue.NewPrefixedReadWriter(txn.recordingReadWriter, []byte("kv/"))
}

// MarkAlternateUpdated hints to the transaction manager that  'objects/info/alternates' file has been updated or
// removed. The file's modification will then be included in the transaction.
func (txn *Transaction) MarkAlternateUpdated() {
	txn.alternateUpdated = true
}

// walFilesPath returns the path to the directory where this transaction is staging the files that will
// be logged alongside the transaction's log entry.
func (txn *Transaction) walFilesPath() string {
	return filepath.Join(txn.stagingDirectory, "wal-files")
}

// snapshotLock contains state used to synchronize snapshotters and the log application with each other.
// Snapshotters wait on the applied channel until all of the committed writes in the read snapshot have
// been applied on the repository. The log application waits until all activeSnapshotters have managed to
// snapshot their state prior to applying the next log entry to the repository.
type snapshotLock struct {
	// applied is closed when the transaction the snapshotters are waiting for has been applied to the
	// repository and is ready for reading.
	applied chan struct{}
	// activeSnapshotters tracks snapshotters who are either taking a snapshot or waiting for the
	// log entry to be applied. Log application waits for active snapshotters to finish before applying
	// the next entry.
	activeSnapshotters sync.WaitGroup
}

// committedEntry is a wrapper for a log entry. It is used to keep track of entries in which their snapshots are still
// accessed by other transactions.
type committedEntry struct {
	// lsn is the associated LSN of the entry
	lsn storage.LSN
	// snapshotReaders accounts for the number of transaction readers of the snapshot.
	snapshotReaders int
	// objectDependencies are the objects this transaction depends upon.
	objectDependencies map[git.ObjectID]struct{}
}

// AcknowledgeTransaction acknowledges log entries up and including lsn as successfully processed
// for the specified LogConsumer. The manager is awakened if it is currently awaiting a new or
// completed transaction.
func (mgr *TransactionManager) AcknowledgeTransaction(lsn storage.LSN) {
	mgr.consumerPos.setPosition(lsn)

	// Alert the manager. If it has a pending acknowledgement already no action is required.
	select {
	case mgr.acknowledgedQueue <- struct{}{}:
	default:
	}
}

// GetTransactionPath returns the path of the log entry's root directory.
func (mgr *TransactionManager) GetTransactionPath(lsn storage.LSN) string {
	return walFilesPathForLSN(mgr.stateDirectory, lsn)
}

// consumerPosition tracks the last LSN acknowledged for a consumer.
type consumerPosition struct {
	// position is the last LSN acknowledged as completed by the consumer.
	position storage.LSN
	sync.Mutex
}

func (p *consumerPosition) getPosition() storage.LSN {
	p.Lock()
	defer p.Unlock()

	return p.position
}

func (p *consumerPosition) setPosition(pos storage.LSN) {
	p.Lock()
	defer p.Unlock()

	p.position = pos
}

// TransactionManager is responsible for transaction management of a single repository. Each repository has
// a single TransactionManager; it is the repository's single-writer. It accepts writes one at a time from
// the admissionQueue. Each admitted write is processed in three steps:
//
//  1. The references being updated are verified by ensuring the expected old tips match what the references
//     actually point to prior to update. The entire transaction is by default aborted if a single reference
//     fails the verification step. The reference verification behavior can be controlled on a per-transaction
//     level by setting:
//     - The reference verification failures can be ignored instead of aborting the entire transaction.
//     If done, the references that failed verification are dropped from the transaction but the updates
//     that passed verification are still performed.
//  2. The transaction is appended to the write-ahead log. Once the write has been logged, it is effectively
//     committed and will be applied to the repository even after restarting.
//  3. The transaction is applied from the write-ahead log to the repository by actually performing the reference
//     changes.
//
// The goroutine that issued the transaction is waiting for the result while these steps are being performed. As
// there is no transaction control for readers yet, the issuer is only notified of a successful write after the
// write has been applied to the repository.
//
// TransactionManager recovers transactions after interruptions by applying the write-ahead logged transactions to
// the repository on start up.
//
// TransactionManager maintains the write-ahead log in a key-value store. It maintains the following key spaces:
// - `partition/<partition_id>/applied_lsn`
//   - This key stores the LSN of the log entry that has been applied to the repository. This allows for
//     determining how far a partition is in processing the log and which log entries need to be applied
//     after starting up. Partition starts from LSN 0 if there are no log entries recorded to have
//     been applied.
//
// - `partition/<partition_id:string>/log/entry/<log_index:uint64>`
//   - These keys hold the actual write-ahead log entries. A partition's first log entry starts at LSN 1
//     and the LSN keeps monotonically increasing from there on without gaps. The write-ahead log
//     entries are processed in ascending order.
//
// The values in the database are marshaled protocol buffer messages. Numbers in the keys are encoded as big
// endian to preserve the sort order of the numbers also in lexicographical order.
type TransactionManager struct {
	// ctx is the context used for all operations.
	ctx context.Context
	// close cancels ctx and stops the transaction processing.
	close context.CancelFunc
	// logger is the logger to use to write log messages.
	logger log.Logger

	// closing is closed when close is called. It unblock transactions that are waiting to be admitted.
	closing <-chan struct{}
	// closed is closed when Run returns. It unblocks transactions that are waiting for a result after
	// being admitted. This is differentiated from ctx.Done in order to enable testing that Run correctly
	// releases awaiters when the transactions processing is stopped.
	closed chan struct{}
	// stateDirectory is an absolute path to a directory where the TransactionManager stores the state related to its
	// write-ahead log.
	stateDirectory string
	// stagingDirectory is a path to a directory where this TransactionManager should stage the files of the transactions
	// before it logs them. The TransactionManager cleans up the files during runtime but stale files may be
	// left around after crashes. The files are temporary and any leftover files are expected to be cleaned up when
	// Gitaly starts.
	stagingDirectory string
	// commandFactory is used to spawn git commands without a repository.
	commandFactory gitcmd.CommandFactory
	// repositoryFactory is used to build localrepo.Repo instances.
	repositoryFactory localrepo.StorageScopedFactory
	// storageName is the name of the storage the TransactionManager's partition is a member of.
	storageName string
	// storagePath is an absolute path to the root of the storage this TransactionManager
	// is operating in.
	storagePath string
	// storage.PartitionID is the ID of the partition this manager is operating on. This is used to determine the database keys.
	partitionID storage.PartitionID
	// db is the handle to the key-value store used for storing the write-ahead log related state.
	db keyvalue.Transactioner
	// admissionQueue is where the incoming writes are waiting to be admitted to the transaction
	// manager.
	admissionQueue chan *Transaction
	// completedQueue is a queue notifying when a transaction finishes.
	completedQueue chan struct{}

	// initialized is closed when the manager has been initialized. It's used to block new transactions
	// from beginning prior to the manager having initialized its runtime state on start up.
	initialized chan struct{}
	// initializationSuccessful is set if the TransactionManager initialized successfully. If it didn't,
	// transactions will fail to begin.
	initializationSuccessful bool
	// mutex guards access to snapshotLocks and appendedLSN. These fields are accessed by both
	// Run and Begin which are ran in different goroutines.
	mutex sync.Mutex

	// cleanupWorkers is a worker pool that TransactionManager uses to run transaction clean up in the
	// background. This way transaction processing is not blocked on the clean up.
	cleanupWorkers *errgroup.Group
	// cleanupWorkerFailed is closed if one of the clean up workers failed. This signals to the manager
	// to stop processing and exit.
	cleanupWorkerFailed chan struct{}
	// cleanupWorkerFailedOnce ensures cleanupWorkerFailed is closed only once.
	cleanupWorkerFailedOnce sync.Once

	// snapshotLocks contains state used for synchronizing snapshotters with the log application. The
	// lock is released after the corresponding log entry is applied.
	snapshotLocks map[storage.LSN]*snapshotLock
	// snapshotManager is responsible for creation and management of file system snapshots.
	snapshotManager *snapshot.Manager

	// conflictMgr is responsible for checking concurrent transactions against each other for conflicts.
	conflictMgr *conflict.Manager

	// appendedLSN holds the LSN of the last log entry appended to the partition's write-ahead log.
	appendedLSN storage.LSN
	// appliedLSN holds the LSN of the last log entry applied to the partition.
	appliedLSN storage.LSN
	// oldestLSN holds the LSN of the head of log entries which is still kept in the database. The manager keeps
	// them because they are still referred by a transaction.
	oldestLSN storage.LSN

	// awaitingTransactions contains transactions waiting for their log entry to be applied to
	// the partition. It's keyed by the LSN the transaction is waiting to be applied and the
	// value is the resultChannel that is waiting the result.
	awaitingTransactions map[storage.LSN]resultChannel
	// committedEntries keeps some latest appended log entries around. Some types of transactions, such as
	// housekeeping, operate on snapshot repository. There is a gap between transaction doing its work and the time
	// when it is committed. They need to verify if concurrent operations can cause conflict. These log entries are
	// still kept around even after they are applied. They are removed when there are no active readers accessing
	// the corresponding snapshots.
	committedEntries *list.List

	// consumer is an the external caller that may perform read-only operations against applied
	// log entries. Log entries are retained until the consumer has acknowledged past their LSN.
	consumer LogConsumer
	// consumerPos tracks the largest LSN that has been acknowledged by consumer.
	consumerPos *consumerPosition
	// acknowledgedQueue is a queue notifying when a transaction has been acknowledged.
	acknowledgedQueue chan struct{}

	// testHooks are used in the tests to trigger logic at certain points in the execution.
	// They are used to synchronize more complex test scenarios. Not used in production.
	testHooks testHooks

	// metrics stores reporters which facilitate metric recording of transactional operations.
	metrics ManagerMetrics
}

type testHooks struct {
	beforeInitialization      func()
	beforeAppendLogEntry      func()
	beforeApplyLogEntry       func()
	beforeStoreAppliedLSN     func()
	beforeDeleteLogEntryFiles func()
	beforeRunExiting          func()
}

// NewTransactionManager returns a new TransactionManager for the given repository.
func NewTransactionManager(
	ptnID storage.PartitionID,
	logger log.Logger,
	db keyvalue.Transactioner,
	storageName,
	storagePath,
	stateDir,
	stagingDir string,
	cmdFactory gitcmd.CommandFactory,
	repositoryFactory localrepo.StorageScopedFactory,
	metrics ManagerMetrics,
	consumer LogConsumer,
) *TransactionManager {
	ctx, cancel := context.WithCancel(context.Background())

	consumerPos := &consumerPosition{}

	cleanupWorkers := &errgroup.Group{}
	cleanupWorkers.SetLimit(25)

	return &TransactionManager{
		ctx:                  ctx,
		close:                cancel,
		logger:               logger,
		closing:              ctx.Done(),
		closed:               make(chan struct{}),
		commandFactory:       cmdFactory,
		repositoryFactory:    repositoryFactory,
		storageName:          storageName,
		storagePath:          storagePath,
		partitionID:          ptnID,
		db:                   db,
		admissionQueue:       make(chan *Transaction),
		completedQueue:       make(chan struct{}, 1),
		initialized:          make(chan struct{}),
		snapshotLocks:        make(map[storage.LSN]*snapshotLock),
		conflictMgr:          conflict.NewManager(),
		stateDirectory:       stateDir,
		stagingDirectory:     stagingDir,
		cleanupWorkers:       cleanupWorkers,
		cleanupWorkerFailed:  make(chan struct{}),
		awaitingTransactions: make(map[storage.LSN]resultChannel),
		committedEntries:     list.New(),
		metrics:              metrics,
		consumer:             consumer,
		consumerPos:          consumerPos,
		acknowledgedQueue:    make(chan struct{}, 1),

		testHooks: testHooks{
			beforeInitialization:      func() {},
			beforeAppendLogEntry:      func() {},
			beforeApplyLogEntry:       func() {},
			beforeStoreAppliedLSN:     func() {},
			beforeDeleteLogEntryFiles: func() {},
			beforeRunExiting:          func() {},
		},
	}
}

// resultChannel represents a future that will yield the result of a transaction once its
// outcome has been decided.
type resultChannel chan error

// commit queues the transaction for processing and returns once the result has been determined.
func (mgr *TransactionManager) commit(ctx context.Context, transaction *Transaction) error {
	span, ctx := tracing.StartSpanIfHasParent(ctx, "transaction.Commit", nil)
	defer span.Finish()

	transaction.result = make(resultChannel, 1)

	if transaction.repositoryTarget() && !transaction.repositoryExists {
		// Determine if the repository was created in this transaction and stage its state
		// for committing if so.
		if err := mgr.stageRepositoryCreation(ctx, transaction); err != nil {
			if errors.Is(err, storage.ErrRepositoryNotFound) {
				// The repository wasn't created as part of this transaction.
				return nil
			}

			return fmt.Errorf("stage repository creation: %w", err)
		}
	}

	if err := mgr.packObjects(ctx, transaction); err != nil {
		return fmt.Errorf("pack objects: %w", err)
	}

	if err := mgr.prepareHousekeeping(ctx, transaction); err != nil {
		return fmt.Errorf("preparing housekeeping: %w", err)
	}

	if transaction.repositoryCreation != nil {
		// Below we'll log the repository exactly as it was created by the transaction. While
		// we can expect the transaction leaves the repository in a good state, we'll override
		// the object directory of the repository so it only contains:
		// - The packfiles we generated above that contain only the reachable objects.
		// - The alternate link if set.
		//
		// This ensures the object state of the repository is exactly as the TransactionManager
		// expects it to be. There should be no objects missing dependencies or else the repository
		// could be corrupted if these objects are used. All objects in the generated pack
		// are verified to have all their dependencies present. Replacing the object directory thus
		// ensures we only log the packfile with the verified objects, and that no loose objects make
		// it into the repository, or anything else that breaks our assumption about the object
		// database contents.
		if err := mgr.replaceObjectDirectory(ctx, transaction); err != nil {
			return fmt.Errorf("replace object directory: %w", err)
		}

		if err := transaction.walEntry.RecordDirectoryCreation(
			transaction.snapshot.Root(),
			transaction.relativePath,
		); err != nil {
			return fmt.Errorf("record directory creation: %w", err)
		}

		if err := transaction.KV().Set(relativePathKey(transaction.relativePath), nil); err != nil {
			return fmt.Errorf("add relative path: %w", err)
		}
	} else {
		if transaction.alternateUpdated {
			stagedAlternatesRelativePath := stats.AlternatesFilePath(transaction.relativePath)
			stagedAlternatesAbsolutePath := mgr.getAbsolutePath(transaction.snapshot.RelativePath(stagedAlternatesRelativePath))
			if _, err := os.Stat(stagedAlternatesAbsolutePath); err != nil {
				if !errors.Is(err, fs.ErrNotExist) {
					return fmt.Errorf("check alternates existence: %w", err)
				}

				// Alternates file did not exist, nothing to stage. This was an unlink operation.
			} else {
				if err := transaction.walEntry.RecordFileCreation(
					stagedAlternatesAbsolutePath,
					stagedAlternatesRelativePath,
				); err != nil && !errors.Is(err, fs.ErrNotExist) {
					return fmt.Errorf("record alternates update: %w", err)
				}
			}
		}

		if transaction.customHooksUpdated {
			// Log the custom hook creation. The deletion of the previous hooks is logged after admission to
			// ensure we log the latest state for deletion in case someone else modified the hooks.
			//
			// If the transaction removed the custom hooks, we won't have anything to log. We'll ignore the
			// ErrNotExist and stage the deletion later.
			if err := transaction.walEntry.RecordDirectoryCreation(
				transaction.snapshot.Root(),
				filepath.Join(transaction.relativePath, repoutil.CustomHooksDir),
			); err != nil && !errors.Is(err, fs.ErrNotExist) {
				return fmt.Errorf("record custom hook directory: %w", err)
			}
		}

		// If there were objects packed that should be committed, record the packfile's creation.
		if transaction.packPrefix != "" {
			packDir := filepath.Join(transaction.relativePath, "objects", "pack")
			for _, fileExtension := range []string{".pack", ".idx", ".rev"} {
				if err := transaction.walEntry.RecordFileCreation(
					filepath.Join(transaction.stagingDirectory, "objects"+fileExtension),
					filepath.Join(packDir, transaction.packPrefix+fileExtension),
				); err != nil {
					return fmt.Errorf("record file creation: %w", err)
				}
			}
		}
	}

	if err := safe.NewSyncer().SyncRecursive(ctx, transaction.walFilesPath()); err != nil {
		return fmt.Errorf("synchronizing WAL directory: %w", err)
	}

	// The reference updates are staged into the transaction when they are verified, including
	// the packed-refs. While the reference files would generally be small, the packed-refs file
	// may be large. Sync the contents of the file before entering the critical section to ensure
	// we don't end up syncing the potentially very large file to disk when we're appending the
	// log entry.
	preImagePackedRefsInode, err := wal.GetInode(transaction.originalPackedRefsFilePath())
	if err != nil {
		return fmt.Errorf("get pre-image packed-refs inode: %w", err)
	}

	postImagePackedRefsPath := mgr.getAbsolutePath(
		transaction.snapshot.RelativePath(transaction.relativePath), "packed-refs",
	)
	postImagePackedRefsInode, err := wal.GetInode(postImagePackedRefsPath)
	if err != nil {
		return fmt.Errorf("get post-image packed-refs inode: %w", err)
	}

	// We check for modifications before syncing so we don't unnecessarily sync the changes to the
	// metadata of the original file if it hasn't been changed.
	if preImagePackedRefsInode != postImagePackedRefsInode && postImagePackedRefsInode > 0 {
		if err := safe.NewSyncer().Sync(ctx, postImagePackedRefsPath); err != nil {
			return fmt.Errorf("sync packed-refs: %w", err)
		}
	}

	if err := func() error {
		defer trace.StartRegion(ctx, "commit queue").End()
		transaction.metrics.commitQueueDepth.Inc()
		defer transaction.metrics.commitQueueDepth.Dec()
		defer prometheus.NewTimer(mgr.metrics.commitQueueWaitSeconds).ObserveDuration()

		select {
		case mgr.admissionQueue <- transaction:
			transaction.admitted = true
			return nil
		case <-ctx.Done():
			return ctx.Err()
		case <-mgr.closing:
			return storage.ErrTransactionProcessingStopped
		}
	}(); err != nil {
		return err
	}

	defer trace.StartRegion(ctx, "result wait").End()
	select {
	case err := <-transaction.result:
		return unwrapExpectedError(err)
	case <-ctx.Done():
		return ctx.Err()
	case <-mgr.closed:
		return storage.ErrTransactionProcessingStopped
	}
}

// replaceObjectDirectory replaces the snapshot repository's object directory
// to only contain the packs generated by TransactionManager and the possible
// alternate link if present.
func (mgr *TransactionManager) replaceObjectDirectory(ctx context.Context, tx *Transaction) (returnedErr error) {
	repoPath, err := tx.snapshotRepository.Path(ctx)
	if err != nil {
		return fmt.Errorf("snapshot repository path: %w", err)
	}

	objectsDir := filepath.Join(repoPath, "objects")
	objectsInfo, err := os.Stat(objectsDir)
	if err != nil {
		return fmt.Errorf("stat: %w", err)
	}

	// Create the standard object directory structure.
	newObjectsDir := filepath.Join(tx.stagingDirectory, "objects_tmp")
	if err := os.Mkdir(newObjectsDir, objectsInfo.Mode().Perm()); err != nil {
		return fmt.Errorf("create new objects dir: %w", err)
	}

	for _, dir := range []string{"info", "pack"} {
		info, err := os.Stat(filepath.Join(objectsDir, dir))
		if err != nil {
			return fmt.Errorf("stat: %w", err)
		}

		if err := os.Mkdir(filepath.Join(newObjectsDir, dir), info.Mode().Perm()); err != nil {
			return fmt.Errorf("mkdir: %w", err)
		}
	}

	// If there were objects packed that should be committed, link the pack to the new
	// replacement object directory.
	if tx.packPrefix != "" {
		for _, fileExtension := range []string{".pack", ".idx", ".rev"} {
			if err := os.Link(
				filepath.Join(tx.stagingDirectory, "objects"+fileExtension),
				filepath.Join(newObjectsDir, "pack", tx.packPrefix+fileExtension),
			); err != nil {
				return fmt.Errorf("link to synthesized object directory: %w", err)
			}
		}
	}

	// If there was an alternate link set by the transaction, place it also in the replacement
	// object directory.
	if tx.alternateUpdated {
		if err := os.Link(
			filepath.Join(objectsDir, "info", "alternates"),
			filepath.Join(newObjectsDir, "info", "alternates"),
		); err != nil {
			return fmt.Errorf("link alternate: %w", err)
		}
	}

	// Remove the old object directory from the snapshot repository and replace it with the
	// object directory that only contains verified data.
	if err := os.RemoveAll(objectsDir); err != nil {
		return fmt.Errorf("remove objects directory: %w", err)
	}

	if err := os.Rename(newObjectsDir, objectsDir); err != nil {
		return fmt.Errorf("rename replacement directory in place: %w", err)
	}

	return nil
}

// stageRepositoryCreation determines the repository's state following a creation. It reads the repository's
// complete state and stages it into the transaction for committing.
func (mgr *TransactionManager) stageRepositoryCreation(ctx context.Context, transaction *Transaction) error {
	if !transaction.repositoryTarget() {
		return errRelativePathNotSet
	}

	span, ctx := tracing.StartSpanIfHasParent(ctx, "transaction.stageRepositoryCreation", nil)
	defer span.Finish()

	objectHash, err := transaction.snapshotRepository.ObjectHash(ctx)
	if err != nil {
		return fmt.Errorf("object hash: %w", err)
	}

	transaction.repositoryCreation = &repositoryCreation{
		objectHash: objectHash,
	}

	references, err := transaction.snapshotRepository.GetReferences(ctx)
	if err != nil {
		return fmt.Errorf("get references: %w", err)
	}

	referenceUpdates := make(git.ReferenceUpdates, len(references))
	for _, ref := range references {
		if ref.IsSymbolic {
			return fmt.Errorf("unexpected symbolic ref: %v", ref)
		}

		referenceUpdates[ref.Name] = git.ReferenceUpdate{
			OldOID: objectHash.ZeroOID,
			NewOID: git.ObjectID(ref.Target),
		}
	}

	transaction.referenceUpdates = []git.ReferenceUpdates{referenceUpdates}

	var customHooks bytes.Buffer
	if err := repoutil.GetCustomHooks(ctx, mgr.logger,
		filepath.Join(mgr.storagePath, transaction.snapshotRepository.GetRelativePath()), &customHooks); err != nil {
		return fmt.Errorf("get custom hooks: %w", err)
	}

	if customHooks.Len() > 0 {
		transaction.MarkCustomHooksUpdated()
	}

	if _, err := gitstorage.ReadAlternatesFile(mgr.getAbsolutePath(transaction.snapshotRepository.GetRelativePath())); err != nil {
		if !errors.Is(err, gitstorage.ErrNoAlternate) {
			return fmt.Errorf("read alternates file: %w", err)
		}

		// Repository had no alternate.
	} else {
		transaction.MarkAlternateUpdated()
	}

	return nil
}

// setupStagingRepository sets up a snapshot that is used for verifying and staging changes. It contains up to
// date state of the partition. It does not have the quarantine configured.
func (mgr *TransactionManager) setupStagingRepository(ctx context.Context, transaction *Transaction, alternateRelativePath string) error {
	defer trace.StartRegion(ctx, "setupStagingRepository").End()

	if !transaction.repositoryTarget() {
		return nil
	}

	span, ctx := tracing.StartSpanIfHasParent(ctx, "transaction.setupStagingRepository", nil)
	defer span.Finish()

	relativePaths := []string{transaction.relativePath}
	if alternateRelativePath != "" {
		relativePaths = append(relativePaths, alternateRelativePath)
	}

	var err error
	transaction.stagingSnapshot, err = mgr.snapshotManager.GetSnapshot(ctx, relativePaths, true)
	if err != nil {
		return fmt.Errorf("new snapshot: %w", err)
	}

	// If this is a creation, the repository does not yet exist in the storage. Create a temporary repository
	// we can use to stage the updates.
	if transaction.repositoryCreation != nil {
		// The reference updates in the transaction are normally verified against the actual repository.
		// If the repository doesn't exist yet, the reference updates are verified against an empty
		// repository to ensure they'll apply when the log entry creates the repository. After the
		// transaction is logged, the staging repository is removed, and the actual repository will be
		// created when the log entry is applied.
		if err := mgr.createRepository(ctx, mgr.getAbsolutePath(transaction.stagingSnapshot.RelativePath(transaction.relativePath)), transaction.repositoryCreation.objectHash.ProtoFormat); err != nil {
			return fmt.Errorf("create staging repository: %w", err)
		}
	}

	if alternateRelativePath != "" {
		alternatesContent, err := filepath.Rel(
			filepath.Join(transaction.relativePath, "objects"),
			filepath.Join(alternateRelativePath, "objects"),
		)
		if err != nil {
			return fmt.Errorf("rel: %w", err)
		}

		if err := os.WriteFile(
			stats.AlternatesFilePath(mgr.getAbsolutePath(transaction.stagingSnapshot.RelativePath(transaction.relativePath))),
			[]byte(alternatesContent),
			mode.File,
		); err != nil {
			return fmt.Errorf("insert modified alternate file: %w", err)
		}
	}

	transaction.stagingRepository = mgr.repositoryFactory.Build(transaction.stagingSnapshot.RelativePath(transaction.relativePath))

	return nil
}

// packPrefixRegexp matches the output of `git index-pack` where it
// prints the packs prefix in the format `pack <digest>`.
var packPrefixRegexp = regexp.MustCompile(`^pack\t([0-9a-f]+)\n$`)

// packObjects walks the objects in the quarantine directory starting from the new
// reference tips introduced by the transaction and the explicitly included objects. All
// objects in the quarantine directory that are encountered during the walk are included in
// a packfile that gets committed with the transaction. All encountered objects that are missing
// from the quarantine directory are considered the transaction's dependencies. The dependencies
// are later verified to exist in the repository before committing the transaction, and they will
// be guarded against concurrent pruning operations. The final pack is staged in the WAL directory
// of the transaction ready for committing. The pack's index and reverse index is also included.
//
// Objects that were not reachable from the walk are not committed with the transaction. Objects
// that already exist in the repository are included in the packfile if the client wrote them into
// the quarantine directory.
//
// The packed objects are not yet checked for validity. See the following issue for more
// details on this: https://gitlab.com/gitlab-org/gitaly/-/issues/5779
func (mgr *TransactionManager) packObjects(ctx context.Context, transaction *Transaction) (returnedErr error) {
	defer trace.StartRegion(ctx, "packObjects").End()

	if !transaction.repositoryTarget() {
		return nil
	}

	if _, err := os.Stat(mgr.getAbsolutePath(transaction.snapshotRepository.GetRelativePath())); err != nil {
		if !errors.Is(err, fs.ErrNotExist) {
			return fmt.Errorf("stat: %w", err)
		}

		// The repository does not exist. Exit early as the Git commands below would fail. There's
		// nothing to pack and no dependencies if the repository doesn't exist.
		return nil
	}

	span, ctx := tracing.StartSpanIfHasParent(ctx, "transaction.packObjects", nil)
	defer span.Finish()

	quarantineOnlySnapshotRepository := transaction.snapshotRepository
	if transaction.snapshotRepository.GetGitObjectDirectory() != "" {
		// We want to only pack the objects that are present in the quarantine as they are potentially
		// new. Disable the alternate, which is the repository's original object directory, so that we'll
		// only walk the objects in the quarantine directory below.
		var err error
		quarantineOnlySnapshotRepository, err = transaction.snapshotRepository.QuarantineOnly()
		if err != nil {
			return fmt.Errorf("quarantine only: %w", err)
		}
	} else {
		// If this transaction is creating a repository, the repository is not configured with a quarantine.
		// The objects in the repository's object directory are the new objects. The repository's actual
		// object directory may contain an `objects/info/alternates` file pointing to an alternate. We don't
		// want to include the objects in the alternate in the packfile given they should already be present
		// in the alternate. In order to only walk the new objects, we disable the alternate by renaming
		// the alternates file so Git doesn't recognize the file, and restore it after we're done walking the
		// objects. We have an issue tracking an option to disasble the alternate through configuration in Git.
		//
		// Issue: https://gitlab.com/gitlab-org/git/-/issues/177
		repoPath, err := quarantineOnlySnapshotRepository.Path(ctx)
		if err != nil {
			return fmt.Errorf("repo path: %w", err)
		}

		originalAlternatesPath := stats.AlternatesFilePath(repoPath)
		disabledAlternatesPath := originalAlternatesPath + ".disabled"
		if err := os.Rename(originalAlternatesPath, disabledAlternatesPath); err != nil {
			if !errors.Is(err, fs.ErrNotExist) {
				return fmt.Errorf("disable alternates: %w", err)
			}

			// There was no alternates file.
		} else {
			// If the alternates file existed, we'll restore it back in its place after packing the objects.
			defer func() {
				if err := os.Rename(disabledAlternatesPath, originalAlternatesPath); err != nil && returnedErr == nil {
					returnedErr = fmt.Errorf("restore alternates: %w", err)
				}
			}()
		}
	}

	objectHash, err := quarantineOnlySnapshotRepository.ObjectHash(ctx)
	if err != nil {
		return fmt.Errorf("object hash: %w", err)
	}

	heads := make([]string, 0)
	for _, referenceUpdates := range transaction.referenceUpdates {
		for _, update := range referenceUpdates {
			if !update.IsRegularUpdate() {
				// We don't have to worry about symrefs here.
				continue
			}

			if update.NewOID == objectHash.ZeroOID {
				// Reference deletions can't introduce new objects so ignore them.
				continue
			}

			heads = append(heads, update.NewOID.String())
		}
	}

	for objectID := range transaction.includedObjects {
		heads = append(heads, objectID.String())
	}

	if len(heads) == 0 {
		// No need to pack objects if there are no changes that can introduce new objects.
		return nil
	}

	objectWalkReader, objectWalkWriter := io.Pipe()

	group, ctx := errgroup.WithContext(ctx)
	group.Go(func() (returnedErr error) {
		defer objectWalkWriter.CloseWithError(returnedErr)

		// Walk the new reference tips and included objects in the quarantine directory. The reachable
		// objects will be included in the transaction's logged packfile and the unreachable ones
		// discarded, and missing objects regard as the transaction's dependencies.
		if err := quarantineOnlySnapshotRepository.WalkObjects(ctx,
			strings.NewReader(strings.Join(heads, "\n")),
			objectWalkWriter,
		); err != nil {
			return fmt.Errorf("walk objects: %w", err)
		}

		return nil
	})

	objectsToPackReader, objectsToPackWriter := io.Pipe()
	// We'll only start the commands needed for object packing if the walk above produces objects
	// we need to pack.
	startObjectPacking := func() {
		packReader, packWriter := io.Pipe()
		group.Go(func() (returnedErr error) {
			defer func() {
				objectsToPackReader.CloseWithError(returnedErr)
				packWriter.CloseWithError(returnedErr)
			}()

			if err := quarantineOnlySnapshotRepository.PackObjects(ctx, objectsToPackReader, packWriter); err != nil {
				return fmt.Errorf("pack objects: %w", err)
			}

			return nil
		})

		group.Go(func() (returnedErr error) {
			defer packReader.CloseWithError(returnedErr)

			// index-pack places the pack, index, and reverse index into the transaction's staging directory.
			var stdout, stderr bytes.Buffer
			if err := quarantineOnlySnapshotRepository.ExecAndWait(ctx, gitcmd.Command{
				Name:  "index-pack",
				Flags: []gitcmd.Option{gitcmd.Flag{Name: "--stdin"}, gitcmd.Flag{Name: "--rev-index"}},
				Args:  []string{filepath.Join(transaction.stagingDirectory, "objects.pack")},
			}, gitcmd.WithStdin(packReader), gitcmd.WithStdout(&stdout), gitcmd.WithStderr(&stderr)); err != nil {
				return structerr.New("index pack: %w", err).WithMetadata("stderr", stderr.String())
			}

			matches := packPrefixRegexp.FindStringSubmatch(stdout.String())
			if len(matches) != 2 {
				return structerr.New("unexpected index-pack output").WithMetadata("stdout", stdout.String())
			}

			transaction.packPrefix = fmt.Sprintf("pack-%s", matches[1])

			return nil
		})
	}

	transaction.objectDependencies = map[git.ObjectID]struct{}{}
	group.Go(func() (returnedErr error) {
		defer objectWalkReader.CloseWithError(returnedErr)

		// objectLine comes in two formats from the walk:
		//   1. '<oid> <path>\n' in case the object is found. <path> may or may not be set.
		//   2. '?<oid>\n' in case the object is not found.
		//
		// Objects that are found are included in the transaction's packfile.
		//
		// Objects that are not found are recorded as the transaction's
		// dependencies since they should exist in the repository.
		scanner := bufio.NewScanner(objectWalkReader)

		defer objectsToPackWriter.CloseWithError(returnedErr)

		packObjectsStarted := false
		for scanner.Scan() {
			objectLine := scanner.Text()
			if objectLine[0] == '?' {
				// Remove the '?' prefix so we're left with just the object ID.
				transaction.objectDependencies[git.ObjectID(objectLine[1:])] = struct{}{}
				continue
			}

			// At this point we have an object that we need to pack. If `pack-objects` and `index-pack`
			// haven't yet been launched, launch them.
			if !packObjectsStarted {
				packObjectsStarted = true
				startObjectPacking()
			}

			// Write the objects to `git pack-objects`. Restore the new line that was
			// trimmed by the scanner.
			if _, err := objectsToPackWriter.Write([]byte(objectLine + "\n")); err != nil {
				return fmt.Errorf("write object id for packing: %w", err)
			}
		}

		if err := scanner.Err(); err != nil {
			return fmt.Errorf("scanning rev-list output: %w", err)
		}

		return nil
	})

	return group.Wait()
}

// unwrapExpectedError unwraps expected errors that may occur and returns them directly to the caller.
func unwrapExpectedError(err error) error {
	// The manager controls its own execution context and it is canceled only when Stop is called.
	// Any context.Canceled errors returned are thus from shutting down so we report that here.
	if errors.Is(err, context.Canceled) {
		return storage.ErrTransactionProcessingStopped
	}

	return err
}

// Run starts the transaction processing. On start up Run loads the indexes of the last appended and applied
// log entries from the database. It will then apply any transactions that have been logged but not applied
// to the repository. Once the recovery is completed, Run starts processing new transactions by verifying the
// references, logging the transaction and finally applying it to the repository. The transactions are acknowledged
// once they've been applied to the repository.
//
// Run keeps running until Stop is called or it encounters a fatal error. All transactions will error with
// storage.ErrTransactionProcessingStopped when Run returns.
func (mgr *TransactionManager) Run() error {
	return mgr.run(mgr.ctx)
}

func (mgr *TransactionManager) run(ctx context.Context) (returnedErr error) {
	defer func() {
		// On-going operations may fail with a context canceled error if the manager is stopped. This is
		// not a real error though given the manager will recover from this on restart. Swallow the error.
		if errors.Is(returnedErr, context.Canceled) {
			returnedErr = nil
		}
	}()

	defer func() {
		if err := mgr.cleanupWorkers.Wait(); err != nil {
			returnedErr = errors.Join(returnedErr, fmt.Errorf("clean up worker: %w", err))
		}
	}()
	// Defer the Stop in order to release all on-going Commit calls in case of error.
	defer close(mgr.closed)
	defer mgr.Close()
	defer mgr.testHooks.beforeRunExiting()

	if err := mgr.initialize(ctx); err != nil {
		return fmt.Errorf("initialize: %w", err)
	}

	for {
		if mgr.appliedLSN < mgr.appendedLSN {
			lsn := mgr.appliedLSN + 1

			if err := mgr.applyLogEntry(ctx, lsn); err != nil {
				return fmt.Errorf("apply log entry: %w", err)
			}

			continue
		}

		// When a log entry is applied, if there is any log in front of it which are still referred, we cannot delete
		// it. This condition is to prevent a "hole" in the list. A transaction referring to a log entry at the
		// low-water mark might scan all afterward log entries. Thus, the manager needs to keep in the database.
		//
		// ┌─ Oldest LSN
		// ┌─ Can be removed ─┐            ┌─ Cannot be removed
		// □ □ □ □ □ □ □ □ □ □ ■ ■ ⧅ ⧅ ⧅ ⧅ ⧅ ⧅ ■ ■ ⧅ ⧅ ⧅ ⧅ ■
		//                     └─ Low-water mark, still referred by another transaction
		if mgr.oldestLSN < mgr.lowWaterMark() {
			if err := mgr.deleteLogEntry(ctx, mgr.oldestLSN); err != nil {
				return fmt.Errorf("deleting log entry: %w", err)
			}

			// The WAL entries are deleted only after there are no transactions using an
			// older read snapshot than the LSN. It's also safe to drop the transaction
			// from the conflict detection history as there are no transactions reading
			// at an older snapshot. Since the changes are already in the transaction's
			// snapshot, it would already base its changes on them.
			mgr.conflictMgr.EvictLSN(ctx, mgr.oldestLSN)

			mgr.oldestLSN++
			continue
		}

		if err := mgr.processTransaction(ctx); err != nil {
			return fmt.Errorf("process transaction: %w", err)
		}
	}
}

// processTransaction waits for a transaction and processes it by verifying and
// logging it.
func (mgr *TransactionManager) processTransaction(ctx context.Context) (returnedErr error) {
	var transaction *Transaction
	select {
	case transaction = <-mgr.admissionQueue:
		defer trace.StartRegion(ctx, "processTransaction").End()
		defer prometheus.NewTimer(mgr.metrics.transactionProcessingDurationSeconds).ObserveDuration()

		// The transaction does not finish itself anymore once it has been admitted for
		// processing. This avoids the client concurrently removing the staged state
		// while the manager is still operating on it. We thus need to defer its finishing.
		//
		// The error is always empty here as we run the clean up in background. If a background
		// task fails, cleanupWorkerFailed channel is closed prompting the manager to exit and
		// return the error from the errgroup.
		defer func() { _ = transaction.finish(true) }()
	case <-mgr.cleanupWorkerFailed:
		return errors.New("cleanup worker failed")
	case <-mgr.completedQueue:
		return nil
	case <-mgr.acknowledgedQueue:
		return nil
	case <-ctx.Done():
	}

	// Return if the manager was stopped. The select is indeterministic so this guarantees
	// the manager stops the processing even if there are transactions in the queue.
	if err := ctx.Err(); err != nil {
		return err
	}

	span, ctx := tracing.StartSpanIfHasParent(ctx, "transaction.processTransaction", nil)
	defer span.Finish()

	if err := func() (commitErr error) {
		repositoryExists, err := mgr.doesRepositoryExist(ctx, transaction.relativePath)
		if err != nil {
			return fmt.Errorf("does repository exist: %w", err)
		}

		logEntry := &gitalypb.LogEntry{
			RelativePath: transaction.relativePath,
		}

		if transaction.repositoryCreation != nil && repositoryExists {
			return ErrRepositoryAlreadyExists
		} else if transaction.repositoryCreation == nil && !repositoryExists {
			return storage.ErrRepositoryNotFound
		}

		alternateRelativePath, err := mgr.verifyAlternateUpdate(ctx, transaction)
		if err != nil {
			return fmt.Errorf("verify alternate update: %w", err)
		}

		if err := mgr.setupStagingRepository(ctx, transaction, alternateRelativePath); err != nil {
			return fmt.Errorf("setup staging snapshot: %w", err)
		}

		// Verify that all objects this transaction depends on are present in the repository. The dependency
		// objects are the reference tips set in the transaction and the objects the transaction's packfile
		// is based on. If an object dependency is missing, the transaction is aborted as applying it would
		// result in repository corruption.
		if err := mgr.verifyObjectsExist(ctx, transaction.stagingRepository, transaction.objectDependencies); err != nil {
			return fmt.Errorf("verify object dependencies: %w", err)
		}

		var zeroOID git.ObjectID
		if transaction.repositoryTarget() {
			objectHash, err := transaction.stagingRepository.ObjectHash(ctx)
			if err != nil {
				return fmt.Errorf("object hash: %w", err)
			}

			zeroOID = objectHash.ZeroOID
		}

		// Prepare the transaction to conflict check it. We'll commit it later if we
		// succeed logging the transaction.
		preparedTX, err := mgr.conflictMgr.Prepare(ctx, &conflict.Transaction{
			ReadLSN:            transaction.SnapshotLSN(),
			TargetRelativePath: transaction.relativePath,
			DeleteRepository:   transaction.deleteRepository,
			ZeroOID:            zeroOID,
			ReferenceUpdates:   transaction.referenceUpdates,
		})
		if err != nil {
			return fmt.Errorf("prepare: %w", err)
		}

		if transaction.repositoryCreation == nil && transaction.runHousekeeping == nil {
			logEntry.ReferenceTransactions, err = mgr.verifyReferences(ctx, transaction)
			if err != nil {
				return fmt.Errorf("verify references: %w", err)
			}
		}

		if transaction.customHooksUpdated {
			// Log a deletion of the existing custom hooks so they are removed before the
			// new ones are put in place.
			if err := transaction.walEntry.RecordDirectoryRemoval(
				mgr.storagePath,
				filepath.Join(transaction.relativePath, repoutil.CustomHooksDir),
			); err != nil && !errors.Is(err, fs.ErrNotExist) {
				return fmt.Errorf("record custom hook removal: %w", err)
			}
		}

		if transaction.deleteRepository {
			if len(transaction.walEntry.Operations()) != 0 {
				return errRepositoryDeletionOtherOperations
			}

			logEntry.RepositoryDeletion = &gitalypb.LogEntry_RepositoryDeletion{}

			if err := transaction.walEntry.RecordDirectoryRemoval(
				mgr.storagePath,
				transaction.relativePath,
			); err != nil && !errors.Is(err, fs.ErrNotExist) {
				return fmt.Errorf("record repository removal: %w", err)
			}

			if err := transaction.KV().Delete(relativePathKey(transaction.relativePath)); err != nil {
				return fmt.Errorf("delete relative path: %w", err)
			}
		}

		if transaction.runHousekeeping != nil {
			housekeepingEntry, err := mgr.verifyHousekeeping(ctx, transaction)
			if err != nil {
				return fmt.Errorf("verifying pack refs: %w", err)
			}
			logEntry.Housekeeping = housekeepingEntry
		}

		if err := mgr.verifyKeyValueOperations(ctx, transaction); err != nil {
			return fmt.Errorf("verify key-value operations: %w", err)
		}

		logEntry.Operations = transaction.walEntry.Operations()

		if err := mgr.appendLogEntry(ctx, transaction.objectDependencies, logEntry, transaction.walFilesPath()); err != nil {
			return fmt.Errorf("append log entry: %w", err)
		}

		// Commit the prepared transaction now that we've managed to commit the log entry.
		preparedTX.Commit(ctx, mgr.appendedLSN)

		return nil
	}(); err != nil {
		transaction.result <- err
		return nil
	}

	mgr.awaitingTransactions[mgr.appendedLSN] = transaction.result

	return nil
}

// verifyKeyValueOperations checks the key-value operations of the transaction for conflicts and includes
// them in the log entry. The conflict checking ensures serializability. Transaction is considered to
// conflict if it read a key a concurrently committed transaction set or deleted. Iterated key prefixes
// are predicate locked.
func (mgr *TransactionManager) verifyKeyValueOperations(ctx context.Context, tx *Transaction) error {
	defer trace.StartRegion(ctx, "verifyKeyValueOperations").End()

	if readSet := tx.recordingReadWriter.ReadSet(); len(readSet) > 0 {
		if err := mgr.walkCommittedEntries(tx, func(entry *gitalypb.LogEntry, _ map[git.ObjectID]struct{}) error {
			for _, op := range entry.GetOperations() {
				var key []byte
				switch op := op.GetOperation().(type) {
				case *gitalypb.LogEntry_Operation_SetKey_:
					key = op.SetKey.GetKey()
				case *gitalypb.LogEntry_Operation_DeleteKey_:
					key = op.DeleteKey.GetKey()
				}

				stringKey := string(key)
				if _, ok := readSet[stringKey]; ok {
					return newConflictingKeyValueOperationError(stringKey)
				}

				for prefix := range tx.recordingReadWriter.PrefixesRead() {
					if bytes.HasPrefix(key, []byte(prefix)) {
						return newConflictingKeyValueOperationError(stringKey)
					}
				}
			}

			return nil
		}); err != nil {
			return fmt.Errorf("walking committed entries: %w", err)
		}
	}

	for key := range tx.recordingReadWriter.WriteSet() {
		key := []byte(key)
		item, err := tx.db.Get(key)
		if err != nil {
			if errors.Is(err, badger.ErrKeyNotFound) {
				tx.walEntry.DeleteKey(key)
				continue
			}

			return fmt.Errorf("get: %w", err)
		}

		value, err := item.ValueCopy(nil)
		if err != nil {
			return fmt.Errorf("value copy: %w", err)
		}

		tx.walEntry.SetKey(key, value)
	}

	return nil
}

// verifyObjectsExist verifies that all objects passed in to the method exist in the repository.
// If an object is missing, an InvalidObjectError error is raised.
func (mgr *TransactionManager) verifyObjectsExist(ctx context.Context, repository *localrepo.Repo, oids map[git.ObjectID]struct{}) error {
	defer trace.StartRegion(ctx, "verifyObjectsExist").End()

	if len(oids) == 0 {
		return nil
	}

	revisions := make([]git.Revision, 0, len(oids))
	for oid := range oids {
		revisions = append(revisions, oid.Revision())
	}

	objectHash, err := repository.ObjectHash(ctx)
	if err != nil {
		return fmt.Errorf("object hash: %w", err)
	}

	if err := checkObjects(ctx, repository, revisions, func(revision git.Revision, oid git.ObjectID) error {
		if objectHash.IsZeroOID(oid) {
			return localrepo.InvalidObjectError(revision)
		}

		return nil
	}); err != nil {
		return fmt.Errorf("check objects: %w", err)
	}

	return nil
}

// Close stops the transaction processing causing Run to return.
func (mgr *TransactionManager) Close() { mgr.close() }

// CloseSnapshots closes any remaining snapshots in the cache. Caller of Run() should
// call it after there are no more active transactions and no new transactions will be
// started.
func (mgr *TransactionManager) CloseSnapshots() error {
	// snapshotManager may not be set if initializing it fails.
	if mgr.snapshotManager == nil {
		return nil
	}

	return mgr.snapshotManager.Close()
}

// snapshotsDir returns the directory where the transactions' snapshots are stored.
func (mgr *TransactionManager) snapshotsDir() string {
	return filepath.Join(mgr.stagingDirectory, "snapshots")
}

// initialize initializes the TransactionManager's state from the database. It loads the appended and the applied
// LSNs and initializes the notification channels that synchronize transaction beginning with log entry applying.
func (mgr *TransactionManager) initialize(ctx context.Context) error {
	defer trace.StartRegion(ctx, "initialize").End()

	defer close(mgr.initialized)

	var appliedLSN gitalypb.LSN
	if err := mgr.readKey(keyAppliedLSN, &appliedLSN); err != nil && !errors.Is(err, badger.ErrKeyNotFound) {
		return fmt.Errorf("read applied LSN: %w", err)
	}

	mgr.appliedLSN = storage.LSN(appliedLSN.GetValue())

	if err := mgr.createStateDirectory(ctx); err != nil {
		return fmt.Errorf("create state directory: %w", err)
	}

	if err := os.Mkdir(mgr.snapshotsDir(), mode.Directory); err != nil {
		return fmt.Errorf("create snapshot manager directory: %w", err)
	}

	var err error
	if mgr.snapshotManager, err = snapshot.NewManager(mgr.logger, mgr.storagePath, mgr.snapshotsDir(), mgr.metrics.snapshot); err != nil {
		return fmt.Errorf("new snapshot manager: %w", err)
	}

	// The LSN of the last appended log entry is determined from the LSN of the latest entry in the log and
	// the latest applied log entry. The manager also keeps track of committed entries and reserves them until there
	// is no transaction refers them. It's possible there are some left-over entries in the database because a
	// transaction can hold the entry stubbornly. So, the manager could not clean them up in the last session.
	//
	// ┌─ oldestLSN                    ┌─ appendedLSN
	// ⧅ ⧅ ⧅ ⧅ ⧅ ⧅ ⧅ ■ ■ ■ ■ ■ ■ ■ ■ ■ ■
	//                └─ appliedLSN
	//
	//
	// oldestLSN is initialized to appliedLSN + 1. If there are no log entries in the log, then everything has been
	// pruned already or there has not been any log entries yet. Setting this +1 avoids trying to clean up log entries
	// that do not exist. If there are some, we'll set oldestLSN to the head of the log below.
	mgr.oldestLSN = mgr.appliedLSN + 1
	// appendedLSN is initialized to appliedLSN. If there are no log entries, then there has been no transaction yet, or
	// all log entries have been applied and have been already pruned. If there are some in the log, we'll update this
	// below to match.
	mgr.appendedLSN = mgr.appliedLSN

	if logEntries, err := os.ReadDir(walFilesPath(mgr.stateDirectory)); err != nil {
		return fmt.Errorf("read wal directory: %w", err)
	} else if len(logEntries) > 0 {
		if mgr.oldestLSN, err = storage.ParseLSN(logEntries[0].Name()); err != nil {
			return fmt.Errorf("parse oldest LSN: %w", err)
		}
		if mgr.appendedLSN, err = storage.ParseLSN(logEntries[len(logEntries)-1].Name()); err != nil {
			return fmt.Errorf("parse appended LSN: %w", err)
		}
	}

	if mgr.consumer != nil {
		mgr.consumer.NotifyNewTransactions(mgr.storageName, mgr.partitionID, mgr.oldestLSN, mgr.appendedLSN)
	}

	// Create a snapshot lock for the applied LSN as it is used for synchronizing
	// the snapshotters with the log application.
	mgr.snapshotLocks[mgr.appliedLSN] = &snapshotLock{applied: make(chan struct{})}
	close(mgr.snapshotLocks[mgr.appliedLSN].applied)

	// Each unapplied log entry should have a snapshot lock as they are created in normal
	// operation when committing a log entry. Recover these entries.
	for i := mgr.appliedLSN + 1; i <= mgr.appendedLSN; i++ {
		mgr.snapshotLocks[i] = &snapshotLock{applied: make(chan struct{})}
	}

	if err := mgr.removeStaleWALFiles(ctx, mgr.oldestLSN, mgr.appendedLSN); err != nil {
		return fmt.Errorf("remove stale packs: %w", err)
	}

	mgr.testHooks.beforeInitialization()
	mgr.initializationSuccessful = true

	return nil
}

// doesRepositoryExist returns whether the repository exists or not.
func (mgr *TransactionManager) doesRepositoryExist(ctx context.Context, relativePath string) (bool, error) {
	defer trace.StartRegion(ctx, "doesRepositoryExist").End()

	stat, err := os.Stat(mgr.getAbsolutePath(relativePath))
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return false, nil
		}

		return false, fmt.Errorf("stat repository directory: %w", err)
	}

	if !stat.IsDir() {
		return false, errNotDirectory
	}

	return true, nil
}

func (mgr *TransactionManager) createStateDirectory(ctx context.Context) error {
	needsFsync := false
	for _, path := range []string{
		mgr.stateDirectory,
		filepath.Join(mgr.stateDirectory, "wal"),
	} {
		if err := os.Mkdir(path, mode.Directory); err != nil {
			if !errors.Is(err, fs.ErrExist) {
				return fmt.Errorf("mkdir: %w", err)
			}

			continue
		}

		// The directory was created so we need to fsync.
		needsFsync = true
	}

	// If the directories already existed and we didn't create them, don't fsync.
	if !needsFsync {
		return nil
	}

	syncer := safe.NewSyncer()
	if err := syncer.SyncRecursive(ctx, mgr.stateDirectory); err != nil {
		return fmt.Errorf("sync: %w", err)
	}

	if err := syncer.SyncParent(ctx, mgr.stateDirectory); err != nil {
		return fmt.Errorf("sync parent: %w", err)
	}

	return nil
}

// getAbsolutePath returns the relative path's absolute path in the storage.
func (mgr *TransactionManager) getAbsolutePath(relativePath ...string) string {
	return filepath.Join(append([]string{mgr.storagePath}, relativePath...)...)
}

// removeStaleWALFiles removes files from the log directory that have no associated log entry.
// Such files can be left around if transaction's files were moved in place successfully
// but the manager was interrupted before successfully persisting the log entry itself.
// If the manager deletes a log entry successfully from the database but is interrupted before it cleans
// up the associated files, such a directory can also be left at the head of the log.
func (mgr *TransactionManager) removeStaleWALFiles(ctx context.Context, oldestLSN, appendedLSN storage.LSN) error {
	needsFsync := false
	for _, possibleStaleFilesPath := range []string{
		// Log entries are pruned one by one. If a write is interrupted, the only possible stale files would be
		// for the log entry preceding the oldest log entry.
		walFilesPathForLSN(mgr.stateDirectory, oldestLSN-1),
		// Log entries are appended one by one to the log. If a write is interrupted, the only possible stale
		// files would be for the next LSN. Remove the files if they exist.
		walFilesPathForLSN(mgr.stateDirectory, appendedLSN+1),
	} {

		if _, err := os.Stat(possibleStaleFilesPath); err != nil {
			if !errors.Is(err, fs.ErrNotExist) {
				return fmt.Errorf("stat: %w", err)
			}

			// No stale files were present.
			continue
		}

		if err := os.RemoveAll(possibleStaleFilesPath); err != nil {
			return fmt.Errorf("remove all: %w", err)
		}

		needsFsync = true
	}

	if needsFsync {
		// Sync the parent directory to flush the file deletion.
		if err := safe.NewSyncer().Sync(ctx, walFilesPath(mgr.stateDirectory)); err != nil {
			return fmt.Errorf("sync: %w", err)
		}
	}

	return nil
}

// walFilesPath returns the WAL directory's path.
func walFilesPath(stateDir string) string {
	return filepath.Join(stateDir, "wal")
}

// walFilesPathForLSN returns an absolute path to a given log entry's WAL files.
func walFilesPathForLSN(stateDir string, lsn storage.LSN) string {
	return filepath.Join(walFilesPath(stateDir), lsn.String())
}

// manifestPath returns the manifest file's path in the log entry.
func manifestPath(logEntryPath string) string {
	return filepath.Join(logEntryPath, "MANIFEST")
}

// packFilePath returns a log entry's pack file's absolute path in the wal files directory.
func packFilePath(walFiles string) string {
	return filepath.Join(walFiles, "transaction.pack")
}

// verifyAlternateUpdate verifies the staged alternate update.
func (mgr *TransactionManager) verifyAlternateUpdate(ctx context.Context, transaction *Transaction) (string, error) {
	defer trace.StartRegion(ctx, "verifyAlternateUpdate").End()

	if !transaction.alternateUpdated {
		return "", nil
	}
	if !transaction.repositoryTarget() {
		return "", errRelativePathNotSet
	}

	span, _ := tracing.StartSpanIfHasParent(ctx, "transaction.verifyAlternateUpdate", nil)
	defer span.Finish()

	repositoryPath := mgr.getAbsolutePath(transaction.relativePath)
	existingAlternate, err := gitstorage.ReadAlternatesFile(repositoryPath)
	if err != nil && !errors.Is(err, gitstorage.ErrNoAlternate) {
		return "", fmt.Errorf("read existing alternates file: %w", err)
	}

	snapshotRepoPath, err := transaction.snapshotRepository.Path(ctx)
	if err != nil {
		return "", fmt.Errorf("snapshot repo path: %w", err)
	}

	stagedAlternate, err := gitstorage.ReadAlternatesFile(snapshotRepoPath)
	if err != nil && !errors.Is(err, gitstorage.ErrNoAlternate) {
		return "", fmt.Errorf("read staged alternates file: %w", err)
	}

	if stagedAlternate == "" {
		if existingAlternate == "" {
			// Transaction attempted to remove an alternate from the repository
			// even if it didn't have one.
			return "", gitstorage.ErrNoAlternate
		}

		if err := transaction.walEntry.RecordAlternateUnlink(mgr.storagePath, transaction.relativePath, existingAlternate); err != nil {
			return "", fmt.Errorf("record alternate unlink: %w", err)
		}

		return "", nil
	}

	if existingAlternate != "" {
		return "", errAlternateAlreadyLinked
	}

	alternateObjectsDir, err := storage.ValidateRelativePath(
		mgr.storagePath,
		filepath.Join(transaction.relativePath, "objects", stagedAlternate),
	)
	if err != nil {
		return "", fmt.Errorf("validate relative path: %w", err)
	}

	alternateRelativePath := filepath.Dir(alternateObjectsDir)
	if alternateRelativePath == transaction.relativePath {
		return "", storage.ErrAlternatePointsToSelf
	}

	// Check that the alternate repository exists. This works as a basic conflict check
	// to prevent linking a repository that was deleted concurrently.
	alternateRepositoryPath := mgr.getAbsolutePath(alternateRelativePath)
	if err := storage.ValidateGitDirectory(alternateRepositoryPath); err != nil {
		return "", fmt.Errorf("validate git directory: %w", err)
	}

	if _, err := gitstorage.ReadAlternatesFile(alternateRepositoryPath); !errors.Is(err, gitstorage.ErrNoAlternate) {
		if err == nil {
			// We don't support chaining alternates like repo-1 > repo-2 > repo-3.
			return "", storage.ErrAlternateHasAlternate
		}

		return "", fmt.Errorf("read alternates file: %w", err)
	}

	return alternateRelativePath, nil
}

// verifyReferences verifies that the references in the transaction apply on top of the already accepted
// reference changes. The old tips in the transaction are verified against the current actual tips.
// It returns the write-ahead log entry for the reference transactions successfully verified.
func (mgr *TransactionManager) verifyReferences(ctx context.Context, transaction *Transaction) ([]*gitalypb.LogEntry_ReferenceTransaction, error) {
	defer trace.StartRegion(ctx, "verifyReferences").End()

	if len(transaction.referenceUpdates) == 0 {
		return nil, nil
	}
	if !transaction.repositoryTarget() {
		return nil, errRelativePathNotSet
	}

	span, _ := tracing.StartSpanIfHasParent(ctx, "transaction.verifyReferences", nil)
	defer span.Finish()

	var referenceTransactions []*gitalypb.LogEntry_ReferenceTransaction
	for _, updates := range transaction.referenceUpdates {
		changes := make([]*gitalypb.LogEntry_ReferenceTransaction_Change, 0, len(updates))
		for reference, update := range updates {
			changes = append(changes, &gitalypb.LogEntry_ReferenceTransaction_Change{
				ReferenceName: []byte(reference),
				NewOid:        []byte(update.NewOID),
				NewTarget:     []byte(update.NewTarget),
			})
		}

		// Sort the reference updates so the reference changes are always logged in a deterministic order.
		sort.Slice(changes, func(i, j int) bool {
			return bytes.Compare(
				changes[i].GetReferenceName(),
				changes[j].GetReferenceName(),
			) == -1
		})

		referenceTransactions = append(referenceTransactions, &gitalypb.LogEntry_ReferenceTransaction{
			Changes: changes,
		})
	}

	// Apply quarantine to the staging repository in order to ensure the new objects are available when we
	// are verifying references. Without it we'd encounter errors about missing objects as the new objects
	// are not in the repository.
	stagingRepositoryWithQuarantine, err := transaction.stagingRepository.Quarantine(ctx, transaction.quarantineDirectory)
	if err != nil {
		return nil, fmt.Errorf("quarantine: %w", err)
	}

	// For the reftable backend, we also need to capture HEAD updates here.
	// So obtain the reference backend to do the specific checks.
	refBackend, err := stagingRepositoryWithQuarantine.ReferenceBackend(ctx)
	if err != nil {
		return nil, fmt.Errorf("reference backend: %w", err)
	}

	if refBackend == git.ReferenceBackendReftables {
		if err := mgr.verifyReferencesWithGitForReftables(ctx, referenceTransactions, transaction, stagingRepositoryWithQuarantine); err != nil {
			return nil, fmt.Errorf("verify references with git: %w", err)
		}
	} else {
		if err := mgr.verifyReferencesWithGit(ctx, referenceTransactions, transaction, stagingRepositoryWithQuarantine); err != nil {
			return nil, fmt.Errorf("verify references with git: %w", err)
		}
	}

	return referenceTransactions, nil
}

// verifyReferencesWithGitForReftables is responsible for converting the logical reference updates
// to transaction operations.
//
// To ensure that we don't modify existing tables and autocompact, we lock the existing tables
// before applying the updates. This way the reftable backend willl only create new tables
func (mgr *TransactionManager) verifyReferencesWithGitForReftables(
	ctx context.Context,
	referenceTransactions []*gitalypb.LogEntry_ReferenceTransaction,
	tx *Transaction,
	repo *localrepo.Repo,
) error {
	reftablePath := mgr.getAbsolutePath(repo.GetRelativePath(), "reftable/")
	existingTables := make(map[string]struct{})
	lockedTables := make(map[string]struct{})

	// reftableWalker allows us to walk the reftable directory.
	reftableWalker := func(handler func(path string) error) fs.WalkDirFunc {
		return func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}

			if d.IsDir() {
				if filepath.Base(path) == "reftable" {
					return nil
				}

				return fmt.Errorf("unexpected directory: %s", filepath.Base(path))
			}

			return handler(path)
		}
	}

	// We first track the existing tables in the reftable directory.
	if err := filepath.WalkDir(
		reftablePath,
		reftableWalker(func(path string) error {
			if filepath.Base(path) == "tables.list" {
				return nil
			}

			existingTables[path] = struct{}{}

			return nil
		}),
	); err != nil {
		return fmt.Errorf("finding reftables: %w", err)
	}

	// We then lock existing tables as to disable the autocompaction.
	for table := range existingTables {
		lockedPath := table + ".lock"

		f, err := os.Create(lockedPath)
		if err != nil {
			return fmt.Errorf("creating reftable lock: %w", err)
		}
		if err = f.Close(); err != nil {
			return fmt.Errorf("closing reftable lock: %w", err)
		}

		lockedTables[lockedPath] = struct{}{}
	}

	// Since autocompaction is now disabled, adding references will
	// add new tables but not compact them.
	for _, referenceTransaction := range referenceTransactions {
		if err := mgr.applyReferenceTransaction(ctx, referenceTransaction.GetChanges(), repo); err != nil {
			return fmt.Errorf("applying reference: %w", err)
		}
	}

	// With this, we can track the new tables added along with the 'tables.list'
	// as operations on the transaction.
	if err := filepath.WalkDir(
		reftablePath,
		reftableWalker(func(path string) error {
			if _, ok := lockedTables[path]; ok {
				return nil
			}

			if _, ok := existingTables[path]; ok {
				return nil
			}

			base := filepath.Base(path)

			if base == "tables.list" {
				return tx.walEntry.RecordFileUpdate(tx.stagingSnapshot.Root(), filepath.Join(tx.relativePath, "reftable", base))
			}
			return tx.walEntry.RecordFileCreation(path, filepath.Join(tx.relativePath, "reftable", base))
		}),
	); err != nil {
		return fmt.Errorf("finding reftables: %w", err)
	}

	// Finally release the locked tables.
	for lockedTable := range lockedTables {
		if err := os.Remove(lockedTable); err != nil {
			return fmt.Errorf("deleting locked file: %w", err)
		}
	}

	return nil
}

func (txn *Transaction) containsReferenceDeletions(ctx context.Context, stagingRepository *localrepo.Repo) (bool, error) {
	objectHash, err := stagingRepository.ObjectHash(ctx)
	if err != nil {
		return false, fmt.Errorf("object hash: %w", err)
	}

	for _, refTX := range txn.referenceUpdates {
		for _, update := range refTX {
			if update.NewOID == objectHash.ZeroOID {
				return true, nil
			}
		}
	}

	return false, nil
}

func (mgr *TransactionManager) stagePackedRefs(ctx context.Context, tx *Transaction, stagingRepository *localrepo.Repo) error {
	// Get the inode of the `packed-refs` file as it was before the transaction. This was
	// recorded when the transaction began.
	preImagePackedRefsInode, err := wal.GetInode(tx.originalPackedRefsFilePath())
	if err != nil {
		return fmt.Errorf("get pre-image packed-refs inode: %w", err)
	}

	// Get the inode of the `packed-refs` file as it is in the snapshot after the transaction. This contains
	// all of the modifications the transaction has performed on it.
	postImagePackedRefsPath := mgr.getAbsolutePath(tx.snapshot.RelativePath(tx.relativePath), "packed-refs")
	postImagePackedRefsInode, err := wal.GetInode(postImagePackedRefsPath)
	if err != nil {
		return fmt.Errorf("get post-image packed-refs inode: %w", err)
	}

	if preImagePackedRefsInode == postImagePackedRefsInode {
		// The transaction itself didn't modify the packed-refs file. However, if there was
		// a concurrent reference packing operation, it would have moved the loose references
		// into packed-refs. As they were loose references in our snapshot, we would have deleted
		// just the loose references. The concurrently committed packed-refs file would now contain
		// the deleted references. This would require us to rewrite the pack as part of applying the
		// reference updates. Abort the transaction due to the conflict as we can't just replace the
		// packed-refs file with our own anymore as it would also lose the other references that were
		// packed.
		//
		// If our transaction includes any reference deletions, ensure there hasn't been a concurrent
		// reference packing operation.
		//
		// We allow modifications to packed-refs other than repacking. This would only be reference
		// deletions. If our transaction didn't modify the packed-refs file, the references we are
		// deleting were not packed. They don't conflict with removal of other references that were
		// removed from the packed-refs file by a concurrent transaction.
		containsReferenceDeletions, err := tx.containsReferenceDeletions(ctx, stagingRepository)
		if err != nil {
			return fmt.Errorf("contains reference deletions: %w", err)
		}

		if containsReferenceDeletions {
			if err := mgr.walkCommittedEntries(tx, func(entry *gitalypb.LogEntry, dependencies map[git.ObjectID]struct{}) error {
				if entry.GetHousekeeping().GetPackRefs() != nil {
					return errConcurrentReferencePacking
				}
				return nil
			}); err != nil {
				return fmt.Errorf("check for concurrent reference packing: %w", err)
			}
		}

		return nil
	}

	// Get the inode of the `packed-refs` file as it is currently in the repository.
	stagingRepoPath, err := stagingRepository.Path(ctx)
	if err != nil {
		return fmt.Errorf("staging repo path: %w", err)
	}

	currentPackedRefsPath := filepath.Join(stagingRepoPath, "packed-refs")
	currentPackedRefsInode, err := wal.GetInode(currentPackedRefsPath)
	if err != nil {
		return fmt.Errorf("get current packed-refs inode: %w", err)
	}

	if preImagePackedRefsInode != currentPackedRefsInode {
		// There's a conflict, the packed-refs file has been concurrently changed.
		return errConcurrentPackedRefsWrite
	}

	packedRefsRelativePath := filepath.Join(tx.relativePath, "packed-refs")
	if currentPackedRefsInode > 0 {
		// If the repository has a packed-refs file already, remove it.
		tx.walEntry.RecordDirectoryEntryRemoval(packedRefsRelativePath)

		// Remove the current packed-refs file from the staging repository if the transaction
		// changed the packed-refs file. If the packed-refs file was removed, this applies
		// the removal. If it was updated, we'll link the new one in place below.
		if err := os.Remove(currentPackedRefsPath); err != nil {
			return fmt.Errorf("remove packed-refs: %w", err)
		}
	}

	if postImagePackedRefsInode > 0 {
		// If there is a new packed refs file, stage it.
		if err := tx.walEntry.RecordFileCreation(
			postImagePackedRefsPath,
			packedRefsRelativePath,
		); err != nil {
			return fmt.Errorf("record new packed-refs: %w", err)
		}

		// If the transaction created or modified the packed-refs file, link the new one into
		// the staging repository.
		if err := os.Link(
			postImagePackedRefsPath,
			currentPackedRefsPath,
		); err != nil {
			return fmt.Errorf("link post-image packed-refs: %w", err)
		}
	}

	return nil
}

// verifyReferencesWithGit verifies the reference updates with git by committing them against a snapshot of the target
// repository. This ensures the updates will go through when they are being applied from the log. This also catches any
// invalid reference names and file/directory conflicts with Git's loose reference storage which can occur with references
// like 'refs/heads/parent' and 'refs/heads/parent/child'.
func (mgr *TransactionManager) verifyReferencesWithGit(ctx context.Context, referenceTransactions []*gitalypb.LogEntry_ReferenceTransaction, tx *Transaction, repo *localrepo.Repo) error {
	// We don't want to delete references from the packed-refs in `RecordReferenceUpdates` below as it can be very
	// slow and blocks all other transaction processing. We avoid this by staging the packed-refs file from
	// transaction's post-image into the staging repository. All references the transaction would have deleted
	// from the packed-refs file thus no longer exist there so `applyReferenceUpdates` below does not have to
	// remove them. Any loose references we must delete are still in the repository, and will be recorded
	// as we apply the deletions.
	//
	// As we're staging the transaction's modified packed-refs file as is, we must ensure no other transaction
	// has modified the file. We do this by checking whether the packed-refs file's inode is the same as it was
	// when our transaction began. If not, someone has modified the packed-refs file and us overriding the changes
	// there could lead to lost updates.
	if err := mgr.stagePackedRefs(ctx, tx, repo); err != nil {
		return fmt.Errorf("stage packed-refs: %w", err)
	}

	objectHash, err := repo.ObjectHash(ctx)
	if err != nil {
		return fmt.Errorf("object hash: %w", err)
	}

	tmpDir := filepath.Join(tx.stagingDirectory, "ref-recorder")
	if err := os.Mkdir(tmpDir, mode.Directory); err != nil {
		return fmt.Errorf("mkdir: %w", err)
	}

	recorder, err := wal.NewReferenceRecorder(tmpDir, tx.walEntry, tx.stagingSnapshot.Root(), tx.relativePath, objectHash.ZeroOID)
	if err != nil {
		return fmt.Errorf("new recorder: %w", err)
	}

	for i, updates := range tx.referenceUpdates {
		if err := mgr.applyReferenceTransaction(ctx, referenceTransactions[i].GetChanges(), repo); err != nil {
			return fmt.Errorf("apply reference transaction: %w", err)
		}

		if err := recorder.RecordReferenceUpdates(ctx, updates); err != nil {
			return fmt.Errorf("record reference updates: %w", err)
		}
	}

	if err := recorder.StagePackedRefs(); err != nil {
		return fmt.Errorf("stage packed-refs: %w", err)
	}

	return nil
}

// verifyHousekeeping verifies if all included housekeeping tasks can be performed. Although it's feasible for multiple
// housekeeping tasks running at the same time, it's not guaranteed they are conflict-free. So, we need to ensure there
// is no other concurrent housekeeping task. Each sub-task also needs specific verification.
func (mgr *TransactionManager) verifyHousekeeping(ctx context.Context, transaction *Transaction) (*gitalypb.LogEntry_Housekeeping, error) {
	defer trace.StartRegion(ctx, "verifyHousekeeping").End()

	span, ctx := tracing.StartSpanIfHasParent(ctx, "transaction.verifyHousekeeping", nil)
	defer span.Finish()

	finishTimer := mgr.metrics.housekeeping.ReportTaskLatency("total", "verify")
	defer finishTimer()

	mgr.mutex.Lock()
	defer mgr.mutex.Unlock()

	// Check for any concurrent housekeeping between this transaction's snapshot LSN and the latest appended LSN.
	if err := mgr.walkCommittedEntries(transaction, func(entry *gitalypb.LogEntry, objectDependencies map[git.ObjectID]struct{}) error {
		if entry.GetHousekeeping() != nil {
			return errHousekeepingConflictConcurrent
		}
		if entry.GetRepositoryDeletion() != nil {
			return errConflictRepositoryDeletion
		}

		// Applying a repacking operation prunes all loose objects on application. If loose objects were concurrently introduced
		// in the repository with the repacking operation, this could lead to corruption if we prune a loose object that is needed.
		// Transactions in general only introduce packs, not loose objects. The only exception to this currently is alternate
		// unlinking operations where the objects of the alternate are hard linked into the member repository. This can technically
		// still introduce loose objects into the repository and trigger this problem as the pools could still have loose objects
		// in them until the first repack.
		//
		// Check if the repository was unlinked from an alternate concurrently.
		for _, op := range entry.GetOperations() {
			switch op := op.GetOperation().(type) {
			case *gitalypb.LogEntry_Operation_RemoveDirectoryEntry_:
				if string(op.RemoveDirectoryEntry.GetPath()) == stats.AlternatesFilePath(transaction.relativePath) {
					return errConcurrentAlternateUnlink
				}
			}
		}

		return nil
	}); err != nil {
		return nil, fmt.Errorf("walking committed entries: %w", err)
	}

	packRefsEntry, err := mgr.verifyPackRefs(ctx, transaction)
	if err != nil {
		return nil, fmt.Errorf("verifying pack refs: %w", err)
	}

	if err := mgr.verifyRepacking(ctx, transaction); err != nil {
		return nil, fmt.Errorf("verifying repacking: %w", err)
	}

	return &gitalypb.LogEntry_Housekeeping{
		PackRefs: packRefsEntry,
	}, nil
}

// applyReferenceTransaction applies a reference transaction with `git update-ref`.
func (mgr *TransactionManager) applyReferenceTransaction(ctx context.Context, changes []*gitalypb.LogEntry_ReferenceTransaction_Change, repository *localrepo.Repo) (returnedErr error) {
	defer trace.StartRegion(ctx, "applyReferenceTransaction").End()

	updater, err := updateref.New(ctx, repository, updateref.WithDisabledTransactions(), updateref.WithNoDeref())
	if err != nil {
		return fmt.Errorf("new: %w", err)
	}
	defer func() {
		if err := updater.Close(); err != nil {
			returnedErr = errors.Join(returnedErr, fmt.Errorf("close updater: %w", err))
		}
	}()

	if err := updater.Start(); err != nil {
		return fmt.Errorf("start: %w", err)
	}

	version, err := repository.GitVersion(ctx)
	if err != nil {
		return fmt.Errorf("git version: %w", err)
	}

	for _, change := range changes {
		if len(change.GetNewTarget()) > 0 {
			if err := updater.UpdateSymbolicReference(
				version,
				git.ReferenceName(change.GetReferenceName()),
				git.ReferenceName(change.GetNewTarget()),
			); err != nil {
				return fmt.Errorf("update symref %q: %w", change.GetReferenceName(), err)
			}
		} else {
			if err := updater.Update(git.ReferenceName(change.GetReferenceName()), git.ObjectID(change.GetNewOid()), ""); err != nil {
				return fmt.Errorf("update %q: %w", change.GetReferenceName(), err)
			}
		}
	}

	if err := updater.Prepare(); err != nil {
		return fmt.Errorf("prepare: %w", err)
	}

	if err := updater.Commit(); err != nil {
		return fmt.Errorf("commit: %w", err)
	}

	return nil
}

// appendLogEntry appends the transaction to the write-ahead log. It first writes the transaction's manifest file
// into the log entry's directory. Afterwards it moves the log entry's directory from the staging area to its final
// place in the write-ahead log.
func (mgr *TransactionManager) appendLogEntry(ctx context.Context, objectDependencies map[git.ObjectID]struct{}, logEntry *gitalypb.LogEntry, logEntryPath string) error {
	defer trace.StartRegion(ctx, "appendLogEntry").End()

	manifestBytes, err := proto.Marshal(logEntry)
	if err != nil {
		return fmt.Errorf("marshal manifest: %w", err)
	}

	// Finalize the log entry by writing the MANIFEST file into the log entry's directory.
	manifestPath := manifestPath(logEntryPath)
	if err := os.WriteFile(manifestPath, manifestBytes, mode.File); err != nil {
		return fmt.Errorf("write manifest: %w", err)
	}

	// Sync the log entry completely before committing it.
	//
	// Ideally the log entry would be completely flushed to the disk before queuing the
	// transaction for commit to ensure we don't write a lot to the disk while in the critical
	// section. We currently stage some of the files only in the critical section though. This
	// is due to currently lacking conflict checks which prevents staging the log entry completely
	// before queuing it for commit.
	//
	// See https://gitlab.com/gitlab-org/gitaly/-/issues/5892 for more details. Once the issue is
	// addressed, we could stage the transaction entirely before queuing it for commit, and thus not
	// need to sync here.
	if err := safe.NewSyncer().SyncRecursive(ctx, logEntryPath); err != nil {
		return fmt.Errorf("synchronizing WAL directory: %w", err)
	}

	mgr.testHooks.beforeAppendLogEntry()

	nextLSN := mgr.appendedLSN + 1
	// Move the log entry from the staging directory into its place in the log.
	destinationPath := walFilesPathForLSN(mgr.stateDirectory, nextLSN)
	if err := os.Rename(logEntryPath, destinationPath); err != nil {
		return fmt.Errorf("move wal files: %w", err)
	}

	// Sync the WAL directory. The manifest has been synced above, and all of the other files
	// have been synced before queuing for commit. At this point we just have to sync the
	// directory entry of the new log entry in the WAL directory to finalize the commit.
	//
	// After this sync, the log entry has been persisted and will be recovered on failure.
	if err := safe.NewSyncer().SyncParent(ctx, destinationPath); err != nil {
		// If this fails, the log entry will be left in the write-ahead log but it is not
		// properly persisted. If the fsync fails, something is seriously wrong and there's no
		// point trying to delete the files. The right thing to do is to terminate Gitaly
		// immediately as going further could cause data loss and corruption. This error check
		// will later be replaced with a panic that terminates Gitaly.
		//
		// For more details, see: https://gitlab.com/gitlab-org/gitaly/-/issues/5774
		return fmt.Errorf("sync log entry: %w", err)
	}

	// After this latch block, the transaction is committed and all subsequent transactions
	// are guaranteed to read it.
	mgr.mutex.Lock()
	mgr.appendedLSN = nextLSN
	mgr.snapshotLocks[nextLSN] = &snapshotLock{applied: make(chan struct{})}
	mgr.committedEntries.PushBack(&committedEntry{
		lsn:                nextLSN,
		objectDependencies: objectDependencies,
	})
	mgr.mutex.Unlock()

	if mgr.consumer != nil {
		mgr.consumer.NotifyNewTransactions(mgr.storageName, mgr.partitionID, mgr.lowWaterMark(), nextLSN)
	}
	return nil
}

// applyLogEntry reads a log entry at the given LSN and applies it to the repository.
func (mgr *TransactionManager) applyLogEntry(ctx context.Context, lsn storage.LSN) error {
	defer trace.StartRegion(ctx, "applyLogEntry").End()

	defer prometheus.NewTimer(mgr.metrics.transactionApplicationDurationSeconds).ObserveDuration()

	logEntry, err := mgr.readLogEntry(lsn)
	if err != nil {
		return fmt.Errorf("read log entry: %w", err)
	}

	// Ensure all snapshotters have finished snapshotting the previous state before we apply
	// the new state to the repository. No new snapshotters can arrive at this point. All
	// new transactions would be waiting for the committed log entry we are about to apply.
	previousLSN := lsn - 1
	mgr.snapshotLocks[previousLSN].activeSnapshotters.Wait()
	mgr.mutex.Lock()
	delete(mgr.snapshotLocks, previousLSN)
	mgr.mutex.Unlock()

	mgr.testHooks.beforeApplyLogEntry()

	if err := mgr.db.Update(func(tx keyvalue.ReadWriter) error {
		if err := applyOperations(ctx, safe.NewSyncer().Sync, mgr.storagePath, walFilesPathForLSN(mgr.stateDirectory, lsn), logEntry.GetOperations(), tx); err != nil {
			return fmt.Errorf("apply operations: %w", err)
		}

		return nil
	}); err != nil {
		return fmt.Errorf("update: %w", err)
	}

	mgr.testHooks.beforeStoreAppliedLSN()
	if err := mgr.storeAppliedLSN(lsn); err != nil {
		return fmt.Errorf("set applied LSN: %w", err)
	}

	mgr.appliedLSN = lsn
	mgr.snapshotManager.SetLSN(lsn)

	// There is no awaiter for a transaction if the transaction manager is recovering
	// transactions from the log after starting up.
	if resultChan, ok := mgr.awaitingTransactions[lsn]; ok {
		resultChan <- nil
		delete(mgr.awaitingTransactions, lsn)
	}

	// Notify the transactions waiting for this log entry to be applied prior to take their
	// snapshot.
	close(mgr.snapshotLocks[lsn].applied)

	return nil
}

// createRepository creates a repository at the given path with the given object format.
func (mgr *TransactionManager) createRepository(ctx context.Context, repositoryPath string, objectFormat gitalypb.ObjectFormat) error {
	objectHash, err := git.ObjectHashByProto(objectFormat)
	if err != nil {
		return fmt.Errorf("object hash by proto: %w", err)
	}

	stderr := &bytes.Buffer{}
	cmd, err := mgr.commandFactory.NewWithoutRepo(ctx, gitcmd.Command{
		Name: "init",
		Flags: []gitcmd.Option{
			gitcmd.Flag{Name: "--bare"},
			gitcmd.Flag{Name: "--quiet"},
			gitcmd.Flag{Name: "--object-format=" + objectHash.Format},
		},
		Args: []string{repositoryPath},
	}, gitcmd.WithStderr(stderr))
	if err != nil {
		return fmt.Errorf("spawn git init: %w", err)
	}

	if err := cmd.Wait(); err != nil {
		return structerr.New("wait git init: %w", err).WithMetadata("stderr", stderr.String())
	}

	return nil
}

// deleteLogEntry deletes the log entry at the given LSN from the log.
func (mgr *TransactionManager) deleteLogEntry(ctx context.Context, lsn storage.LSN) error {
	defer trace.StartRegion(ctx, "deleteLogEntry").End()

	tmpDir, err := os.MkdirTemp(mgr.stagingDirectory, "")
	if err != nil {
		return fmt.Errorf("mkdir temp: %w", err)
	}

	logEntryPath := walFilesPathForLSN(mgr.stateDirectory, lsn)
	// We can't delete a directory atomically as we have to first delete all of its content.
	// If the deletion was interrupted, we'd be left with a corrupted log entry on the disk.
	// To perform the deletion atomically, we move the to be deleted log entry out from the
	// log into a temporary directory and sync the move. After that, the log entry is no longer
	// in the log, and we can delete the files without having to worry about the deletion being
	// interrupted and being left with a corrupted log entry.
	if err := os.Rename(logEntryPath, filepath.Join(tmpDir, "to_delete")); err != nil {
		return fmt.Errorf("rename: %w", err)
	}

	if err := safe.NewSyncer().SyncParent(ctx, logEntryPath); err != nil {
		return fmt.Errorf("sync file deletion: %w", err)
	}

	// With the log entry removed from the log, we can now delete the files. There's no need
	// to sync the deletions as the log entry is a temporary directory that will be removed
	// on start up if they are left around from a crash.
	if err := os.RemoveAll(tmpDir); err != nil {
		return fmt.Errorf("remove files: %w", err)
	}

	return nil
}

// readLogEntry returns the log entry from the given position in the log.
func (mgr *TransactionManager) readLogEntry(lsn storage.LSN) (*gitalypb.LogEntry, error) {
	manifestBytes, err := os.ReadFile(manifestPath(walFilesPathForLSN(mgr.stateDirectory, lsn)))
	if err != nil {
		return nil, fmt.Errorf("read manifest: %w", err)
	}

	var logEntry gitalypb.LogEntry
	if err := proto.Unmarshal(manifestBytes, &logEntry); err != nil {
		return nil, fmt.Errorf("unmarshal manifest: %w", err)
	}

	return &logEntry, nil
}

// storeAppliedLSN stores the partition's applied LSN in the database.
func (mgr *TransactionManager) storeAppliedLSN(lsn storage.LSN) error {
	return mgr.setKey(keyAppliedLSN, lsn.ToProto())
}

// setKey marshals and stores a given protocol buffer message into the database under the given key.
func (mgr *TransactionManager) setKey(key []byte, value proto.Message) error {
	marshaledValue, err := proto.Marshal(value)
	if err != nil {
		return fmt.Errorf("marshal value: %w", err)
	}

	writeBatch := mgr.db.NewWriteBatch()
	defer writeBatch.Cancel()

	if err := writeBatch.Set(key, marshaledValue); err != nil {
		return fmt.Errorf("set: %w", err)
	}

	return writeBatch.Flush()
}

// readKey reads a key from the database and unmarshals its value in to the destination protocol
// buffer message.
func (mgr *TransactionManager) readKey(key []byte, destination proto.Message) error {
	return mgr.db.View(func(txn keyvalue.ReadWriter) error {
		item, err := txn.Get(key)
		if err != nil {
			return fmt.Errorf("get: %w", err)
		}

		return item.Value(func(value []byte) error { return proto.Unmarshal(value, destination) })
	})
}

// lowWaterMark returns the earliest LSN of log entries which should be kept in the database. Any log entries LESS than
// this mark are removed.
func (mgr *TransactionManager) lowWaterMark() storage.LSN {
	mgr.mutex.Lock()
	defer mgr.mutex.Unlock()

	// Greater than the maximum position of any consumer.
	minConsumed := mgr.appliedLSN + 1

	if mgr.consumer != nil {
		// Position is the last acknowledged LSN, this is eligible for pruning.
		// lowWaterMark returns the lowest LSN that cannot be pruned, so add one.
		pos := mgr.consumerPos.getPosition() + 1
		if pos < minConsumed {
			minConsumed = pos
		}
	}

	elm := mgr.committedEntries.Front()
	if elm == nil {
		return minConsumed
	}

	committed := elm.Value.(*committedEntry).lsn
	if minConsumed < committed {
		return minConsumed
	}

	return committed
}

// updateCommittedEntry updates the reader counter of the committed entry of the snapshot that this transaction depends on.
func (mgr *TransactionManager) updateCommittedEntry(snapshotLSN storage.LSN) *committedEntry {
	// Since the goroutine doing this is holding the lock, the snapshotLSN shouldn't change and no new transactions
	// can be committed or added. That should guarantee .Back() is always the latest transaction and the one we're
	// using to base our snapshot on.
	if elm := mgr.committedEntries.Back(); elm != nil {
		entry := elm.Value.(*committedEntry)
		entry.snapshotReaders++
		return entry
	}

	entry := &committedEntry{
		lsn:             snapshotLSN,
		snapshotReaders: 1,
	}

	mgr.committedEntries.PushBack(entry)

	return entry
}

// walkCommittedEntries walks all committed entries after input transaction's snapshot LSN. It loads the content of the
// entry from disk and triggers the callback with entry content.
func (mgr *TransactionManager) walkCommittedEntries(transaction *Transaction, callback func(*gitalypb.LogEntry, map[git.ObjectID]struct{}) error) error {
	for elm := mgr.committedEntries.Front(); elm != nil; elm = elm.Next() {
		committed := elm.Value.(*committedEntry)
		if committed.lsn <= transaction.snapshotLSN {
			continue
		}
		entry, err := mgr.readLogEntry(committed.lsn)
		if err != nil {
			return errCommittedEntryGone
		}
		// Transaction manager works on the partition level, including a repository and all of its pool
		// member repositories (if any). We need to filter log entries of the repository this
		// transaction targets.
		if entry.GetRelativePath() != transaction.relativePath {
			continue
		}
		if err := callback(entry, committed.objectDependencies); err != nil {
			return fmt.Errorf("callback: %w", err)
		}
	}
	return nil
}

// cleanCommittedEntry reduces the snapshot readers counter of the committed entry. It also removes entries with no more
// readers at the head of the list.
func (mgr *TransactionManager) cleanCommittedEntry(entry *committedEntry) bool {
	entry.snapshotReaders--

	removedAnyEntry := false
	elm := mgr.committedEntries.Front()
	for elm != nil {
		front := elm.Value.(*committedEntry)
		if front.snapshotReaders > 0 {
			// If the first entry had still some snapshot readers, that means
			// our transaction was not the oldest reader. We can't remove any entries
			// as they'll still be needed for conflict checking the older transactions.
			return removedAnyEntry
		}

		mgr.committedEntries.Remove(elm)
		removedAnyEntry = true
		elm = mgr.committedEntries.Front()
	}
	return removedAnyEntry
}
