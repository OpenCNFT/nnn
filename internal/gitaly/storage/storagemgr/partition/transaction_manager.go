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
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/prometheus/client_golang/prometheus"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/gitcmd"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/housekeeping"
	housekeepingcfg "gitlab.com/gitlab-org/gitaly/v16/internal/git/housekeeping/config"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/stats"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/updateref"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/repoutil"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/gitstorage"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/keyvalue"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/mode"
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

	// errWritableAllRepository is returned when a transaction is started with
	// no relative path filter specified and is not read-only. Transactions do
	// not currently support writing to multiple repositories and so a writable
	// transaction without a specified target relative path would be ambiguous.
	errWritableAllRepository = errors.New("cannot start writable all repository transaction")

	// keyAppliedLSN is the database key storing a partition's last applied log entry's LSN.
	keyAppliedLSN = []byte("applied_lsn")
	// keyCommittedLSN is the database key storing a partition's last committed log entry's LSN.
	keyCommittedLSN = []byte("committed_lsn")
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

// ReferenceVerificationError is returned when a reference's old OID did not match the expected.
type ReferenceVerificationError struct {
	// ReferenceName is the name of the reference that failed verification.
	ReferenceName git.ReferenceName
	// ExpectedOldOID is the OID the reference was expected to point to.
	ExpectedOldOID git.ObjectID
	// ActualOldOID is the OID the reference actually pointed to.
	ActualOldOID git.ObjectID
	// NewOID is the OID the reference was attempted to be pointed to.
	NewOID git.ObjectID
}

// Error returns the formatted error string.
func (err ReferenceVerificationError) Error() string {
	return fmt.Sprintf("expected %q to point to %q but it pointed to %q", err.ReferenceName, err.ExpectedOldOID, err.ActualOldOID)
}

// repositoryCreation models a repository creation in a transaction.
type repositoryCreation struct {
	// objectHash defines the object format the repository is created with.
	objectHash git.ObjectHash
}

// runHousekeeping models housekeeping tasks. It is supposed to handle housekeeping tasks for repositories
// such as the cleanup of unneeded files and optimizations for the repository's data structures.
type runHousekeeping struct {
	packRefs          *runPackRefs
	repack            *runRepack
	writeCommitGraphs *writeCommitGraphs
}

// runPackRefs models refs packing housekeeping task. It packs heads and tags for efficient repository access.
type runPackRefs struct {
	// PrunedRefs contain a list of references pruned by the `git-pack-refs` command. They are used
	// for comparing to the ref list of the destination repository
	PrunedRefs map[git.ReferenceName]struct{}
	// reftablesBefore contains the data in 'tables.list' before the compaction. This is used to
	// compare with the destination repositories 'tables.list'.
	reftablesBefore []string
	// reftablesAfter contains the data in 'tables.list' after the compaction. This is used for
	// generating the combined 'tables.list' during verification.
	reftablesAfter []string
}

// runRepack models repack housekeeping task. We support multiple repacking strategies. At this stage, the outside
// scheduler determines which strategy to use. The transaction manager is responsible for executing it. In the future,
// we want to make housekeeping smarter by migrating housekeeping scheduling responsibility to this manager. That work
// is tracked in https://gitlab.com/gitlab-org/gitaly/-/issues/5709.
type runRepack struct {
	// config tells which strategy and baggaged options.
	config       housekeepingcfg.RepackObjectsConfig
	isFullRepack bool
	// newFiles contains the list of new packfiles to be applied into the destination repository.
	newFiles []string
	// deletedFiles contains the list of packfiles that should be removed in the destination repository. The repacking
	// command runs in the snapshot repository for a significant amount of time. Meanwhile, other requests
	// containing new objects can land and be applied before housekeeping task finishes. So, we keep a snapshot of
	// the packfile structure before and after running the task. When the manager applies the task, it targets those
	// known files only. Other packfiles can still co-exist along side with the resulting repacked packfile(s).
	deletedFiles []string
}

// writeCommitGraphs models a commit graph update.
type writeCommitGraphs struct {
	// config includes the configs for writing commit graph.
	config housekeepingcfg.WriteCommitGraphConfig
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
	finish func() error
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
	walEntry                 *wal.Entry
	skipVerificationFailures bool
	initialReferenceValues   map[git.ReferenceName]git.Reference
	referenceUpdates         []git.ReferenceUpdates
	customHooksUpdated       bool
	repositoryCreation       *repositoryCreation
	deleteRepository         bool
	includedObjects          map[git.ObjectID]struct{}
	runHousekeeping          *runHousekeeping
	alternateUpdated         bool

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
		snapshotLSN:  mgr.committedLSN,
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

	txn.finish = func() error {
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

	defer func() {
		if returnedErr != nil {
			if err := txn.finish(); err != nil {
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

	return txn.finish()
}

// SnapshotLSN returns the LSN of the Transaction's read snapshot.
func (txn *Transaction) SnapshotLSN() storage.LSN {
	return txn.snapshotLSN
}

// Root returns the path to the read snapshot.
func (txn *Transaction) Root() string {
	return txn.snapshot.Root()
}

// SkipVerificationFailures configures the transaction to skip reference updates that fail verification.
// If a reference update fails verification with this set, the update is dropped from the transaction but
// other successful reference updates will be made. By default, the entire transaction is aborted if a
// reference fails verification.
//
// The default behavior models `git push --atomic`. Toggling this option models the behavior without
// the `--atomic` flag.
func (txn *Transaction) SkipVerificationFailures() {
	txn.skipVerificationFailures = true
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
func (txn *Transaction) UpdateReferences(updates git.ReferenceUpdates) {
	u := git.ReferenceUpdates{}

	for reference, update := range updates {
		oldOID := update.OldOID
		oldTarget := update.OldTarget

		if initialValue, ok := txn.initialReferenceValues[reference]; ok {
			if !initialValue.IsSymbolic {
				oldOID = git.ObjectID(initialValue.Target)
			} else {
				oldTarget = git.ReferenceName(initialValue.Target)
			}
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
	txn.referenceUpdates = append(txn.referenceUpdates, u)
}

// flattenReferenceTransactions flattens the recorded reference transactions by dropping
// all intermediate states. The returned git.ReferenceUpdates contains the reference changes
// with the OldOID set to the reference's value at the beginning of the transaction, and the
// NewOID set to the reference's final value after all of the changes.
func (txn *Transaction) flattenReferenceTransactions() git.ReferenceUpdates {
	flattenedUpdates := git.ReferenceUpdates{}
	for _, updates := range txn.referenceUpdates {
		for reference, update := range updates {
			u := git.ReferenceUpdate{
				OldOID:    update.OldOID,
				NewOID:    update.NewOID,
				OldTarget: update.OldTarget,
				NewTarget: update.NewTarget,
			}

			if previousUpdate, ok := flattenedUpdates[reference]; ok {
				if previousUpdate.OldOID != "" {
					u.OldOID = previousUpdate.OldOID
				}
				if previousUpdate.OldTarget != "" {
					u.OldTarget = previousUpdate.OldTarget
				}
			}

			flattenedUpdates[reference] = u
		}
	}

	return flattenedUpdates
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
//  2. The transaction is committed to the write-ahead log. Once the write has been logged, it is effectively
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
	// mutex guards access to snapshotLocks and committedLSN. These fields are accessed by both
	// Run and Begin which are ran in different goroutines.
	mutex sync.Mutex

	// snapshotLocks contains state used for synchronizing snapshotters with the log application. The
	// lock is released after the corresponding log entry is applied.
	snapshotLocks map[storage.LSN]*snapshotLock
	// snapshotManager is responsible for creation and management of file system snapshots.
	snapshotManager *snapshot.Manager

	// ┌─ oldestLSN                    ┌─ committedLSN
	// ⧅ ⧅ ⧅ ⧅ ⧅ ⧅ ⧅ ■ ■ ■ ■ ■ ■ ■ ■ ■ ■ □ □ □ □ □ □ □
	//               └─ appliedLSN                   └─ appendedLSN
	//
	// appendedLSN holds the LSN of the last log entry emitted by the current node but not yet acknowledged by other
	// nodes. After so, the committedLSN is increment respectively and catches up with appendedLSN. If Raft is not
	// enabled or functions as a single-node cluster, committedLSN is increment instantly.
	appendedLSN storage.LSN
	// committedLSN holds the LSN of the last log entry committed to the partition's write-ahead log. A log entry is
	// considered to be committed if it's accepted by the majority of cluster members. Eventually, it will be
	// applied by all cluster members.
	committedLSN storage.LSN
	// appliedLSN holds the LSN of the last log entry applied to the partition.
	appliedLSN storage.LSN
	// oldestLSN holds the LSN of the head of log entries which is still kept in the database. The manager keeps
	// them because they are still referred by a transaction.
	oldestLSN storage.LSN

	// awaitingTransactions contains transactions waiting for their log entry to be applied to
	// the partition. It's keyed by the LSN the transaction is waiting to be applied and the
	// value is the resultChannel that is waiting the result.
	awaitingTransactions map[storage.LSN]resultChannel

	// appendedEntries keeps track of appended but not-yet committed entries. After an entry is committed, it is
	// removed from this map. This provides quick reference to those entries.
	appendedEntries map[storage.LSN]*gitalypb.LogEntry
	// committedEntries keeps some latest committed log entries around. Some types of transactions, such as
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
	beforeAppendLogEntry      func(storage.LSN)
	beforeCommitLogEntry      func(storage.LSN)
	beforeApplyLogEntry       func(storage.LSN)
	beforeStoreAppliedLSN     func(storage.LSN)
	beforeDeleteLogEntryFiles func(storage.LSN)
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
		stateDirectory:       stateDir,
		stagingDirectory:     stagingDir,
		awaitingTransactions: make(map[storage.LSN]resultChannel),
		appendedEntries:      map[storage.LSN]*gitalypb.LogEntry{},
		committedEntries:     list.New(),
		metrics:              metrics,
		consumer:             consumer,
		consumerPos:          consumerPos,
		acknowledgedQueue:    make(chan struct{}, 1),

		testHooks: testHooks{
			beforeInitialization:      func() {},
			beforeAppendLogEntry:      func(storage.LSN) {},
			beforeCommitLogEntry:      func(storage.LSN) {},
			beforeApplyLogEntry:       func(storage.LSN) {},
			beforeStoreAppliedLSN:     func(storage.LSN) {},
			beforeDeleteLogEntryFiles: func(storage.LSN) {},
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

	// Create a directory to store all staging files.
	if err := os.Mkdir(transaction.walFilesPath(), mode.Directory); err != nil {
		return fmt.Errorf("create wal files directory: %w", err)
	}

	transaction.walEntry = wal.NewEntry(transaction.walFilesPath())

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

		if err := transaction.walEntry.RecordRepositoryCreation(
			transaction.snapshot.Root(),
			transaction.relativePath,
		); err != nil {
			return fmt.Errorf("record repository creation: %w", err)
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
	// we don't end up syncing the potentially very large file to disk when we're committing the
	// log entry.
	preImagePackedRefsInode, err := getInode(transaction.originalPackedRefsFilePath())
	if err != nil {
		return fmt.Errorf("get pre-image packed-refs inode: %w", err)
	}

	postImagePackedRefsPath := mgr.getAbsolutePath(
		transaction.snapshot.RelativePath(transaction.relativePath), "packed-refs",
	)
	postImagePackedRefsInode, err := getInode(postImagePackedRefsPath)
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

// prepareHousekeeping composes and prepares necessary steps on the staging repository before the changes are staged and
// applied. All commands run in the scope of the staging repository. Thus, we can avoid any impact on other concurrent
// transactions.
func (mgr *TransactionManager) prepareHousekeeping(ctx context.Context, transaction *Transaction) error {
	if transaction.runHousekeeping == nil {
		return nil
	}

	span, ctx := tracing.StartSpanIfHasParent(ctx, "transaction.prepareHousekeeping", nil)
	defer span.Finish()

	finishTimer := mgr.metrics.housekeeping.ReportTaskLatency("total", "prepare")
	defer finishTimer()

	if err := mgr.preparePackRefs(ctx, transaction); err != nil {
		return err
	}
	if err := mgr.prepareRepacking(ctx, transaction); err != nil {
		return err
	}
	if err := mgr.prepareCommitGraphs(ctx, transaction); err != nil {
		return err
	}
	return nil
}

// preparePackRefs runs pack refs on the repository after detecting
// its reference backend type.
func (mgr *TransactionManager) preparePackRefs(ctx context.Context, transaction *Transaction) error {
	defer trace.StartRegion(ctx, "preparePackRefs").End()

	if transaction.runHousekeeping.packRefs == nil {
		return nil
	}

	span, ctx := tracing.StartSpanIfHasParent(ctx, "transaction.preparePackRefs", nil)
	defer span.Finish()

	finishTimer := mgr.metrics.housekeeping.ReportTaskLatency("pack-refs", "prepare")
	defer finishTimer()

	refBackend, err := transaction.snapshotRepository.ReferenceBackend(ctx)
	if err != nil {
		return fmt.Errorf("reference backend: %w", err)
	}

	if refBackend == git.ReferenceBackendReftables {
		if err = mgr.preparePackRefsReftable(ctx, transaction); err != nil {
			return fmt.Errorf("reftable backend: %w", err)
		}
		return nil
	}

	if err = mgr.preparePackRefsFiles(ctx, transaction); err != nil {
		return fmt.Errorf("files backend: %w", err)
	}
	return nil
}

// preparePackRefsReftable is used to prepare compaction for reftables.
//
// The flow here is to find the delta of tables modified post compactions. We note the
// list of tables which were deleted and which were added. In the verification stage,
// we use this information to finally create the modified tables.list. Which is also
// why we don't track 'tables.list' operation here.
func (mgr *TransactionManager) preparePackRefsReftable(ctx context.Context, transaction *Transaction) error {
	runPackRefs := transaction.runHousekeeping.packRefs
	repoPath := mgr.getAbsolutePath(transaction.snapshotRepository.GetRelativePath())

	tablesListPre, err := git.ReadReftablesList(repoPath)
	if err != nil {
		return fmt.Errorf("reading tables.list pre-compaction: %w", err)
	}

	// Execute git-pack-refs command. The command runs in the scope of the snapshot repository. Thus, we can
	// let it prune the ref references without causing any impact to other concurrent transactions.
	var stderr bytes.Buffer
	if err := transaction.snapshotRepository.ExecAndWait(ctx, gitcmd.Command{
		Name: "pack-refs",
		// By using the '--auto' flag, we ensure that git uses the best heuristic
		// for compaction. For reftables, it currently uses a geometric progression.
		// This ensures we don't keep compacting unnecessarily to a single file.
		Flags: []gitcmd.Option{gitcmd.Flag{Name: "--auto"}},
	}, gitcmd.WithStderr(&stderr)); err != nil {
		return structerr.New("exec pack-refs: %w", err).WithMetadata("stderr", stderr.String())
	}

	tablesListPost, err := git.ReadReftablesList(repoPath)
	if err != nil {
		return fmt.Errorf("reading tables.list post-compaction: %w", err)
	}

	// If there are no changes after compaction, we don't need to log anything.
	if slices.Equal(tablesListPre, tablesListPost) {
		return nil
	}

	tablesPostMap := make(map[string]struct{})
	for _, table := range tablesListPost {
		tablesPostMap[table] = struct{}{}
	}

	for _, table := range tablesListPre {
		if _, ok := tablesPostMap[table]; !ok {
			// If the table no longer exists, we remove it.
			transaction.walEntry.RecordDirectoryEntryRemoval(
				filepath.Join(transaction.relativePath, "reftable", table),
			)
		} else {
			// If the table exists post compaction too, remove it from the
			// map, since we don't want to record an existing table.
			delete(tablesPostMap, table)
		}
	}

	for file := range tablesPostMap {
		// The remaining tables in tableListPost are new tables
		// which need to be recorded.
		if err := transaction.walEntry.RecordFileCreation(
			filepath.Join(repoPath, "reftable", file),
			filepath.Join(transaction.relativePath, "reftable", file),
		); err != nil {
			return fmt.Errorf("creating new table: %w", err)
		}
	}

	runPackRefs.reftablesAfter = tablesListPost
	runPackRefs.reftablesBefore = tablesListPre

	return nil
}

// preparePackRefsFiles runs git-pack-refs command against the snapshot repository. It collects the resulting packed-refs
// file and the list of pruned references. Unfortunately, git-pack-refs doesn't output which refs are pruned. So, we
// performed two ref walkings before and after running the command. The difference between the two walks is the list of
// pruned refs. This workaround works but is not performant on large repositories with huge amount of loose references.
// Smaller repositories or ones that run housekeeping frequent won't have this issue.
// The work of adding pruned refs dump to `git-pack-refs` is tracked here:
// https://gitlab.com/gitlab-org/git/-/issues/222
func (mgr *TransactionManager) preparePackRefsFiles(ctx context.Context, transaction *Transaction) error {
	runPackRefs := transaction.runHousekeeping.packRefs
	repoPath := mgr.getAbsolutePath(transaction.snapshotRepository.GetRelativePath())

	if err := mgr.removePackedRefsLocks(ctx, repoPath); err != nil {
		return fmt.Errorf("remove stale packed-refs locks: %w", err)
	}
	// First walk to collect the list of loose refs.
	looseReferences := make(map[git.ReferenceName]struct{})
	if err := filepath.WalkDir(filepath.Join(repoPath, "refs"), func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !entry.IsDir() {
			// Get fully qualified refs.
			ref, err := filepath.Rel(repoPath, path)
			if err != nil {
				return fmt.Errorf("extracting ref name: %w", err)
			}
			looseReferences[git.ReferenceName(ref)] = struct{}{}
		}
		return nil
	}); err != nil {
		return fmt.Errorf("initial walking refs directory: %w", err)
	}

	// Execute git-pack-refs command. The command runs in the scope of the snapshot repository. Thus, we can
	// let it prune the ref references without causing any impact to other concurrent transactions.
	var stderr bytes.Buffer
	if err := transaction.snapshotRepository.ExecAndWait(ctx, gitcmd.Command{
		Name:  "pack-refs",
		Flags: []gitcmd.Option{gitcmd.Flag{Name: "--all"}},
	}, gitcmd.WithStderr(&stderr)); err != nil {
		return structerr.New("exec pack-refs: %w", err).WithMetadata("stderr", stderr.String())
	}

	// Copy the resulting packed-refs file to the WAL directory.
	if err := os.Link(
		filepath.Join(filepath.Join(repoPath, "packed-refs")),
		filepath.Join(transaction.walFilesPath(), "packed-refs"),
	); err != nil {
		return fmt.Errorf("copying packed-refs file to WAL directory: %w", err)
	}

	// Second walk and compare with the initial list of loose references. Any disappeared refs are pruned.
	for ref := range looseReferences {
		_, err := os.Stat(filepath.Join(repoPath, ref.String()))
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				runPackRefs.PrunedRefs[ref] = struct{}{}
			} else {
				return fmt.Errorf("second walk refs directory: %w", err)
			}
		}
	}

	return nil
}

// prepareRepacking runs git-repack(1) command against the snapshot repository using desired repacking strategy. Each
// strategy has a different cost and effect corresponding to scheduling frequency.
// - IncrementalWithUnreachable: pack all loose objects into one packfile. This strategy is a no-op because all new
// objects regardless of their reachablity status are packed by default by the manager.
// - Geometric: merge all packs together with geometric repacking. This is expensive or cheap depending on which packs
// get merged. No need for a connectivity check.
// - FullWithUnreachable: merge all packs into one but keep unreachable objects. This is more expensive but we don't
// take connectivity into account. This strategy is essential for object pool. As we cannot prune objects in a pool,
// packing them into one single packfile boosts its performance.
// - FullWithCruft: Merge all packs into one and prune unreachable objects. It is the most effective, but yet costly
// strategy. We cannot run this type of task frequently on a large repository. This strategy is handled as a full
// repacking without cruft because we don't need object expiry.
// Before the command runs, we capture a snapshot of existing packfiles. After the command finishes, we re-capture the
// list and extract the list of to-be-updated packfiles. This practice is to prevent repacking task from deleting
// packfiles of other concurrent updates at the applying phase.
func (mgr *TransactionManager) prepareRepacking(ctx context.Context, transaction *Transaction) error {
	defer trace.StartRegion(ctx, "prepareRepacking").End()

	if transaction.runHousekeeping.repack == nil {
		return nil
	}
	if !transaction.repositoryTarget() {
		return errRelativePathNotSet
	}

	span, ctx := tracing.StartSpanIfHasParent(ctx, "transaction.prepareRepacking", nil)
	defer span.Finish()

	finishTimer := mgr.metrics.housekeeping.ReportTaskLatency("repack", "prepare")
	defer finishTimer()

	var err error
	repack := transaction.runHousekeeping.repack

	// Build a working repository pointing to snapshot repository. Housekeeping task can access the repository
	// without the needs for quarantine.
	workingRepository := mgr.repositoryFactory.Build(transaction.snapshot.RelativePath(transaction.relativePath))
	repoPath := mgr.getAbsolutePath(workingRepository.GetRelativePath())

	if repack.isFullRepack, err = housekeeping.ValidateRepacking(repack.config); err != nil {
		return fmt.Errorf("validating repacking: %w", err)
	}

	if repack.config.Strategy == housekeepingcfg.RepackObjectsStrategyIncrementalWithUnreachable {
		// Once the transaction manager has been applied and at least one complete repack has occurred, there
		// should be no loose unreachable objects remaining in the repository. When the transaction manager
		// processes a change, it consolidates all unreachable objects and objects about to become reachable
		// into a new packfile, which is then placed in the repository. As a result, unreachable objects may
		// still exist but are confined to packfiles. These will eventually be cleaned up during a full repack.
		// In the interim, geometric repacking is utilized to optimize the structure of packfiles for faster
		// access. Therefore, this operation is effectively a no-op. However, we maintain it for the sake of
		// backward compatibility with the existing housekeeping scheduler.
		return errRepackNotSupportedStrategy
	}

	// Capture the list of packfiles and their baggages before repacking.
	beforeFiles, err := mgr.collectPackfiles(ctx, repoPath)
	if err != nil {
		return fmt.Errorf("collecting existing packfiles: %w", err)
	}

	switch repack.config.Strategy {
	case housekeepingcfg.RepackObjectsStrategyGeometric:
		// Geometric repacking rearranges the list of packfiles according to a geometric progression. This process
		// does not consider object reachability. Since all unreachable objects remain within small packfiles,
		// they become included in the newly created packfiles. Geometric repacking does not prune any objects.
		if err := housekeeping.PerformGeometricRepacking(ctx, workingRepository, repack.config); err != nil {
			return fmt.Errorf("perform geometric repacking: %w", err)
		}
	case housekeepingcfg.RepackObjectsStrategyFullWithUnreachable:
		// Git does not pack loose unreachable objects if there are no existing packs in the repository.
		// Perform an incremental repack first. This ensures all loose object are part of a pack and will be
		// included in the full pack we're about to build. This allows us to remove the loose objects from the
		// repository when applying the pack without losing any objects.
		//
		// Issue: https://gitlab.com/gitlab-org/git/-/issues/336
		if err := housekeeping.PerformIncrementalRepackingWithUnreachable(ctx, workingRepository); err != nil {
			return fmt.Errorf("perform geometric repacking: %w", err)
		}

		// This strategy merges all packfiles into a single packfile, simultaneously removing any loose objects
		// if present. Unreachable objects are then appended to the end of this unified packfile. Although the
		// `git-repack(1)` command does not offer an option to specifically pack loose unreachable objects, this
		// is not an issue because the transaction manager already ensures that unreachable objects are
		// contained within packfiles. Therefore, this strategy effectively consolidates all packfiles into a
		// single one. Adopting this strategy is crucial for alternates, as it ensures that we can manage
		// objects within an object pool without the capability to prune them.
		if err := housekeeping.PerformFullRepackingWithUnreachable(ctx, workingRepository, repack.config); err != nil {
			return err
		}
	case housekeepingcfg.RepackObjectsStrategyFullWithCruft:
		// Both of above strategies don't prune unreachable objects. They re-organize the objects between
		// packfiles. In the traditional housekeeping, the manager gets rid of unreachable objects via full
		// repacking with cruft. It pushes all unreachable objects to a cruft packfile and keeps track of each
		// object mtimes. All unreachable objects exceeding a grace period are cleaned up. The grace period is
		// to ensure the housekeeping doesn't delete a to-be-reachable object accidentally, for example when GC
		// runs while a concurrent push is being processed.
		// The transaction manager handles concurrent requests very differently from the original git way. Each
		// request runs on a snapshot repository and the results are collected in the form of packfiles. Those
		// packfiles contain resulting reachable and unreachable objects. As a result, we don't need to take
		// object expiry nor curft pack into account. This operation triggers a normal full repack without
		// cruft packing.
		// Afterward, packed unreachable objects are removed. During migration to transaction system, there
		// might be some loose unreachable objects. They will eventually be packed via either of the above tasks.
		if err := housekeeping.PerformRepack(ctx, workingRepository, repack.config,
			// Do a full repack. By using `-a` instead of `-A` we will immediately discard unreachable
			// objects instead of exploding them into loose objects.
			gitcmd.Flag{Name: "-a"},
			// Don't include objects part of alternate.
			gitcmd.Flag{Name: "-l"},
			// Delete loose objects made redundant by this repack and redundant packfiles.
			gitcmd.Flag{Name: "-d"},
		); err != nil {
			return err
		}
	}

	// Re-capture the list of packfiles and their baggages after repacking.
	afterFiles, err := mgr.collectPackfiles(ctx, repoPath)
	if err != nil {
		return fmt.Errorf("collecting new packfiles: %w", err)
	}

	for file := range beforeFiles {
		// We delete the files only if it's missing from the before set.
		if _, exist := afterFiles[file]; !exist {
			repack.deletedFiles = append(repack.deletedFiles, file)
		}
	}

	for file := range afterFiles {
		// Similarly, we don't need to link existing packfiles.
		if _, exist := beforeFiles[file]; !exist {
			repack.newFiles = append(repack.newFiles, file)
			if err := os.Link(
				filepath.Join(filepath.Join(repoPath, "objects", "pack"), file),
				filepath.Join(transaction.walFilesPath(), file),
			); err != nil {
				return fmt.Errorf("copying packfiles to WAL directory: %w", err)
			}
		}
	}

	return nil
}

// prepareCommitGraphs updates the commit-graph in the snapshot repository. It then hard-links the
// graphs to the staging repository so it can be applied by the transaction manager.
func (mgr *TransactionManager) prepareCommitGraphs(ctx context.Context, transaction *Transaction) error {
	defer trace.StartRegion(ctx, "prepareCommitGraphs").End()

	if transaction.runHousekeeping.writeCommitGraphs == nil {
		return nil
	}
	if !transaction.repositoryTarget() {
		return errRelativePathNotSet
	}

	span, ctx := tracing.StartSpanIfHasParent(ctx, "transaction.prepareCommitGraphs", nil)
	defer span.Finish()

	finishTimer := mgr.metrics.housekeeping.ReportTaskLatency("commit-graph", "prepare")
	defer finishTimer()

	workingRepository := mgr.repositoryFactory.Build(transaction.snapshot.RelativePath(transaction.relativePath))
	repoPath := mgr.getAbsolutePath(workingRepository.GetRelativePath())

	if err := housekeeping.WriteCommitGraph(ctx, workingRepository, transaction.runHousekeeping.writeCommitGraphs.config); err != nil {
		return fmt.Errorf("re-writing commit graph: %w", err)
	}

	commitGraphsDir := filepath.Join(repoPath, "objects", "info", "commit-graphs")
	if graphEntries, err := os.ReadDir(commitGraphsDir); err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("reading commit-graphs directory: %w", err)
		}
	} else if len(graphEntries) > 0 {
		walGraphsDir := filepath.Join(transaction.walFilesPath(), "commit-graphs")
		if err := os.Mkdir(walGraphsDir, mode.Directory); err != nil {
			return fmt.Errorf("creating commit-graphs dir in WAL dir: %w", err)
		}
		for _, entry := range graphEntries {
			if err := os.Link(
				filepath.Join(commitGraphsDir, entry.Name()),
				filepath.Join(walGraphsDir, entry.Name()),
			); err != nil {
				return fmt.Errorf("linking commit-graph entry to WAL dir: %w", err)
			}
		}
	}

	return nil
}

// packfileExtensions contains the packfile extension and its dependencies. They will be collected after running
// repacking command.
var packfileExtensions = map[string]struct{}{
	"multi-pack-index": {},
	".pack":            {},
	".idx":             {},
	".rev":             {},
	".mtimes":          {},
	".bitmap":          {},
}

// collectPackfiles collects the list of packfiles and their luggage files.
func (mgr *TransactionManager) collectPackfiles(ctx context.Context, repoPath string) (map[string]struct{}, error) {
	files, err := os.ReadDir(filepath.Join(repoPath, "objects", "pack"))
	if err != nil {
		return nil, fmt.Errorf("reading objects/pack dir: %w", err)
	}

	// Filter packfiles and relevant files.
	collectedFiles := make(map[string]struct{})
	for _, file := range files {
		// objects/pack directory should not include any sub-directory. We can simply ignore them.
		if file.IsDir() {
			continue
		}
		for extension := range packfileExtensions {
			if strings.HasSuffix(file.Name(), extension) {
				collectedFiles[file.Name()] = struct{}{}
			}
		}
	}

	return collectedFiles, nil
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

// Run starts the transaction processing. On start up Run loads the indexes of the last committed and applied
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

	// Defer the Stop in order to release all on-going Commit calls in case of error.
	defer close(mgr.closed)
	defer mgr.Close()
	defer mgr.testHooks.beforeRunExiting()

	if err := mgr.initialize(ctx); err != nil {
		return fmt.Errorf("initialize: %w", err)
	}

	for {
		// We prioritize applying committed log entries to the partition first.
		if mgr.appliedLSN < mgr.committedLSN {
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

		// The Transaction does not finish itself anymore once it has been admitted for
		// processing. This avoids the Transaction concurrently removing the staged state
		// while the manager is still operating on it. We thus need to defer its finishing.
		defer func() {
			if err := transaction.finish(); err != nil && returnedErr == nil {
				returnedErr = fmt.Errorf("finish transaction: %w", err)
			}
		}()
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

		if transaction.repositoryCreation == nil && transaction.runHousekeeping == nil && !transaction.deleteRepository {
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

		return mgr.proposeLogEntry(ctx, transaction.objectDependencies, logEntry, transaction.walFilesPath())
	}(); err != nil {
		transaction.result <- err
		return nil
	}

	mgr.awaitingTransactions[mgr.committedLSN] = transaction.result

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

// initialize initializes the TransactionManager's state from the database. It loads the committed and the applied
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

	// The LSN of the last committed log entry is determined from the LSN of the latest entry in the log and
	// the latest applied log entry. The manager also keeps track of committed entries and reserves them until there
	// is no transaction refers them. It's possible there are some left-over entries in the database because a
	// transaction can hold the entry stubbornly. So, the manager could not clean them up in the last session.
	//
	// ┌─ oldestLSN                    ┌─ committedLSN
	// ⧅ ⧅ ⧅ ⧅ ⧅ ⧅ ⧅ ■ ■ ■ ■ ■ ■ ■ ■ ■ ■ □ □ □ □ □ □ □
	//               └─ appliedLSN                   └─ appendedLSN
	//
	//
	// oldestLSN is initialized to appliedLSN + 1. If there are no log entries in the log, then everything has been
	// pruned already or there has not been any log entries yet. Setting this +1 avoids trying to clean up log entries
	// that do not exist. If there are some, we'll set oldestLSN to the head of the log below.
	mgr.oldestLSN = mgr.appliedLSN + 1

	// CommittedLSN is loaded from DB. A log entry is appended first and marked as committed later. There's a chance
	// that log entry is never marked as committed. After a restart, especially after a crash, the manager won't be
	// able to tell if it's committed or not. Thus, we need to persist this index.
	// Because index persistence is introduced later, we need to fallback to appliedLSN if that key does not exist
	// in the DB.
	var committedLSN gitalypb.LSN
	if err := mgr.readKey(keyCommittedLSN, &committedLSN); err != nil {
		if !errors.Is(err, badger.ErrKeyNotFound) {
			return fmt.Errorf("read committed LSN: %w", err)
		}
		mgr.committedLSN = mgr.appliedLSN
	} else {
		mgr.committedLSN = storage.LSN(committedLSN.GetValue())
	}

	// appendedLSN is always set to committedLSN after starting. If a log entry hasn't been committed, it could be
	// discarded. Its caller never received the acknowledgement.
	mgr.appendedLSN = mgr.committedLSN

	if logEntries, err := os.ReadDir(walFilesPath(mgr.stateDirectory)); err != nil {
		return fmt.Errorf("read wal directory: %w", err)
	} else if len(logEntries) > 0 {
		// All log entries starting from mgr.committedLSN + 1 are not committed. No reason to keep them around.
		// Returned log entries are sorted in ascending order. We iterate backward and break when the iterating
		// LSN drops below committedLSN.
		for i := len(logEntries) - 1; i >= 0; i-- {
			logEntry := logEntries[i]

			lsn, err := storage.ParseLSN(logEntry.Name())
			if err != nil {
				return fmt.Errorf("parse LSN: %w", err)
			}
			if lsn <= mgr.committedLSN {
				// Found some on-disk log entries older than or equal to committedLSN. They might be
				// referenced by other transactions before restart. Eventually, they'll be removed in
				// the main loop.
				if mgr.oldestLSN, err = storage.ParseLSN(logEntries[0].Name()); err != nil {
					return fmt.Errorf("parse oldest LSN: %w", err)
				}
				break
			}
			if err := mgr.deleteLogEntry(mgr.ctx, lsn); err != nil {
				return fmt.Errorf("cleaning uncommitted log entry: %w", err)
			}
		}
	}

	if mgr.consumer != nil {
		mgr.consumer.NotifyNewTransactions(mgr.storageName, mgr.partitionID, mgr.oldestLSN, mgr.committedLSN)
	}

	// Create a snapshot lock for the applied LSN as it is used for synchronizing
	// the snapshotters with the log application.
	mgr.snapshotLocks[mgr.appliedLSN] = &snapshotLock{applied: make(chan struct{})}
	close(mgr.snapshotLocks[mgr.appliedLSN].applied)

	// Each unapplied log entry should have a snapshot lock as they are created in normal
	// operation when committing a log entry. Recover these entries.
	for i := mgr.appliedLSN + 1; i <= mgr.committedLSN; i++ {
		mgr.snapshotLocks[i] = &snapshotLock{applied: make(chan struct{})}
	}

	if err := mgr.removeStaleWALFiles(ctx, mgr.oldestLSN, mgr.committedLSN); err != nil {
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

// removePackedRefsLocks removes any packed-refs.lock and packed-refs.new files present in the manager's
// repository. No grace period for the locks is given as any lockfiles present must be stale and can be
// safely removed immediately.
func (mgr *TransactionManager) removePackedRefsLocks(ctx context.Context, repositoryPath string) error {
	for _, lock := range []string{".new", ".lock"} {
		lockPath := filepath.Join(repositoryPath, "packed-refs"+lock)

		// We deliberately do not fsync this deletion. Should a crash occur before this is persisted
		// to disk, the restarted transaction manager will simply remove them again.
		if err := os.Remove(lockPath); err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				continue
			}

			return fmt.Errorf("remove %v: %w", lockPath, err)
		}
	}

	return nil
}

// removeStaleWALFiles removes files from the log directory that have no associated log entry.
// Such files can be left around if transaction's files were moved in place successfully
// but the manager was interrupted before successfully persisting the log entry itself.
// If the manager deletes a log entry successfully from the database but is interrupted before it cleans
// up the associated files, such a directory can also be left at the head of the log.
func (mgr *TransactionManager) removeStaleWALFiles(ctx context.Context, oldestLSN, committedLSN storage.LSN) error {
	needsFsync := false
	for _, possibleStaleFilesPath := range []string{
		// Log entries are pruned one by one. If a write is interrupted, the only possible stale files would be
		// for the log entry preceding the oldest log entry.
		walFilesPathForLSN(mgr.stateDirectory, oldestLSN-1),
		// Log entries are committed one by one to the log. If a write is interrupted, the only possible stale
		// files would be for the next LSN. Remove the files if they exist.
		walFilesPathForLSN(mgr.stateDirectory, committedLSN+1),
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

	flattenedReferenceTransactions := transaction.flattenReferenceTransactions()
	revisions := make([]git.Revision, 0, len(flattenedReferenceTransactions))
	for referenceName := range flattenedReferenceTransactions {
		// Transactions should only stage references with valid names as otherwise Git would already
		// fail when they try to stage them against their snapshot. `update-ref` happily accepts references
		// outside of `refs` directory so such references could theoretically arrive here. We thus sanity
		// check that all references modified are within the refs directory.
		//
		// HEAD is a special case and refers to a default branch update.
		if !strings.HasPrefix(referenceName.String(), "refs/") && referenceName != "HEAD" {
			return nil, InvalidReferenceFormatError{ReferenceName: referenceName}
		}

		revisions = append(revisions, referenceName.Revision())
	}

	droppedReferenceUpdates := map[git.ReferenceName]struct{}{}
	if err := checkObjects(ctx, stagingRepositoryWithQuarantine, revisions, func(revision git.Revision, actualOldTip git.ObjectID) error {
		referenceName := git.ReferenceName(revision)
		update := flattenedReferenceTransactions[referenceName]

		if update.IsRegularUpdate() && update.OldOID != actualOldTip {
			if transaction.skipVerificationFailures {
				droppedReferenceUpdates[referenceName] = struct{}{}
				return nil
			}

			return ReferenceVerificationError{
				ReferenceName:  referenceName,
				ExpectedOldOID: update.OldOID,
				ActualOldOID:   actualOldTip,
				NewOID:         update.NewOID,
			}
		}

		if update.OldOID == update.NewOID && update.OldTarget == update.NewTarget {
			// This was a no-op and doesn't need to be written out. The reference's old value has been
			// verified now to match what is expected.
			droppedReferenceUpdates[referenceName] = struct{}{}
			return nil
		}

		return nil
	}); err != nil {
		return nil, fmt.Errorf("check objects: %w", err)
	}

	var referenceTransactions []*gitalypb.LogEntry_ReferenceTransaction
	for _, updates := range transaction.referenceUpdates {
		changes := make([]*gitalypb.LogEntry_ReferenceTransaction_Change, 0, len(updates))
		for reference, update := range updates {
			if _, ok := droppedReferenceUpdates[reference]; ok {
				continue
			}

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
// before applying the updates. This way the reftable backend will only create new tables
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
	preImagePackedRefsInode, err := getInode(tx.originalPackedRefsFilePath())
	if err != nil {
		return fmt.Errorf("get pre-image packed-refs inode: %w", err)
	}

	// Get the inode of the `packed-refs` file as it is in the snapshot after the transaction. This contains
	// all of the modifications the transaction has performed on it.
	postImagePackedRefsPath := mgr.getAbsolutePath(tx.snapshot.RelativePath(tx.relativePath), "packed-refs")
	postImagePackedRefsInode, err := getInode(postImagePackedRefsPath)
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
	currentPackedRefsInode, err := getInode(currentPackedRefsPath)
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

	for _, referenceTransaction := range referenceTransactions {
		if err := tx.walEntry.RecordReferenceUpdates(ctx,
			mgr.storagePath,
			tx.stagingSnapshot.Prefix(),
			tx.relativePath,
			referenceTransaction,
			objectHash,
			func(changes []*gitalypb.LogEntry_ReferenceTransaction_Change) error {
				return mgr.applyReferenceTransaction(ctx, changes, repo)
			},
		); err != nil {
			return fmt.Errorf("record reference updates: %w", err)
		}
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

	// Check for any concurrent housekeeping between this transaction's snapshot LSN and the latest committed LSN.
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

	repackEntry, err := mgr.verifyRepacking(ctx, transaction)
	if err != nil {
		return nil, fmt.Errorf("verifying repacking: %w", err)
	}

	commitGraphsEntry, err := mgr.verifyCommitGraphs(ctx, transaction)
	if err != nil {
		return nil, fmt.Errorf("verifying commit graph update: %w", err)
	}

	return &gitalypb.LogEntry_Housekeeping{
		PackRefs:          packRefsEntry,
		Repack:            repackEntry,
		WriteCommitGraphs: commitGraphsEntry,
	}, nil
}

// verifyPackRefsReftable verifies if the compaction performed can be safely
// applied to the repository.

// We merge the tables.list generated by our compaction with the existing
// repositories tables.list. Because there could have been new tables after
// we performed compaction.
func (mgr *TransactionManager) verifyPackRefsReftable(transaction *Transaction) (*gitalypb.LogEntry_Housekeeping_PackRefs, error) {
	tables := transaction.runHousekeeping.packRefs.reftablesAfter
	if len(tables) < 1 {
		return nil, nil
	}

	// The tables.list from the snapshot repository should be identical to that of the staging
	// repository. However, concurrent writes might have occurred which wrote new tables to the
	// staging repository. We shouldn't loose that data. So we merge the compacted tables.list
	// with the newer tables from the staging repositories tables.list.
	repoPath := mgr.getAbsolutePath(transaction.stagingRepository.GetRelativePath())
	newTableList, err := git.ReadReftablesList(repoPath)
	if err != nil {
		return nil, fmt.Errorf("reading tables.list: %w", err)
	}

	snapshotRepoPath := mgr.getAbsolutePath(transaction.snapshotRepository.GetRelativePath())

	// tables.list is hard-linked from the repository to the snapshot, we shouldn't
	// directly write to it as we'd modify the original. So let's remove the
	// hard-linked file.
	if err = os.Remove(filepath.Join(snapshotRepoPath, "reftable", "tables.list")); err != nil {
		return nil, fmt.Errorf("removing tables.list: %w", err)
	}

	// We need to merge the tables.list of snapshotRepo with the latest from stagingRepo.
	tablesBefore := transaction.runHousekeeping.packRefs.reftablesBefore
	finalTableList := append(tables, newTableList[len(tablesBefore):]...)

	// Write the updated tables.list so we can add the required operations.
	if err := os.WriteFile(
		filepath.Join(snapshotRepoPath, "reftable", "tables.list"),
		[]byte(strings.Join(finalTableList, "\n")),
		mode.File,
	); err != nil {
		return nil, fmt.Errorf("writing tables.list: %w", err)
	}

	// Add operation to update the tables.list.
	if err := transaction.walEntry.RecordFileUpdate(
		transaction.snapshot.Root(),
		filepath.Join(transaction.relativePath, "reftable", "tables.list"),
	); err != nil {
		return nil, fmt.Errorf("updating tables.list: %w", err)
	}

	return nil, nil
}

// verifyPackRefsFiles verifies if the pack-refs housekeeping task can be logged. Ideally, we can just apply the packed-refs
// file and prune the loose references. Unfortunately, there could be a ref modification between the time the pack-refs
// command runs and the time this transaction is logged. Thus, we need to verify if the transaction conflicts with the
// current state of the repository.
//
// There are three cases when a reference is modified:
// - Reference creation: this is the easiest case. The new reference exists as a loose reference on disk and shadows the
// one in the packed-ref.
// - Reference update: similarly, the loose reference shadows the one in packed-refs with the new OID. However, we need
// to remove it from the list of pruned references. Otherwise, the repository continues to use the old OID.
// - Reference deletion. When a reference is deleted, both loose reference and the entry in the packed-refs file are
// removed. The reflogs are also removed. In addition, we don't use reflogs in Gitaly as core.logAllRefUpdates defaults
// to false in bare repositories. It could of course be that an admin manually enabled it by modifying the config
// on-disk directly. There is no way to extract reference deletion between two states.
//
// In theory, if there is any reference deletion, it can be removed from the packed-refs file. However, it requires
// parsing and regenerating the packed-refs file. So, let's settle down with a conflict error at this point.
func (mgr *TransactionManager) verifyPackRefsFiles(ctx context.Context, transaction *Transaction) (*gitalypb.LogEntry_Housekeeping_PackRefs, error) {
	objectHash, err := transaction.stagingRepository.ObjectHash(ctx)
	if err != nil {
		return nil, fmt.Errorf("object hash: %w", err)
	}
	packRefs := transaction.runHousekeeping.packRefs

	// Check for any concurrent ref deletion between this transaction's snapshot LSN to the end.
	if err := mgr.walkCommittedEntries(transaction, func(entry *gitalypb.LogEntry, objectDependencies map[git.ObjectID]struct{}) error {
		for _, refTransaction := range entry.GetReferenceTransactions() {
			for _, change := range refTransaction.GetChanges() {
				// We handle HEAD updates through the git-update-ref, but since
				// it is not part of the packed-refs file, we don't need to worry about it.
				if bytes.Equal(change.GetReferenceName(), []byte("HEAD")) {
					continue
				}

				if objectHash.IsZeroOID(git.ObjectID(change.GetNewOid())) {
					// Oops, there is a reference deletion. Bail out.
					return errPackRefsConflictRefDeletion
				}
				// Ref update. Remove the updated ref from the list of pruned refs so that the
				// new OID in loose reference shadows the outdated OID in packed-refs.
				delete(packRefs.PrunedRefs, git.ReferenceName(change.GetReferenceName()))
			}
		}
		return nil
	}); err != nil {
		return nil, fmt.Errorf("walking committed entries: %w", err)
	}

	var prunedRefs [][]byte
	for ref := range packRefs.PrunedRefs {
		prunedRefs = append(prunedRefs, []byte(ref))
	}
	return &gitalypb.LogEntry_Housekeeping_PackRefs{
		PrunedRefs: prunedRefs,
	}, nil
}

// verifyPackRefs verifies if the git-pack-refs(1) can be applied without any conflicts.
// It calls the reference backend specific function to handle the core logic.
func (mgr *TransactionManager) verifyPackRefs(ctx context.Context, transaction *Transaction) (*gitalypb.LogEntry_Housekeeping_PackRefs, error) {
	if transaction.runHousekeeping.packRefs == nil {
		return nil, nil
	}

	span, ctx := tracing.StartSpanIfHasParent(ctx, "transaction.verifyPackRefs", nil)
	defer span.Finish()

	finishTimer := mgr.metrics.housekeeping.ReportTaskLatency("pack-refs", "verify")
	defer finishTimer()

	refBackend, err := transaction.stagingRepository.ReferenceBackend(ctx)
	if err != nil {
		return nil, fmt.Errorf("reference backend: %w", err)
	}

	if refBackend == git.ReferenceBackendReftables {
		packRefs, err := mgr.verifyPackRefsReftable(transaction)
		if err != nil {
			return nil, fmt.Errorf("reftable backend: %w", err)
		}
		return packRefs, nil
	}

	packRefs, err := mgr.verifyPackRefsFiles(ctx, transaction)
	if err != nil {
		return nil, fmt.Errorf("files backend: %w", err)
	}
	return packRefs, nil
}

// verifyRepacking checks the object repacking operations for conflicts.
//
// Object repacking without pruning is conflict-free operation. It only rearranges the objects on the disk into
// a more optimal physical format. All objects that other transactions could need are still present in pure repacking
// operations.
//
// Repacking operations that prune unreachable objects from the repository may lead to conflicts. Conflicts may occur
// if concurrent transactions depend on the unreachable objects.
//
// 1. Transactions may point references to the previously unreachable objects and make them reachable.
// 2. Transactions may write new objects that depend on the unreachable objects.
//
// In both cases a pruning operation that removes the objects must be aborted. In the first case, the pruning
// operation would remove reachable objects from the repository and the repository becomes corrupted. In the second case,
// the new objects written into the repository may not be necessarily reachable. Transactions depend on an invariant
// that all objects in the repository are valid. Therefore, we must also reject transactions that attempt to remove
// dependencies of unreachable objects even if such state isn't considered corrupted by Git.
//
// As we don't have a list of pruned objects at hand, the conflicts are identified by checking whether the recorded
// dependencies of a transaction would still exist in the repository after applying the pruning operation.
func (mgr *TransactionManager) verifyRepacking(ctx context.Context, transaction *Transaction) (_ *gitalypb.LogEntry_Housekeeping_Repack, returnedErr error) {
	repack := transaction.runHousekeeping.repack
	if repack == nil {
		return nil, nil
	}

	span, ctx := tracing.StartSpanIfHasParent(ctx, "transaction.verifyRepacking", nil)
	defer span.Finish()

	finishTimer := mgr.metrics.housekeeping.ReportTaskLatency("repack", "verify")
	defer finishTimer()

	// Other strategies re-organize packfiles without pruning unreachable objects. No need to run following
	// expensive verification.
	if repack.config.Strategy != housekeepingcfg.RepackObjectsStrategyFullWithCruft {
		return &gitalypb.LogEntry_Housekeeping_Repack{
			NewFiles:     repack.newFiles,
			DeletedFiles: repack.deletedFiles,
			IsFullRepack: repack.isFullRepack,
		}, nil
	}

	// Setup a working repository of the destination repository and all changes of current transactions. All
	// concurrent changes must land in that repository already.
	snapshot, err := mgr.snapshotManager.GetSnapshot(ctx, []string{transaction.relativePath}, true)
	if err != nil {
		return nil, fmt.Errorf("setting up new snapshot for verifying repacking: %w", err)
	}
	defer func() {
		if err := snapshot.Close(); err != nil {
			returnedErr = errors.Join(returnedErr, fmt.Errorf("close snapshot: %w", err))
		}
	}()

	workingRepository := mgr.repositoryFactory.Build(snapshot.RelativePath(transaction.relativePath))
	workingRepositoryPath, err := workingRepository.Path(ctx)
	if err != nil {
		return nil, fmt.Errorf("getting working repository path: %w", err)
	}

	// Remove loose objects as we'd do on application.
	if err := mgr.pruneLooseObjects(workingRepositoryPath); err != nil {
		return nil, fmt.Errorf("prune loose objects: %w", err)
	}

	// Apply the changes of current transaction.
	if err := mgr.replacePackfiles(ctx, workingRepositoryPath, transaction.walFilesPath(), repack.newFiles, repack.deletedFiles); err != nil {
		return nil, fmt.Errorf("applying packfiles for verifying repacking: %w", err)
	}

	// Apply new commit graph if any. Although commit graph update belongs to another task, a repacking task will
	// result in rewriting the commit graph. It would be nice to apply the commit graph when setting up working
	// repository for repacking task.
	if err := mgr.replaceCommitGraphs(ctx, workingRepositoryPath, transaction.walFilesPath()); err != nil {
		return nil, fmt.Errorf("applying commit graph for verifying repacking: %w", err)
	}

	// Collect object dependencies. All of them should exist in the resulting packfile or new concurrent
	// packfiles while repacking is running.
	objectDependencies := map[git.ObjectID]struct{}{}
	if err := mgr.walkCommittedEntries(transaction, func(entry *gitalypb.LogEntry, txnObjectDependencies map[git.ObjectID]struct{}) error {
		for oid := range txnObjectDependencies {
			objectDependencies[oid] = struct{}{}
		}

		return nil
	}); err != nil {
		return nil, fmt.Errorf("walking committed entries: %w", err)
	}

	if err := mgr.verifyObjectsExist(ctx, workingRepository, objectDependencies); err != nil {
		var errInvalidObject localrepo.InvalidObjectError
		if errors.As(err, &errInvalidObject) {
			return nil, errRepackConflictPrunedObject
		}

		return nil, fmt.Errorf("verify objects exist: %w", err)
	}

	return &gitalypb.LogEntry_Housekeeping_Repack{
		NewFiles:     repack.newFiles,
		DeletedFiles: repack.deletedFiles,
		IsFullRepack: repack.isFullRepack,
	}, nil
}

// verifyCommitGraphs verifies if the commit-graph update is valid. As we replace the whole commit-graph directory by
// the new directory, this step is a no-op now.
func (mgr *TransactionManager) verifyCommitGraphs(ctx context.Context, transaction *Transaction) (*gitalypb.LogEntry_Housekeeping_WriteCommitGraphs, error) {
	if transaction.runHousekeeping.writeCommitGraphs == nil {
		return nil, nil
	}

	finishTimer := mgr.metrics.housekeeping.ReportTaskLatency("commit-graph", "verify")
	defer finishTimer()

	return &gitalypb.LogEntry_Housekeeping_WriteCommitGraphs{}, nil
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

// proposeLogEntry proposes a log etnry of a transaction to the write-ahead log. It first writes the transaction's
// manifest file into the log entry's directory. Second, it sends the log entry to other cluster members if needed.
// Afterwards it moves the log entry's directory from the staging area to its final place in the write-ahead log.
func (mgr *TransactionManager) proposeLogEntry(ctx context.Context, objectDependencies map[git.ObjectID]struct{}, logEntry *gitalypb.LogEntry, logEntryPath string) error {
	nextLSN := mgr.appendedLSN + 1
	if err := mgr.appendLogEntry(ctx, nextLSN, logEntry, logEntryPath); err != nil {
		return fmt.Errorf("append log entry: %w", err)
	}
	if err := mgr.commitLogEntry(ctx, nextLSN, objectDependencies); err != nil {
		return fmt.Errorf("commit log entry: %w", err)
	}
	return nil
}

// appendLogEntry appends the transaction to the write-ahead log.
func (mgr *TransactionManager) appendLogEntry(ctx context.Context, nextLSN storage.LSN, logEntry *gitalypb.LogEntry, logEntryPath string) error {
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

	mgr.testHooks.beforeAppendLogEntry(nextLSN)

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
	mgr.appendedEntries[mgr.appendedLSN] = logEntry
	mgr.mutex.Unlock()

	return nil
}

// commitLogEntry commits the transaction to the write-ahead log.
func (mgr *TransactionManager) commitLogEntry(ctx context.Context, nextLSN storage.LSN, objectDependencies map[git.ObjectID]struct{}) error {
	defer trace.StartRegion(ctx, "commitLogEntry").End()
	mgr.testHooks.beforeCommitLogEntry(nextLSN)

	// Persist committed LSN before updating internal states.
	if err := mgr.storeCommittedLSN(nextLSN); err != nil {
		return fmt.Errorf("persisting committed entry: %w", err)
	}

	mgr.mutex.Lock()
	mgr.committedLSN = nextLSN
	mgr.snapshotLocks[nextLSN] = &snapshotLock{applied: make(chan struct{})}
	if _, exist := mgr.appendedEntries[nextLSN]; !exist {
		mgr.mutex.Unlock()
		return fmt.Errorf("log entry %s not found in the appended list", nextLSN)
	}
	delete(mgr.appendedEntries, nextLSN)
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

	mgr.testHooks.beforeApplyLogEntry(lsn)

	if err := applyOperations(ctx, safe.NewSyncer().Sync, mgr.storagePath, walFilesPathForLSN(mgr.stateDirectory, lsn), logEntry, mgr.db); err != nil {
		return fmt.Errorf("apply operations: %w", err)
	}

	// If the repository is being deleted, just delete it without any other changes given
	// they'd all be removed anyway. Reapplying the other changes after a crash would also
	// not work if the repository was successfully deleted before the crash.
	if logEntry.GetRepositoryDeletion() == nil {
		if err := mgr.applyHousekeeping(ctx, lsn, logEntry); err != nil {
			return fmt.Errorf("apply housekeeping: %w", err)
		}
	}

	mgr.testHooks.beforeStoreAppliedLSN(lsn)
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

// applyHousekeeping applies housekeeping results to the target repository.
func (mgr *TransactionManager) applyHousekeeping(ctx context.Context, lsn storage.LSN, logEntry *gitalypb.LogEntry) error {
	if logEntry.GetHousekeeping() == nil {
		return nil
	}

	span, _ := tracing.StartSpanIfHasParent(ctx, "transaction.applyHousekeeping", nil)
	defer span.Finish()

	finishTimer := mgr.metrics.housekeeping.ReportTaskLatency("total", "apply")
	defer finishTimer()

	if err := mgr.applyPackRefs(ctx, lsn, logEntry); err != nil {
		return fmt.Errorf("applying pack refs: %w", err)
	}

	if err := mgr.applyRepacking(ctx, lsn, logEntry); err != nil {
		return fmt.Errorf("applying repacking: %w", err)
	}

	if err := mgr.applyCommitGraphs(ctx, lsn, logEntry); err != nil {
		return fmt.Errorf("applying the commit graph: %w", err)
	}

	return nil
}

func (mgr *TransactionManager) applyPackRefs(ctx context.Context, lsn storage.LSN, logEntry *gitalypb.LogEntry) error {
	// For reftables, pack-refs is done via transaction operations, and we keep
	// this nil. So we'd exit early too.
	if logEntry.GetHousekeeping().GetPackRefs() == nil {
		return nil
	}

	span, _ := tracing.StartSpanIfHasParent(ctx, "transaction.applyPackRefs", nil)
	defer span.Finish()

	finishTimer := mgr.metrics.housekeeping.ReportTaskLatency("pack-refs", "apply")
	defer finishTimer()

	repositoryPath := mgr.getAbsolutePath(logEntry.GetRelativePath())
	// Remove packed-refs lock. While we shouldn't be producing any new stale locks, it makes sense to have
	// this for historic state until we're certain none of the repositories contain stale locks anymore.
	// This clean up is not needed afterward.
	if err := mgr.removePackedRefsLocks(ctx, repositoryPath); err != nil {
		return fmt.Errorf("applying pack-refs: %w", err)
	}

	packedRefsPath := filepath.Join(repositoryPath, "packed-refs")
	// Replace the packed-refs file.
	if err := os.Remove(packedRefsPath); err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("removing existing pack-refs: %w", err)
		}
	}
	if err := os.Link(
		filepath.Join(walFilesPathForLSN(mgr.stateDirectory, lsn), "packed-refs"),
		packedRefsPath,
	); err != nil {
		return fmt.Errorf("linking new packed-refs: %w", err)
	}

	modifiedDirs := map[string]struct{}{}
	// Prune loose references. The log entry carries the list of fully qualified references to prune.
	for _, ref := range logEntry.GetHousekeeping().GetPackRefs().GetPrunedRefs() {
		path := filepath.Join(repositoryPath, string(ref))
		if err := os.Remove(path); err != nil {
			if !errors.Is(err, os.ErrNotExist) {
				return structerr.New("pruning loose reference: %w", err).WithMetadata("ref", path)
			}
		}
		modifiedDirs[filepath.Dir(path)] = struct{}{}
	}

	syncer := safe.NewSyncer()
	// Traverse all modified dirs back to the root "refs" dir of the repository. Remove any empty directory
	// along the way. It prevents leaving empty dirs around after a loose ref is pruned. `git-pack-refs`
	// command does dir removal for us, but in staging repository during preparation stage. In the actual
	// repository,  we need to do it ourselves.
	rootRefDir := filepath.Join(repositoryPath, "refs")
	for dir := range modifiedDirs {
		for dir != rootRefDir {
			if isEmpty, err := isDirEmpty(dir); err != nil {
				// If a dir does not exist, it properly means a directory may already be deleted by a
				// previous interrupted attempt on applying the log entry. We simply ignore the error
				// and move up the directory hierarchy.
				if errors.Is(err, fs.ErrNotExist) {
					dir = filepath.Dir(dir)
					continue
				} else {
					return fmt.Errorf("checking empty ref dir: %w", err)
				}
			} else if !isEmpty {
				break
			}

			if err := os.Remove(dir); err != nil {
				return fmt.Errorf("removing empty ref dir: %w", err)
			}
			dir = filepath.Dir(dir)
		}
		// If there is any empty dir along the way, it's removed and dir pointer moves up until the dir
		// is not empty or reaching the root dir. That one should be fsynced to flush the dir removal.
		// If there is no empty dir, it stays at the dir of pruned refs, which also needs a flush.
		if err := syncer.Sync(ctx, dir); err != nil {
			return fmt.Errorf("sync dir: %w", err)
		}
	}

	// Sync the root of the repository to flush packed-refs replacement.
	if err := syncer.SyncParent(ctx, packedRefsPath); err != nil {
		return fmt.Errorf("sync parent: %w", err)
	}
	return nil
}

// applyRepacking applies the new packfile set and removed known pruned packfiles. New packfiles created by concurrent
// changes are kept intact.
func (mgr *TransactionManager) applyRepacking(ctx context.Context, lsn storage.LSN, logEntry *gitalypb.LogEntry) error {
	if logEntry.GetHousekeeping().GetRepack() == nil {
		return nil
	}

	span, _ := tracing.StartSpanIfHasParent(ctx, "transaction.applyRepacking", nil)
	defer span.Finish()

	finishTimer := mgr.metrics.housekeeping.ReportTaskLatency("repack", "apply")
	defer finishTimer()

	repack := logEntry.GetHousekeeping().GetRepack()
	repoPath := mgr.getAbsolutePath(logEntry.GetRelativePath())

	if err := mgr.replacePackfiles(ctx, repoPath, walFilesPathForLSN(mgr.stateDirectory, lsn), repack.GetNewFiles(), repack.GetDeletedFiles()); err != nil {
		return fmt.Errorf("applying packfiles into destination repository: %w", err)
	}

	// During migration to the new transaction system, loose objects might still exist here and there. So, this task
	// needs to clean up redundant loose objects. After the target repository runs repacking for the first time,
	// there shouldn't be any further loose objects. All of them exist in packfiles. Afterward, this command will
	// exist instantly. We can remove this run after the transaction system is fully applied.
	if err := mgr.pruneLooseObjects(repoPath); err != nil {
		return fmt.Errorf("prune loose objects: %w", err)
	}

	if repack.GetIsFullRepack() {
		if err := stats.UpdateFullRepackTimestamp(mgr.getAbsolutePath(logEntry.GetRelativePath()), time.Now()); err != nil {
			return fmt.Errorf("updating repack timestamp: %w", err)
		}
	}

	if err := safe.NewSyncer().Sync(ctx, filepath.Join(repoPath, "objects")); err != nil {
		return fmt.Errorf("sync objects dir: %w", err)
	}
	return nil
}

// Git stores loose objects in the object directory under subdirectories with two hex digits in their name.
var regexpLooseObjectDir = regexp.MustCompile("^[[:xdigit:]]{2}$")

// pruneLooseObjects removes all loose objects from the object directory.
func (mgr *TransactionManager) pruneLooseObjects(repositoryPath string) error {
	absoluteObjectDirectory := filepath.Join(repositoryPath, "objects")

	entries, err := os.ReadDir(absoluteObjectDirectory)
	if err != nil {
		return fmt.Errorf("read dir: %w", err)
	}

	for _, entry := range entries {
		if entry.IsDir() && regexpLooseObjectDir.MatchString(entry.Name()) {
			if err := os.RemoveAll(filepath.Join(absoluteObjectDirectory, entry.Name())); err != nil {
				return fmt.Errorf("remove all: %w", err)
			}
		}
	}

	return nil
}

// applyCommitGraphs replaces the existing commit-graph folder or monolithic file by the new commit-graph chain. The
// new chain can be identical to the existing chain, but the fee of checking is similar to the replacement fee. Thus, we
// can simply remove and link new directory over. Apart from commit-graph task, the repacking task also replaces the
// chain before verification.
func (mgr *TransactionManager) applyCommitGraphs(ctx context.Context, lsn storage.LSN, logEntry *gitalypb.LogEntry) error {
	if logEntry.GetHousekeeping().GetWriteCommitGraphs() == nil {
		return nil
	}

	span, _ := tracing.StartSpanIfHasParent(ctx, "transaction.applyCommitGraphs", nil)
	defer span.Finish()

	finishTimer := mgr.metrics.housekeeping.ReportTaskLatency("commit-graph", "apply")
	defer finishTimer()

	repoPath := mgr.getAbsolutePath(logEntry.GetRelativePath())
	if err := mgr.replaceCommitGraphs(ctx, repoPath, walFilesPathForLSN(mgr.stateDirectory, lsn)); err != nil {
		return fmt.Errorf("rewriting commit-graph: %w", err)
	}

	return nil
}

// replacePackfiles replaces the set of packfiles at the destination repository by the new set of packfiles from WAL
// directory. Any packfile outside the input deleted files are kept intact.
func (mgr *TransactionManager) replacePackfiles(ctx context.Context, repoPath string, walPath string, newFiles []string, deletedFiles []string) error {
	for _, file := range newFiles {
		if err := os.Link(
			filepath.Join(walPath, file),
			filepath.Join(repoPath, "objects", "pack", file),
		); err != nil {
			// A new resulting packfile might exist if the log entry is re-applied after a crash.
			if !errors.Is(err, os.ErrExist) {
				return fmt.Errorf("linking new packfile: %w", err)
			}
		}
	}

	for _, file := range deletedFiles {
		if err := os.Remove(filepath.Join(repoPath, "objects", "pack", file)); err != nil {
			// Repacking task is the only operation that touch an on-disk packfile. Other operations should
			// only create new packfiles. However, after a crash, a pre-existing packfile might be cleaned up
			// beforehand.
			if !errors.Is(err, os.ErrNotExist) {
				return fmt.Errorf("clean up repacked packfile: %w", err)
			}
		}
	}

	if err := safe.NewSyncer().Sync(ctx, filepath.Join(repoPath, "objects", "pack")); err != nil {
		return fmt.Errorf("sync objects/pack dir: %w", err)
	}

	return nil
}

func (mgr *TransactionManager) replaceCommitGraphs(ctx context.Context, repoPath string, walPath string) error {
	// Clean up and apply commit graphs.
	walGraphsDir := filepath.Join(walPath, "commit-graphs")
	if graphEntries, err := os.ReadDir(walGraphsDir); err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("reading dir: %w", err)
		}
	} else {
		if err := os.Remove(filepath.Join(repoPath, "objects", "info", "commit-graph")); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("resetting commit-graph file: %w", err)
		}
		commitGraphsDir := filepath.Join(repoPath, "objects", "info", "commit-graphs")
		if err := os.RemoveAll(commitGraphsDir); err != nil {
			return fmt.Errorf("resetting commit-graphs dir: %w", err)
		}
		if err := os.Mkdir(commitGraphsDir, mode.Directory); err != nil {
			return fmt.Errorf("creating commit-graphs dir: %w", err)
		}
		for _, entry := range graphEntries {
			if err := os.Link(
				filepath.Join(walGraphsDir, entry.Name()),
				filepath.Join(commitGraphsDir, entry.Name()),
			); err != nil {
				return fmt.Errorf("linking commit-graph entry: %w", err)
			}
		}
		if err := safe.NewSyncer().Sync(ctx, commitGraphsDir); err != nil {
			return fmt.Errorf("sync objects/pack dir: %w", err)
		}
		if err := safe.NewSyncer().SyncParent(ctx, commitGraphsDir); err != nil {
			return fmt.Errorf("sync objects/pack dir: %w", err)
		}
	}
	return nil
}

// isDirEmpty checks if a directory is empty.
func isDirEmpty(dir string) (bool, error) {
	f, err := os.Open(dir)
	if err != nil {
		return false, err
	}
	defer f.Close()

	// Read at most one entry from the directory. If we get EOF, the directory is empty
	if _, err = f.Readdirnames(1); errors.Is(err, io.EOF) {
		return true, nil
	}
	return false, err
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

// storeCommittedLSN stores the partition's committed LSN in the database.
func (mgr *TransactionManager) storeCommittedLSN(lsn storage.LSN) error {
	return mgr.setKey(keyCommittedLSN, lsn.ToProto())
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
