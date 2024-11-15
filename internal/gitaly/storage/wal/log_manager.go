package wal

import (
	"container/list"
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"runtime/trace"
	"sync"

	"github.com/dgraph-io/badger/v4"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/keyvalue"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/mode"
	"gitlab.com/gitlab-org/gitaly/v16/internal/safe"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
	"google.golang.org/protobuf/proto"
)

var (
	// ErrEntryReferenceGone is returned when the log entry of a LSN is gone from database while it's still
	// accessed by other transactions.
	ErrEntryReferenceGone = errors.New("in-used entry references is gone")
	// KeyCommittedLSN is the database key storing a partition's last committed log entry's LSN.
	KeyCommittedLSN = []byte("committed_lsn")
	// KeyAppliedLSN is the database key storing a partition's last applied log entry's LSN.
	KeyAppliedLSN = []byte("applied_lsn")
)

// walStatePath returns the WAL directory's path.
func walStatePath(stateDir string) string {
	return filepath.Join(stateDir, "wal")
}

// LogEntryPath returns an absolute path to a given log entry's WAL files.
func LogEntryPath(stateDir string, lsn storage.LSN) string {
	return filepath.Join(walStatePath(stateDir), lsn.String())
}

// manifestPath returns the manifest file's path in the log entry.
func manifestPath(logEntryPath string) string {
	return filepath.Join(logEntryPath, "MANIFEST")
}

// EntryReference is a wrapper for a log entry reference. It is used to keep track of entries which are still referenced
// by other transactions.
type EntryReference struct {
	// lsn is the associated LSN of the entry
	lsn storage.LSN
	// refs accounts for the number of references to this entry.
	refs int
	// objectDependencies are the objects this transaction depends upon.
	objectDependencies map[git.ObjectID]struct{}
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

type testLogHooks struct {
	BeforeAppendLogEntry  func(storage.LSN)
	BeforeCommitLogEntry  func(storage.LSN)
	BeforeStoreAppliedLSN func(storage.LSN)
}

// LogManager is responsible for managing the Write-Ahead Log (WAL) entries on disk. It maintains the in-memory state
// and indexing system that reflect the functional state of the WAL. The LogManager ensures safe and consistent
// proposals, applications, and prunings of log entries, acting as the interface for transactional log operations. It
// coordinates with LogConsumer to allow safe consumption of log entries while handling retention and cleanup based on
// references and acknowledgements. It effectively abstracts WAL operations from the TransactionManager, contributing to
// a cleaner separation of concerns and making the system more maintainable and extensible.
type LogManager struct {
	mutex sync.Mutex
	db    keyvalue.Transactioner

	// storageName is the name of the storage the LogManager's partition is a member of.
	storageName string
	// storage.PartitionID is the ID of the partition this manager is operating on. This is used to determine the database keys.
	partitionID storage.PartitionID

	// stagingDirectory is the directory where log entries are processed before being logged. It is cleaned up when
	// the log entry is logged.
	stagingDirectory string
	// stateDirectory is an absolute path to a directory where write-ahead log stores log entries
	stateDirectory string

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

	// appendedEntries keeps track of appended but not-yet committed entries. After an entry is committed, it is
	// removed from this map. This provides quick reference to those entries without the need for disk operations.
	appendedEntries map[storage.LSN]*gitalypb.LogEntry

	// entryReferences keeps some latest committed log entries around. Some types of transactions, such as
	// housekeeping, operate on snapshot repository. There is a gap between transaction doing its work and the time
	// when it is committed. They need to verify if concurrent operations can cause conflict. These log entries are
	// still kept around even after they are applied. They are removed when there are no references.
	entryReferences *list.List

	// consumer is an the external caller that may perform read-only operations against applied
	// log entries. Log entries are retained until the consumer has acknowledged past their LSN.
	consumer storage.LogConsumer
	// consumerPos tracks the largest LSN that has been acknowledged by consumer.
	consumerPos *consumerPosition

	// notifyQueue is a queue notifying when there is a new change.
	notifyQueue chan struct{}

	// TestHooks are used in the tests to trigger logic at certain points in the execution.
	// They are used to synchronize more complex test scenarios. Not used in production.
	TestHooks testLogHooks
}

// NewLogManager returns an instance of LogManager.
func NewLogManager(storageName string, partitionID storage.PartitionID, db keyvalue.Transactioner, stagingDirectory string, stateDirectory string, consumer storage.LogConsumer) *LogManager {
	return &LogManager{
		db:               db,
		storageName:      storageName,
		partitionID:      partitionID,
		stagingDirectory: stagingDirectory,
		stateDirectory:   stateDirectory,
		entryReferences:  list.New(),
		appendedEntries:  map[storage.LSN]*gitalypb.LogEntry{},
		consumer:         consumer,
		consumerPos:      &consumerPosition{},
		notifyQueue:      make(chan struct{}, 1),
		TestHooks: testLogHooks{
			BeforeAppendLogEntry:  func(storage.LSN) {},
			BeforeCommitLogEntry:  func(storage.LSN) {},
			BeforeStoreAppliedLSN: func(storage.LSN) {},
		},
	}
}

// Initialize sets up the initial state of the LogManager, preparing it to manage the write-ahead log entries. It reads
// the last applied LSN from the database to resume from where it left off, creates necessary directories, and
// initializes in-memory tracking variables such as committedLSN and oldestLSN based on the files present in the WAL
// directory. This method also removes any stale log files that may have been left due to interrupted operations,
// ensuring the WAL directory only contains valid log entries. If a LogConsumer is present, it notifies it of the
// initial log entry state, enabling consumers to start processing from the correct point. Proper initialization is
// crucial for maintaining data consistency and ensuring that log entries are managed accurately upon system startup.
func (mgr *LogManager) Initialize(ctx context.Context) error {
	var appliedLSN gitalypb.LSN
	if err := mgr.readKey(KeyAppliedLSN, &appliedLSN); err != nil && !errors.Is(err, badger.ErrKeyNotFound) {
		return fmt.Errorf("read applied LSN: %w", err)
	}

	mgr.appliedLSN = storage.LSN(appliedLSN.GetValue())

	if err := mgr.createStateDirectory(ctx); err != nil {
		return fmt.Errorf("create state directory: %w", err)
	}

	// The LSN of the last appended log entry is determined from the LSN of the latest entry in the log and
	// the latest applied log entry. The manager also keeps track of committed entries and reserves them until there
	// is no transaction refers them. It's possible there are some left-over entries in the database because a
	// transaction can hold the entry stubbornly. So, the manager could not clean them up in the last session.
	//
	// ┌─ oldestLSN                    ┌─ committedLSN
	// ⧅ ⧅ ⧅ ⧅ ⧅ ⧅ ⧅ ■ ■ ■ ■ ■ ■ ■ ■ ■ ■ □ □ □ □ □ □ □
	//               └─ appliedLSN                   └─ appendedLSN
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
	if err := mgr.readKey(KeyCommittedLSN, &committedLSN); err != nil {
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

	if logEntries, err := os.ReadDir(walStatePath(mgr.stateDirectory)); err != nil {
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
			if err := mgr.deleteLogEntry(ctx, lsn); err != nil {
				return fmt.Errorf("cleaning uncommitted log entry: %w", err)
			}
		}
	}

	if err := mgr.removeStaleWALFiles(ctx); err != nil {
		return fmt.Errorf("remove stale packs: %w", err)
	}

	if mgr.consumer != nil && mgr.committedLSN != 0 {
		mgr.consumer.NotifyNewEntries(mgr.storageName, mgr.partitionID, mgr.oldestLSN, mgr.committedLSN)
	}

	return nil
}

// StateDirectory returns the state directory under the management of this manager.
func (mgr *LogManager) StateDirectory() string {
	return mgr.stateDirectory
}

// NotifyQueue returns a notify channel so that caller can poll new changes.
func (mgr *LogManager) NotifyQueue() <-chan struct{} {
	return mgr.notifyQueue
}

// Propose proposes a log etnry of a transaction to the write-ahead log. It first writes the transaction's manifest file
// into the log entry's directory. Second, it sends the log entry to other cluster members if needed. Afterwards it
// moves the log entry's directory from the staging area to its final place in the write-ahead log. The landing order of
// proposals is nondeterministic. The caller is responsible for serialization, if it needs to, before calling Propose().
func (mgr *LogManager) Propose(ctx context.Context, objectDependencies map[git.ObjectID]struct{}, logEntry *gitalypb.LogEntry, logEntryPath string) (storage.LSN, error) {
	defer trace.StartRegion(ctx, "proposeLogEntry").End()

	nextLSN := mgr.appendedLSN + 1
	if err := mgr.AppendLogEntry(ctx, nextLSN, logEntry, logEntryPath); err != nil {
		return 0, fmt.Errorf("append log entry: %w", err)
	}

	if err := mgr.CommitLogEntry(ctx, nextLSN, objectDependencies); err != nil {
		return 0, fmt.Errorf("commit log entry: %w", err)
	}

	if mgr.consumer != nil {
		mgr.consumer.NotifyNewEntries(mgr.storageName, mgr.partitionID, mgr.lowWaterMark(), nextLSN)
	}

	return nextLSN, nil
}

// AppendLogEntry appends a new log entry at the specified LSN (Log Sequence Number), using the provided log entry data
// and file path, ensuring the entry is durably written. An appended entry is not safe to be referenced in the snapshot
// because it might be overriden by log entries having the same LSN.
func (mgr *LogManager) AppendLogEntry(ctx context.Context, lsn storage.LSN, logEntry *gitalypb.LogEntry, logEntryPath string) error {
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

	mgr.TestHooks.BeforeCommitLogEntry(lsn)

	// Move the log entry from the staging directory into its place in the log.
	destinationPath := mgr.GetEntryPath(lsn)
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

	mgr.mutex.Lock()
	mgr.appendedLSN = lsn
	mgr.appendedEntries[mgr.appendedLSN] = logEntry
	mgr.mutex.Unlock()

	return nil
}

// CommitLogEntry marks the log entry at the given LSN as committed. Afterward, that entry is safe to be referenced by
// snapshots. Eventually, it will be applied.
func (mgr *LogManager) CommitLogEntry(ctx context.Context, nextLSN storage.LSN, objectDependencies map[git.ObjectID]struct{}) error {
	mgr.TestHooks.BeforeCommitLogEntry(nextLSN)

	// Persist committed LSN before updating other internal states.
	if err := mgr.storeCommittedLSN(nextLSN); err != nil {
		return fmt.Errorf("persisting committed entry: %w", err)
	}

	mgr.mutex.Lock()
	if _, exist := mgr.appendedEntries[nextLSN]; !exist {
		mgr.mutex.Unlock()
		return fmt.Errorf("log entry %s not found in the appended list", nextLSN)
	}
	delete(mgr.appendedEntries, nextLSN)
	mgr.entryReferences.PushBack(&EntryReference{
		lsn:                nextLSN,
		objectDependencies: objectDependencies,
	})
	mgr.mutex.Unlock()

	return nil
}

func (mgr *LogManager) createStateDirectory(ctx context.Context) error {
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

// removeStaleWALFiles removes files from the log directory that have no associated log entry.
// Such files can be left around if transaction's files were moved in place successfully
// but the manager was interrupted before successfully persisting the log entry itself.
// If the manager deletes a log entry successfully from the database but is interrupted before it cleans
// up the associated files, such a directory can also be left at the head of the log.
func (mgr *LogManager) removeStaleWALFiles(ctx context.Context) error {
	needsFsync := false
	for _, possibleStaleFilesPath := range []string{
		// Log entries are pruned one by one. If a write is interrupted, the only possible stale files would be
		// for the log entry preceding the oldest log entry.
		LogEntryPath(mgr.stateDirectory, mgr.oldestLSN-1),
		// Log entries are committed one by one to the log. If a write is interrupted, the only possible stale
		// files would be for the next LSN. Remove the files if they exist.
		LogEntryPath(mgr.stateDirectory, mgr.committedLSN+1),
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
		if err := safe.NewSyncer().Sync(ctx, walStatePath(mgr.stateDirectory)); err != nil {
			return fmt.Errorf("sync: %w", err)
		}
	}

	return nil
}

// RemoveAppliedLogEntries prunes log entries from the Write-Ahead Log (WAL) that have been applied and are no longer
// needed. It ensures efficient storage management by removing redundant entries while maintaining the integrity of the
// log sequence. The method respects the established low-water mark, ensuring no entries that might still be required
// for transaction consistency are deleted.
func (mgr *LogManager) RemoveAppliedLogEntries(ctx context.Context) (storage.LSN, error) {
	mgr.mutex.Lock()
	defer mgr.mutex.Unlock()

	// When a log entry is applied, if there is any log in front of it which are still referred, we cannot delete
	// it. This condition is to prevent a "hole" in the list. A transaction referring to a log entry at the
	// low-water mark might scan all afterward log entries. Thus, the manager needs to keep in the database.
	//
	// ┌─ Oldest LSN
	// ┌─ Can be removed ─┐            ┌─ Cannot be removed
	// □ □ □ □ □ □ □ □ □ □ ■ ■ ⧅ ⧅ ⧅ ⧅ ⧅ ⧅ ■ ■ ⧅ ⧅ ⧅ ⧅ ■
	//                     └─ Low-water mark, still referred by another transaction
	if mgr.oldestLSN < mgr.lowWaterMark() {
		removingLSN := mgr.oldestLSN
		if err := mgr.deleteLogEntry(ctx, removingLSN); err != nil {
			return 0, fmt.Errorf("deleting log entry: %w", err)
		}
		mgr.oldestLSN++
		return removingLSN, nil
	}
	return 0, nil
}

// NextApplyLSN returns the LSN of the next entry to apply. If all entries are applied, default zero value is returned.
func (mgr *LogManager) NextApplyLSN() storage.LSN {
	mgr.mutex.Lock()
	defer mgr.mutex.Unlock()
	if mgr.appliedLSN < mgr.committedLSN {
		return mgr.appliedLSN + 1
	}
	return 0
}

// CommittedLSN returns the index of latest committed log entry.
func (mgr *LogManager) CommittedLSN() storage.LSN {
	mgr.mutex.Lock()
	defer mgr.mutex.Unlock()

	return mgr.committedLSN
}

// AppliedLSN returns the index of latest applied log entry.
func (mgr *LogManager) AppliedLSN() storage.LSN {
	mgr.mutex.Lock()
	defer mgr.mutex.Unlock()

	return mgr.appliedLSN
}

// StoreAppliedLSN stores the partition's applied LSN in the database.
func (mgr *LogManager) StoreAppliedLSN(lsn storage.LSN) error {
	mgr.TestHooks.BeforeStoreAppliedLSN(lsn)

	mgr.mutex.Lock()
	defer mgr.mutex.Unlock()

	if err := mgr.setKey(KeyAppliedLSN, lsn.ToProto()); err != nil {
		return fmt.Errorf("storing applied LSN: %w", err)
	}
	mgr.appliedLSN = lsn
	return nil
}

// StoreCommittedLSN stores the partition's committed LSN in the database.
func (mgr *LogManager) storeCommittedLSN(lsn storage.LSN) error {
	mgr.mutex.Lock()
	defer mgr.mutex.Unlock()

	if err := mgr.setKey(KeyCommittedLSN, lsn.ToProto()); err != nil {
		return fmt.Errorf("storing committedLSN: %w", err)
	}
	mgr.committedLSN = lsn
	return nil
}

// AcknowledgeConsumerPos acknowledges log entries up and including lsn as successfully processed
// for the specified LogConsumer. The manager is awakened if it is currently awaiting a new or
// completed transaction.
func (mgr *LogManager) AcknowledgeConsumerPos(lsn storage.LSN) {
	mgr.consumerPos.setPosition(lsn)

	// Alert the manager. If it has a pending acknowledgement already no action is required.
	select {
	case mgr.notifyQueue <- struct{}{}:
	default:
	}
}

// GetEntryPath returns the path of the log entry's root directory.
func (mgr *LogManager) GetEntryPath(lsn storage.LSN) string {
	return LogEntryPath(mgr.stateDirectory, lsn)
}

// deleteLogEntry deletes the log entry at the given LSN from the log.
func (mgr *LogManager) deleteLogEntry(ctx context.Context, lsn storage.LSN) error {
	defer trace.StartRegion(ctx, "deleteLogEntry").End()

	tmpDir, err := os.MkdirTemp(mgr.stagingDirectory, "")
	if err != nil {
		return fmt.Errorf("mkdir temp: %w", err)
	}

	logEntryPath := LogEntryPath(mgr.stateDirectory, lsn)
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

// ReadLogEntry returns the log entry from the given position in the log.
func (mgr *LogManager) ReadLogEntry(lsn storage.LSN) (*gitalypb.LogEntry, error) {
	manifestBytes, err := os.ReadFile(manifestPath(LogEntryPath(mgr.stateDirectory, lsn)))
	if err != nil {
		return nil, fmt.Errorf("read manifest: %w", err)
	}

	var logEntry gitalypb.LogEntry
	if err := proto.Unmarshal(manifestBytes, &logEntry); err != nil {
		return nil, fmt.Errorf("unmarshal manifest: %w", err)
	}

	return &logEntry, nil
}

// setKey marshals and stores a given protocol buffer message into the database under the given key.
func (mgr *LogManager) setKey(key []byte, value proto.Message) error {
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
func (mgr *LogManager) readKey(key []byte, destination proto.Message) error {
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
func (mgr *LogManager) lowWaterMark() storage.LSN {
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

	elm := mgr.entryReferences.Front()
	if elm == nil {
		return minConsumed
	}

	committed := elm.Value.(*EntryReference).lsn
	if minConsumed < committed {
		return minConsumed
	}

	return committed
}

// IncrementEntryReference updates the ref counter of a committed entry. An entry reference is created after the
// corresponding log entry is committed. WAL allows the caller to refer to latest committed log entry only. It does not
// allow cross-reference to an entry in the middle of the list. So, this method increases the ref counter of the tail of
// the list or create a new one if the list does not exist.
func (mgr *LogManager) IncrementEntryReference(lsn storage.LSN) *EntryReference {
	mgr.mutex.Lock()
	defer mgr.mutex.Unlock()

	if elm := mgr.entryReferences.Back(); elm != nil {
		entry := elm.Value.(*EntryReference)
		entry.refs++
		return entry
	}

	entry := &EntryReference{
		lsn:  lsn,
		refs: 1,
	}
	mgr.entryReferences.PushBack(entry)

	return entry
}

// WalkCommittedEntries walks all committed entries after input LSN. It loads the content of the
// entry from disk and triggers the callback with entry content.
func (mgr *LogManager) WalkCommittedEntries(lsn storage.LSN, relativePath string, callback func(*gitalypb.LogEntry, map[git.ObjectID]struct{}) error) error {
	mgr.mutex.Lock()
	defer mgr.mutex.Unlock()

	for elm := mgr.entryReferences.Front(); elm != nil; elm = elm.Next() {
		committed := elm.Value.(*EntryReference)
		if committed.lsn <= lsn {
			continue
		}
		entry, err := mgr.ReadLogEntry(committed.lsn)
		if err != nil {
			return ErrEntryReferenceGone
		}
		// Transaction manager works on the partition level, including a repository and all of its pool
		// member repositories (if any). We need to filter log entries of the repository this
		// transaction targets.
		if entry.GetRelativePath() != relativePath {
			continue
		}
		if err := callback(entry, committed.objectDependencies); err != nil {
			return fmt.Errorf("callback: %w", err)
		}
	}
	return nil
}

// DecrementEntryReference reduces the ref counter of the committed entry. It also removes entries with no more readers at
// the head of the list.
func (mgr *LogManager) DecrementEntryReference(entry *EntryReference) {
	mgr.mutex.Lock()
	defer mgr.mutex.Unlock()

	entry.refs--
	if entry.refs < 0 {
		panic("negative log entry reference counter")
	}
	mgr.cleanupEntryReferences()
}

func (mgr *LogManager) cleanupEntryReferences() {
	removedAnyEntry := false
	elm := mgr.entryReferences.Front()
	for elm != nil {
		front := elm.Value.(*EntryReference)
		if front.refs > 0 {
			// If the first entry had some references, that means our transaction was not the oldest reader.
			// We can't remove any entries as they'll still be needed for conflict checking the older
			// transactions.
			break
		}

		mgr.entryReferences.Remove(elm)
		removedAnyEntry = true
		elm = mgr.entryReferences.Front()
	}
	if removedAnyEntry {
		// Signal the manager this transaction finishes. The purpose of this signaling is to wake it up
		// and clean up stale entries in the database. The manager scans and removes leading empty
		// entries. We signal only if the transaction modifies the in-memory committed entry.
		// This signal queue is buffered. If the queue is full, the manager hasn't woken up. The
		// next scan will cover the work of the prior one. So, no need to let the transaction wait.
		// ┌─ 1st signal        ┌─ The manager scans til here
		// □ □ □ □ □ □ □ □ □ □ ■ ■ ⧅ ⧅ ⧅ ⧅ ⧅ ⧅ ■ ■ ⧅ ⧅ ⧅ ⧅ ■
		//        └─ 2nd signal
		select {
		case mgr.notifyQueue <- struct{}{}:
		default:
		}
	}
}
