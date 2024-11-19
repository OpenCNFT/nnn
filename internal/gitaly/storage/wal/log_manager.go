package wal

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"runtime/trace"
	"sync"
	"sync/atomic"

	"github.com/dgraph-io/badger/v4"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/keyvalue"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/mode"
	"gitlab.com/gitlab-org/gitaly/v16/internal/safe"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
	"google.golang.org/protobuf/proto"
)

// keyCommittedLSN is the database key storing a partition's last committed log entry's LSN.
var keyCommittedLSN = []byte("committed_lsn")

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

type positionType int

const (
	// appliedPosition keeps track of latest applied position. WAL could not prune a log entry if it has not been applied.
	appliedPosition positionType = iota + 1
	// referencedPosition keeps track of the referenced position of a log entry. After a log entry has been
	// committed, it could be referenced by other transactions. Thus, we need to keep track of the earliest
	// LSN which is still referenced.
	referencedPosition
	// consumerPosition keeps track of the latest consumer acknowledgement.
	consumerPosition
)

// position tracks the last LSN acknowledged for of a particular type.
type position struct {
	lsn atomic.Value
}

func (p *position) getPosition() storage.LSN {
	pos := p.lsn.Load()
	if pos == nil {
		return 0
	}
	return pos.(storage.LSN)
}

func (p *position) setPosition(pos storage.LSN) {
	p.lsn.Store(pos)
}

type testLogHooks struct {
	// BeforeAppendLogEntry is triggered before an entry is appended to WAL.
	BeforeAppendLogEntry func(storage.LSN)
	// BeforeCommitLogEntry is triggered before an entry is marked as committed.
	BeforeCommitLogEntry func(storage.LSN)
}

// LogManager is responsible for managing the Write-Ahead Log (WAL) entries on disk. It maintains the in-memory state
// and indexing system that reflect the functional state of the WAL. The LogManager ensures safe and consistent
// proposals, applications, and prunings of log entries, acting as the interface for transactional log operations. It
// coordinates with LogConsumer to allow safe consumption of log entries while handling retention and cleanup based on
// references and acknowledgements. It effectively abstracts WAL operations from the TransactionManager, contributing to
// a cleaner separation of concerns and making the system more maintainable and extensible.
type LogManager struct {
	// mutex protects access to critical states, especially `oldestLSN` and `committedLSN`, as well as the integrity
	// of inflight log entries. Since indices are monotonic, two parallel log committing operations result in pushing
	// files into the same directory and breaking the manifest file. Thus, Parallel log entry committing and pruning
	// are not supported.
	mutex sync.Mutex

	// storageName is the name of the storage the LogManager's partition is a member of.
	storageName string
	// storage.PartitionID is the ID of the partition this manager is operating on. This is used to determine the database keys.
	partitionID storage.PartitionID
	// db stores essential WAL functional states, especially committedLSN. Those states are valuable after WAL
	// starts.
	db keyvalue.Transactioner

	// tmpDirectory is the directory storing temporary data. One example is log entry deletion. WAL moves a log
	// entry to this dir before removing them completely.
	tmpDirectory string
	// stateDirectory is an absolute path to a directory where write-ahead log stores log entries
	stateDirectory string

	// appendedLSN holds the LSN of the last log entry emitted by the current node but not yet acknowledged by other
	// nodes. After so, the committedLSN is increment respectively and catches up with appendedLSN. If Raft is not
	// enabled or functions as a single-node cluster, committedLSN is increment instantly.
	appendedLSN storage.LSN
	// committedLSN holds the LSN of the last log entry committed to the partition's write-ahead log.
	committedLSN storage.LSN
	// oldestLSN holds the LSN of the head of log entries which is still kept in the database. The manager keeps
	// them because they are still referred by a transaction.
	oldestLSN storage.LSN

	// consumer is an external caller that may perform read-only operations against applied log entries. Log entries
	// are retained until the consumer has acknowledged past their LSN.
	consumer storage.LogConsumer
	// positions tracks positions of log entries eing used externally. Those positions are tracked so that WAL
	// doesn't prune a log entry accidentally.
	positions map[positionType]*position

	// appendedEntries keeps track of appended but not-yet committed entries. After an entry is committed, it is
	// removed from this map. This provides quick reference to those entries.
	appendedEntries map[storage.LSN]*gitalypb.LogEntry

	// notifyQueue is a queue notifying when there is a new change.
	notifyQueue chan struct{}

	// TestHooks are used in the tests to trigger logic at certain points in the execution.
	// They are used to synchronize more complex test scenarios. Not used in production.
	TestHooks testLogHooks
}

// NewLogManager returns an instance of LogManager.
func NewLogManager(storageName string, partitionID storage.PartitionID, db keyvalue.Transactioner, stagingDirectory string, stateDirectory string, consumer storage.LogConsumer) *LogManager {
	positions := map[positionType]*position{
		appliedPosition:    {},
		referencedPosition: {},
	}
	if consumer != nil {
		positions[consumerPosition] = &position{}
	}
	return &LogManager{
		storageName:     storageName,
		partitionID:     partitionID,
		db:              db,
		tmpDirectory:    stagingDirectory,
		stateDirectory:  stateDirectory,
		consumer:        consumer,
		positions:       positions,
		notifyQueue:     make(chan struct{}, 1),
		appendedEntries: map[storage.LSN]*gitalypb.LogEntry{},
		TestHooks: testLogHooks{
			BeforeAppendLogEntry: func(storage.LSN) {},
			BeforeCommitLogEntry: func(storage.LSN) {},
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
func (mgr *LogManager) Initialize(ctx context.Context, appliedLSN storage.LSN) error {
	if err := mgr.createStateDirectory(ctx); err != nil {
		return fmt.Errorf("create state directory: %w", err)
	}

	// The LSN of the last committed log entry is determined from the LSN of the latest entry in the log and
	// the latest applied log entry. The manager also keeps track of committed entries and reserves them until there
	// is no transaction refers them. It's possible there are some left-over entries in the database because a
	// transaction can hold the entry stubbornly. So, the manager could not clean them up in the last session.
	//
	//  ┌─ oldestLSN                    ┌─ committedLSN
	// ┌┴┐ ┌─┐ ┌─┐ ┌▼┐ ┌▼┐ ┌▼┐ ┌▼┐ ┌▼┐ ┌▼┐
	// └─┘ └─┘ └─┘ └┬┘ └─┘ └─┘ └─┘ └─┘ └─┘
	//  ◄───────►   └─ appliedLSN
	// Can remove
	//
	// oldestLSN is initialized to appliedLSN + 1. If there are no log entries in the log, then everything has been
	// pruned already or there has not been any log entries yet. Setting this +1 avoids trying to clean up log entries
	// that do not exist. If there are some, we'll set oldestLSN to the head of the log below.
	mgr.oldestLSN = appliedLSN + 1
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
		mgr.committedLSN = appliedLSN
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

	mgr.AcknowledgeAppliedPos(appliedLSN)
	mgr.AcknowledgeReferencedPos(mgr.committedLSN)

	return nil
}

// AcknowledgeAppliedPos acknowledges the position of latest applied log entry.
func (mgr *LogManager) AcknowledgeAppliedPos(lsn storage.LSN) {
	mgr.positions[appliedPosition].setPosition(lsn)
}

// AcknowledgeReferencedPos acknowledge the earliest referenced position.
func (mgr *LogManager) AcknowledgeReferencedPos(lsn storage.LSN) {
	mgr.positions[referencedPosition].setPosition(lsn)
}

// AcknowledgeConsumerPos acknowledges log entries up and including lsn as successfully processed
// for the specified LogConsumer. The manager is awakened if it is currently awaiting a new or
// completed transaction.
func (mgr *LogManager) AcknowledgeConsumerPos(lsn storage.LSN) {
	if mgr.consumer == nil {
		panic("log manager's consumer must be present prior to AcknowledgeConsumerPos call")
	}
	mgr.positions[consumerPosition].setPosition(lsn)

	// Alert the outsider. If it has a pending acknowledgement already no action is required.
	select {
	case mgr.notifyQueue <- struct{}{}:
	default:
	}
}

// StateDirectory returns the state directory under the management of this manager.
func (mgr *LogManager) StateDirectory() string {
	return mgr.stateDirectory
}

// NotifyQueue returns a notify channel so that caller can poll new changes.
func (mgr *LogManager) NotifyQueue() <-chan struct{} {
	return mgr.notifyQueue
}

// CommitLogEntry commits an entry to the write-ahead log. logEntryPath is an
// absolute path to the directory that represents the log entry. It moves the
// log entry's directory to the WAL, and returns its LSN once it has been
// committed to the log.
func (mgr *LogManager) CommitLogEntry(ctx context.Context, logEntry *gitalypb.LogEntry, logEntryPath string) (storage.LSN, error) {
	mgr.mutex.Lock()
	defer mgr.mutex.Unlock()

	nextLSN := mgr.appendedLSN + 1
	if err := mgr.AppendLogEntry(ctx, nextLSN, logEntry, logEntryPath); err != nil {
		return 0, fmt.Errorf("append log entry: %w", err)
	}
	if err := mgr.MarkLogEntryAsCommitted(ctx, nextLSN); err != nil {
		return 0, fmt.Errorf("commit log entry: %w", err)
	}

	// After this latch block, the transaction is committed and all subsequent transactions
	// are guaranteed to read it.
	if mgr.consumer != nil {
		mgr.consumer.NotifyNewEntries(mgr.storageName, mgr.partitionID, mgr.lowWaterMark(), nextLSN)
	}
	return nextLSN, nil
}

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

	mgr.TestHooks.BeforeAppendLogEntry(lsn)

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

	mgr.appendedLSN = lsn
	mgr.appendedEntries[mgr.appendedLSN] = logEntry

	return nil
}

func (mgr *LogManager) MarkLogEntryAsCommitted(ctx context.Context, lsn storage.LSN) error {
	mgr.TestHooks.BeforeCommitLogEntry(lsn)

	// Persist committed LSN before updating internal states.
	if err := mgr.storeCommittedLSN(lsn); err != nil {
		return fmt.Errorf("persisting committed entry: %w", err)
	}

	mgr.committedLSN = lsn
	delete(mgr.appendedEntries, lsn)
	return nil
}

func (mgr *LogManager) createStateDirectory(ctx context.Context) error {
	needsFsync := false
	for _, path := range []string{
		mgr.stateDirectory,
		filepath.Join(mgr.stateDirectory, "wal"),
	} {
		err := os.Mkdir(path, mode.Directory)
		switch {
		case errors.Is(err, fs.ErrExist):
			continue
		case err != nil:
			return fmt.Errorf("mkdir: %w", err)
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

// PruneLogEntries prunes log entries from the Write-Ahead Log (WAL) that have been committed and are no longer
// needed. It ensures efficient storage management by removing redundant entries while maintaining the integrity of the
// log sequence. The method respects the established low-water mark, ensuring no entries that might still be required
// for transaction consistency are deleted.
func (mgr *LogManager) PruneLogEntries(ctx context.Context) (storage.LSN, error) {
	mgr.mutex.Lock()
	defer mgr.mutex.Unlock()

	// When a log entry is applied, if there is any log in front of it which are still referred, we cannot delete
	// it. This condition is to prevent a "hole" in the list. A transaction referring to a log entry at the
	// low-water mark might scan all afterward log entries. Thus, the manager needs to keep in the database.
	//
	//                  ┌── Consumer not acknowledged
	//                  │       ┌─ Referenced by snapshots
	//    Can remove    │       │       ┌─ Free, but cannot be removed
	//  ◄───────────►   │       │       │
	// ┌─┐ ┌─┐ ┌─┐ ┌─┐ ┌▼┐ ┌▼┐ ┌▼┐ ┌▼┐ ┌▼┐ ┌▼┐
	// └┬┘ └─┘ └─┘ └─┘ └─┘ └─┘ └─┘ └─┘ └─┘ └─┘
	//  └─ oldestLSN    ▲
	//                  │
	//            Low-water mark
	//
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

// CommittedLSN returns the index of latest committed log entry.
func (mgr *LogManager) CommittedLSN() storage.LSN {
	mgr.mutex.Lock()
	defer mgr.mutex.Unlock()

	return mgr.committedLSN
}

// GetEntryPath returns the path of the log entry's root directory.
func (mgr *LogManager) GetEntryPath(lsn storage.LSN) string {
	return LogEntryPath(mgr.stateDirectory, lsn)
}

// deleteLogEntry deletes the log entry at the given LSN from the log.
func (mgr *LogManager) deleteLogEntry(ctx context.Context, lsn storage.LSN) error {
	defer trace.StartRegion(ctx, "deleteLogEntry").End()

	tmpDir, err := os.MkdirTemp(mgr.tmpDirectory, "")
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

// lowWaterMark returns the earliest LSN of log entries which should be kept in the database. Any log entries LESS than
// this mark are removed.
func (mgr *LogManager) lowWaterMark() storage.LSN {
	minAcknowledged := mgr.committedLSN + 1

	// Position is the last acknowledged LSN, this is eligible for pruning.
	// lowWaterMark returns the lowest LSN that cannot be pruned, so add one.
	for _, pos := range mgr.positions {
		if pos.getPosition()+1 < minAcknowledged {
			minAcknowledged = pos.getPosition() + 1
		}
	}

	return minAcknowledged
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

// storeCommittedLSN stores the partition's committed LSN in the database.
func (mgr *LogManager) storeCommittedLSN(lsn storage.LSN) error {
	return mgr.setKey(keyCommittedLSN, lsn.ToProto())
}
