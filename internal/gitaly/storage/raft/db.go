package raft

import (
	"context"
	"errors"
	"fmt"

	"github.com/lni/dragonboat/v4/statemachine"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/keyvalue"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/storagemgr"
)

// dbAccessor defines an interface to read/write values inside the key-value DB in a transactional
// fashion. All changes are automatically committed after the callback function exits.
type dbAccessor interface {
	read(context.Context, func(keyvalue.ReadWriter) error) error
	write(context.Context, func(keyvalue.ReadWriter) error) error
}

// namespacedDBAccessor is a default implementation of dbAccessor interface.
type namespacedDBAccessor struct {
	access func(context.Context, bool, func(keyvalue.ReadWriter) error) error
}

func (a *namespacedDBAccessor) read(ctx context.Context, fn func(keyvalue.ReadWriter) error) error {
	return a.access(ctx, true, fn)
}

func (a *namespacedDBAccessor) write(ctx context.Context, fn func(keyvalue.ReadWriter) error) error {
	return a.access(ctx, false, fn)
}

func newNamespacedDBAccessor(node storage.Node, storageName string, namespace []byte) *namespacedDBAccessor {
	return &namespacedDBAccessor{
		access: func(ctx context.Context, readOnly bool, fn func(keyvalue.ReadWriter) error) (returnedErr error) {
			storageHandle, err := node.GetStorage(storageName)
			if err != nil {
				return fmt.Errorf("get storage: %w", err)
			}

			partition, err := storageHandle.GetPartition(ctx, storagemgr.MetadataPartitionID)
			if err != nil {
				return fmt.Errorf("get partition: %w", err)
			}
			defer partition.Close()

			tx, err := partition.Begin(ctx, storage.BeginOptions{
				Write: !readOnly,
				// Filter out the disk state entirely as we're only interested in doing KV operations here.
				RelativePaths: []string{},
			})
			if err != nil {
				return fmt.Errorf("begin: %w", err)
			}
			defer func() {
				if returnedErr != nil {
					if err := tx.Rollback(ctx); err != nil {
						returnedErr = errors.Join(returnedErr, fmt.Errorf("rollback: %w", err))
					}
				}
			}()

			if err := fn(keyvalue.NewPrefixedReadWriter(tx.KV(), namespace)); err != nil {
				return err
			}

			if err := tx.Commit(ctx); err != nil {
				return fmt.Errorf("commit: %w", err)
			}

			return nil
		},
	}
}

// dbForStorage returns a namedspaced DB accessor function for specific information of a storage in
// Raft cluster such as allocated storage ID, last applied replica groups, etc.
func dbForStorage(node storage.Node, storageName string) dbAccessor {
	return newNamespacedDBAccessor(node, storageName, []byte("raft/self/"))
}

// dbForMetadataGroup returns a namedspaced DB accessfor function to store the data of metadata Raft
// group. Those data consists of cluster-wide information such as list of registered storages and
// their replication groups, etc.
func dbForMetadataGroup(node storage.Node, storageName string) dbAccessor {
	return newNamespacedDBAccessor(node, storageName, []byte("raft/cluster/"))
}

var keyLastApplied = []byte("applied_lsn")

// Statemachine is an interface that wraps dragonboat's statemachine. It is a superset of
// dragonboat's IOnDiskStateMachine interface.
type Statemachine interface {
	// This interface has the following functions.
	statemachine.IOnDiskStateMachine
	// Open implements Open function of IOnDiskStateMachine interface. It opens the existing on disk
	// state machine to be used or it creates a new state machine with empty state if it does not exist.
	// Open returns the most recent index value of the Raft log that has been persisted, or it returns 0
	// when the state machine is a new one.
	// Open(<-chan struct{}) (uint64, error)

	// Update implements Update function of IOnDiskStateMachine instance. The input Entry slice is a
	// list of continuous proposed and committed commands from clients. At this point, the input entries
	// are finalized and acknowledged by all replicas. The application is responsible for validating the
	// data before submitting the log for replication. The statemachine must handle known application
	// errors, races, conflicts, etc. and return the result back. The log entry is still applied but
	// not necessarily leads to any changes. Otherwise, the cluster is unable to move on without
	// manual interventions. The error is returned if there is a non-recoverable problem with
	// underlying storage so that the log entries will be retried later.
	// The library guarantees linearizable access to this function and the monotonic index
	// increment. It's worth nothing that the indices are not necessarily continuous because the
	// library might include some internal operations which are transparent to the statemachine.
	// Update([]statemachine.Entry) ([]statemachine.Entry, error)

	// Lookup queries the state of the IOnDiskStateMachine instance. The caller is guaranteed to be
	// on the same node of this statemachine. So, the request and response are in protobuf format.
	// No need to marshal it back and forth.
	// Lookup(interface{}) (interface{}, error)

	// Sync synchronizes all in-core state of the state machine to persisted storage so the state
	// machine can continue from its latest state after reboot. Our underlying DB flushes to disk right
	// after a transaction finishes.
	// Sync() error

	// PrepareSnapshot prepares the snapshot to be concurrently captured and streamed. The
	// implemented struct must create a snapshot including all entries before the last index at the
	// time this function is called exclusively. The statemachine will continue to accept new
	// updates while the snapshot is being created.
	// PrepareSnapshot() (interface{}, error)

	// SaveSnapshot saves the point in time state of the IOnDiskStateMachine
	// instance identified by the input state identifier, which is usually not
	// the latest state of the IOnDiskStateMachine instance, to the provided
	// io.Writer.
	// SaveSnapshot(interface{}, io.Writer, <-chan struct{}) error

	// RecoverFromSnapshot recovers the state of the IOnDiskStateMachine instance
	// from a snapshot captured by the SaveSnapshot() method on a remote node.
	// RecoverFromSnapshot(io.Reader, <-chan struct{}) error

	// Close closes the IOnDiskStateMachine instance. Close is invoked when the
	// state machine is in a ready-to-exit state in which there will be no further
	// call to the Update, Sync, PrepareSnapshot, SaveSnapshot and the
	// RecoverFromSnapshot method.
	// Our DB is managed by an outsider manager. So, this is a no-op.
	// Close() error

	// LastApplied returns the last applied index of the state machine.
	LastApplied() (raftID, error)
}
