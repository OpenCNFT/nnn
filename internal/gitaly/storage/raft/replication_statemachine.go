package raft

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/dgraph-io/badger/v4"
	"github.com/lni/dragonboat/v4/statemachine"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/keyvalue"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
	"google.golang.org/protobuf/proto"
)

// partitionOpType is an operation type, denoting the operation to perform for a partition
// managed by this Raft group.
type partitionOpType byte

const (
	// opTrackPartition tells the observer to track a partition. There are two occasions:
	// - When the Raft group is restarted.
	// - When the Raft group detects a new partition creation.
	opTrackPartition = partitionOpType(iota)
)

// partitionOperation defines an operation to perform on a partition. From the scope of replication
// metadata, only partition creation and deletion are supported.
type partitionOperation struct {
	op        partitionOpType
	partition *gitalypb.Partition
}

const (
	resultRegisterPartitionSuccessfully = updateResult(iota)
	resultRegisterPartitionExisted
)

// replicationStateMachine is a state machine that manages the data for a replication Raft group.
// It manages two types of objects:
// - If the replication Raft group is for an authority, the state machine manages the partitions
// created by that authority storage.
// - If the replication Raft group is for a replica, the state machine manages the partitions the
// replica should manage.
// The statemachine triggers the configured callback when it receives new creation/deletion
// operations.
type replicationStateMachine struct {
	ctx         context.Context
	db          dbAccessor
	authority   bool
	storageID   raftID
	callback    func([]*partitionOperation)
	callbackOps []*partitionOperation
}

// Open initializes the replication state machine and returns the last applied index. The data are
// persisted to disk by the keyvalue package, so we don't maintain a separate in-memory
// representation here.
func (s *replicationStateMachine) Open(stopC <-chan struct{}) (uint64, error) {
	lastApplied, err := s.LastApplied()
	if err != nil {
		return 0, fmt.Errorf("reading last index from DB: %w", err)
	}

	select {
	case <-stopC:
		return 0, statemachine.ErrOpenStopped
	default:
		return lastApplied.ToUint64(), nil
	}
}

// LastApplied returns the last applied index of the state machine.
func (s *replicationStateMachine) LastApplied() (lastApplied raftID, err error) {
	return lastApplied, s.db.read(s.ctx, func(txn keyvalue.ReadWriter) error {
		lastApplied, err = s.getLastIndex(txn)
		if err != nil {
			err = fmt.Errorf("getting last index from DB: %w", err)
		}
		return err
	})
}

// Update applies entry to replication group. It supports the only operation now:
// - *gitalypb.RegisterPartitionRequest
func (s *replicationStateMachine) Update(entries []statemachine.Entry) ([]statemachine.Entry, error) {
	defer func() {
		// Always reset the list of operations for callback, regardless of the update result.
		s.callbackOps = []*partitionOperation{}
	}()
	var returnedEntries []statemachine.Entry

	if err := s.db.write(s.ctx, func(txn keyvalue.ReadWriter) error {
		entries, err := s.update(txn, entries)
		if err != nil {
			return err
		}
		returnedEntries = entries
		return nil
	}); err != nil {
		return nil, err
	}

	if len(s.callbackOps) > 0 {
		s.callback(s.callbackOps)
	}
	return returnedEntries, nil
}

func (s *replicationStateMachine) update(txn keyvalue.ReadWriter, entries []statemachine.Entry) (_ []statemachine.Entry, returnedErr error) {
	lastApplied, err := s.getLastIndex(txn)
	if err != nil {
		return nil, fmt.Errorf("reading last index from DB: %w", err)
	}

	var returnedEntries []statemachine.Entry
	for _, entry := range entries {
		if lastApplied >= raftID(entry.Index) {
			return nil, fmt.Errorf("log entry with previously applied index, last applied %d entry index %d", lastApplied, entry.Index)
		}
		result, err := s.updateEntry(txn, &entry)
		if err != nil {
			return nil, fmt.Errorf("updating entry index %d: %w", entry.Index, err)
		}
		returnedEntries = append(returnedEntries, statemachine.Entry{
			Index:  entry.Index,
			Result: *result,
		})
		lastApplied = raftID(entry.Index)
	}
	if err := txn.Set(keyLastApplied, lastApplied.MarshalBinary()); err != nil {
		return nil, fmt.Errorf("setting last index: %w", err)
	}
	return returnedEntries, nil
}

func (s *replicationStateMachine) updateEntry(txn keyvalue.ReadWriter, entry *statemachine.Entry) (*statemachine.Result, error) {
	var result statemachine.Result

	msg, err := anyProtoUnmarshal(entry.Cmd)
	if err != nil {
		return nil, fmt.Errorf("unmarshalling command: %w", err)
	}

	switch req := msg.(type) {
	case *gitalypb.RegisterPartitionRequest:
		returnedPartition, err := s.upsertPartition(txn, req.GetPartition())
		if err != nil {
			return nil, fmt.Errorf("handling RegisterPartitionRequest: %w", err)
		}

		if returnedPartition == nil {
			returnedPartition = req.GetPartition()
			result.Value = uint64(resultRegisterPartitionSuccessfully)

			s.callbackOps = append(s.callbackOps, &partitionOperation{
				op:        opTrackPartition,
				partition: returnedPartition,
			})
		} else {
			result.Value = uint64(resultRegisterPartitionExisted)
		}

		response, err := anyProtoMarshal(&gitalypb.RegisterPartitionResponse{Partition: returnedPartition})
		if err != nil {
			return nil, fmt.Errorf("marshaling RegisterPartitionResponse: %w", err)
		}
		result.Data = response

		return &result, nil
	default:
		return nil, fmt.Errorf("request not supported: %s", msg.ProtoReflect().Descriptor().Name())
	}
}

// Lookup queries the state machine. It supports the only operation now:
// - *gitalypb.GetRegisteredPartitionsRequest
func (s *replicationStateMachine) Lookup(cmd interface{}) (interface{}, error) {
	switch cmd.(type) {
	case *gitalypb.GetRegisteredPartitionsRequest:
		response := &gitalypb.GetRegisteredPartitionsResponse{}

		if err := s.db.read(s.ctx, func(txn keyvalue.ReadWriter) error {
			it := txn.NewIterator(keyvalue.IteratorOptions{
				Prefix: []byte(s.storagePrefix(s.storageID)),
			})
			defer it.Close()

			for it.Rewind(); it.Valid(); it.Next() {
				if err := it.Item().Value(func(value []byte) error {
					var partition gitalypb.Partition
					if err := proto.Unmarshal(value, &partition); err != nil {
						return fmt.Errorf("unmarshalling partition from DB: %w", err)
					}
					response.Partitions = append(response.Partitions, &partition)

					return nil
				}); err != nil {
					return fmt.Errorf("iterating through partitions: %w", err)
				}
			}
			return nil
		}); err != nil {
			return nil, fmt.Errorf("reading registered partitions: %w", err)
		}

		return response, nil
	default:
		return nil, fmt.Errorf("request not supported: %T", cmd)
	}
}

// Sync is a no-op because our DB flushes to disk on commit.
func (s *replicationStateMachine) Sync() error { return nil }

// PrepareSnapshot is a no-op until we start supporting snapshots.
func (s *replicationStateMachine) PrepareSnapshot() (interface{}, error) {
	return nil, fmt.Errorf("PrepareSnapshot hasn't been not supported")
}

// SaveSnapshot is a no-op until we start supporting snapshots.
func (s *replicationStateMachine) SaveSnapshot(_ interface{}, _ io.Writer, _ <-chan struct{}) error {
	return fmt.Errorf("SaveSnapshot hasn't been not supported")
}

// RecoverFromSnapshot is a no-op until we start supporting snapshots.
func (s *replicationStateMachine) RecoverFromSnapshot(_ io.Reader, _ <-chan struct{}) error {
	return fmt.Errorf("RecoverFromSnapshot hasn't been not supported")
}

// Close is a no-op because our DB is managed externally.
func (s *replicationStateMachine) Close() error { return nil }

func (s *replicationStateMachine) getLastIndex(txn keyvalue.ReadWriter) (raftID, error) {
	var appliedIndex raftID

	item, err := txn.Get(keyLastApplied)
	if err != nil {
		if errors.Is(err, badger.ErrKeyNotFound) {
			return 0, nil
		}
		return 0, err
	}
	return appliedIndex, item.Value(func(value []byte) error {
		appliedIndex.UnmarshalBinary(value)
		return nil
	})
}

func (s *replicationStateMachine) upsertPartition(txn keyvalue.ReadWriter, partition *gitalypb.Partition) (*gitalypb.Partition, error) {
	key := s.partitionKey(partition)
	item, err := txn.Get(key)
	if err != nil {
		if errors.Is(err, badger.ErrKeyNotFound) {
			marshaledPartition, err := proto.Marshal(partition)
			if err != nil {
				return nil, fmt.Errorf("marshaling partition: %w", err)
			}
			if err := txn.Set(key, marshaledPartition); err != nil {
				return nil, fmt.Errorf("saving partition %d of authority storage %d: %w", partition.GetPartitionId(), partition.GetAuthorityId(), err)
			}
			return nil, nil
		}
		return nil, err
	}

	var existingPartition gitalypb.Partition
	return &existingPartition, item.Value(func(value []byte) error {
		return proto.Unmarshal(value, &existingPartition)
	})
}

func (s *replicationStateMachine) partitionKey(partition *gitalypb.Partition) []byte {
	return []byte(fmt.Sprintf("%s%s", s.storagePrefix(raftID(partition.GetAuthorityId())), raftID(partition.GetPartitionId()).MarshalBinary()))
}

func (s *replicationStateMachine) storagePrefix(storageID raftID) string {
	if s.authority {
		return "self/"
	}
	return fmt.Sprintf("%s/", storageID.MarshalBinary())
}

var _ = Statemachine(&replicationStateMachine{})

// newReplicationStateMachine returns a state machine that manages data for a replication Raft group.
func newReplicationStateMachine(ctx context.Context, db dbAccessor, authority bool, storageID raftID, callback func([]*partitionOperation)) *replicationStateMachine {
	return &replicationStateMachine{ctx: ctx, db: db, authority: authority, storageID: storageID, callback: callback}
}
