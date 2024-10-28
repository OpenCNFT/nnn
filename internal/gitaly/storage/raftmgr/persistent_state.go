package raftmgr

import (
	"errors"
	"fmt"

	"github.com/dgraph-io/badger/v4"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/keyvalue"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

var (
	// KeyHardState stores the current state of a Node to be saved to DB before Messages are sent.
	KeyHardState = []byte("hard_state")
	// KeyConfState stores the current config state used when generating snapshot.
	KeyConfState = []byte("conf_state")
)

// persistentState is responsible for managing the persistence of Raft's state, log entries, and configuration state,
// using a write-ahead log (WAL) and a KV database for durability. It ensures that state is saved and retrievable across
// restarts and node failures, thus aiding in recovery and consistency within the Raft protocol. Raft states are updated
// after each batch of entry processing. Thus, we cannot use WAL as the persistent storage for that kind of data.
// This struct implements `raft.Storage` interface from etcd/raft library.
type persistentState struct {
	db  keyvalue.Transactioner
	wal storage.WriteAheadLog
}

func newPersistentState(db keyvalue.Transactioner, wal storage.WriteAheadLog) *persistentState {
	return &persistentState{
		db:  db,
		wal: wal,
	}
}

// Entries implements raft.Storage.
func (ps *persistentState) Entries(lo uint64, hi uint64, maxSize uint64) ([]raftpb.Entry, error) {
	firstLSN := uint64(ps.wal.FirstLSN())
	lastLSN := uint64(ps.wal.LastLSN())
	if lo < firstLSN {
		return nil, raft.ErrCompacted
	}
	if firstLSN > lastLSN {
		return nil, raft.ErrUnavailable
	}
	if hi > lastLSN+1 {
		return nil, fmt.Errorf("reading out-of-bound entries %d > %d", hi, lastLSN)
	}

	var entries []raftpb.Entry

	for lsn := lo; lsn < hi; lsn++ {
		entry, err := ps.wal.ReadLogEntry(storage.LSN(lsn))
		if err != nil {
			return nil, raft.ErrCompacted
		}
		msg, err := proto.Marshal(entry)
		if err != nil {
			return nil, fmt.Errorf("marshaling log entry: %w", err)
		}
		entries = append(entries, raftpb.Entry{
			Term:  entry.GetMetadata().GetRaftTerm(),
			Index: lsn,
			Type:  raftpb.EntryType(entry.GetMetadata().GetRaftType()),
			Data:  msg,
		})
	}
	return entries, nil
}

// InitialState retrieves the initial Raft HardState and ConfState from persistent storage. It is used to initialize the
// Raft node with the previously saved state.
func (ps *persistentState) InitialState() (raftpb.HardState, raftpb.ConfState, error) {
	hardState, err := ps.readHardState()
	if err != nil {
		return raftpb.HardState{}, raftpb.ConfState{}, fmt.Errorf("reading hard state: %w", err)
	}

	confState, err := ps.readConfState()
	if err != nil {
		return raftpb.HardState{}, raftpb.ConfState{}, fmt.Errorf("reading conf state: %w", err)
	}

	return hardState, confState, nil
}

// LastIndex returns the last index of all entries currently available in the log.
// This corresponds to the last LSN in the write-ahead log.
func (ps *persistentState) LastIndex() (uint64, error) {
	return uint64(ps.wal.LastLSN()), nil
}

// FirstIndex returns the first index of all entries currently available in the log.
// This corresponds to the first LSN in the write-ahead log.
func (ps *persistentState) FirstIndex() (uint64, error) {
	return uint64(ps.wal.FirstLSN()), nil
}

// Snapshot returns the latest snapshot of the state machine. As we haven't supported autocompaction feature, this
// method always returns Unavailable error.
// For more information: https://gitlab.com/gitlab-org/gitaly/-/issues/6463
func (ps *persistentState) Snapshot() (raftpb.Snapshot, error) {
	return raftpb.Snapshot{}, raft.ErrSnapshotTemporarilyUnavailable
}

// Term returns the term of the entry at a given index. It retrieves the term from log entry's metadata. If the
// corresponding log entry is compacted, it retrieves the term from the hard state conditionallly.
func (ps *persistentState) Term(i uint64) (uint64, error) {
	firstLSN := uint64(ps.wal.FirstLSN())
	lastLSN := uint64(ps.wal.LastLSN())
	if i < firstLSN {
		// If firstLSN > lastLSN, it means all log entries are committed and cleaned up from WAL. If the queried
		// log entry is equal to lastLSN, we could imply its term from the hard state.
		if i == lastLSN {
			if hardState, err := ps.readHardState(); err == nil && hardState.Commit == i {
				return hardState.Term, nil
			}
		}
		return 0, raft.ErrCompacted
	}
	if i > lastLSN {
		return 0, raft.ErrUnavailable
	}

	entry, err := ps.wal.ReadLogEntry(storage.LSN(i))
	if err != nil {
		// If the entry does not exist here, there's a good chance WAL cleaned it up after we query the first
		// LSN. Similar to the above case, we could fall back to use hard state.
		if i == lastLSN {
			if hardState, err := ps.readHardState(); err == nil && hardState.Commit == i {
				return hardState.Term, nil
			}
		}
		return 0, fmt.Errorf("reading log entry: %w", err)
	}
	return entry.GetMetadata().GetRaftTerm(), nil
}

// Bootstrapped checks whether the node has completed its initial bootstrap process
// by verifying the existence of a saved hard state.
func (ps *persistentState) Bootstrapped() (bool, error) {
	var hardState gitalypb.RaftHardStateV1
	if err := ps.readKey(KeyHardState, &hardState); err != nil {
		if errors.Is(err, badger.ErrKeyNotFound) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// SaveConfState persists the current Raft configuration state to disk, ensuring that configuration changes are durable.
func (ps *persistentState) SaveHardState(hardState raftpb.HardState) error {
	return ps.setKey(KeyHardState, &gitalypb.RaftHardStateV1{
		Term:   hardState.Term,
		Vote:   hardState.Vote,
		Commit: hardState.Commit,
	})
}

func (ps *persistentState) readHardState() (raftpb.HardState, error) {
	var hardState gitalypb.RaftHardStateV1
	if err := ps.readKey(KeyHardState, &hardState); err != nil {
		if errors.Is(err, badger.ErrKeyNotFound) {
			return raftpb.HardState{}, nil
		}
		return raftpb.HardState{}, err
	}
	return raftpb.HardState{
		Term:   hardState.GetTerm(),
		Vote:   hardState.GetVote(),
		Commit: hardState.GetCommit(),
	}, nil
}

// SaveConfState persists node's current conf state. It is used when generating snapshot.
func (ps *persistentState) SaveConfState(confState *raftpb.ConfState) error {
	return ps.setKey(KeyConfState, &gitalypb.RaftConfStateV1{
		Voters:         confState.Voters,
		Learners:       confState.Learners,
		VotersOutgoing: confState.VotersOutgoing,
		LearnersNext:   confState.LearnersNext,
		AutoLeave:      confState.AutoLeave,
	})
}

func (ps *persistentState) readConfState() (raftpb.ConfState, error) {
	var confState gitalypb.RaftConfStateV1
	if err := ps.readKey(KeyConfState, &confState); err != nil {
		if errors.Is(err, badger.ErrKeyNotFound) {
			return raftpb.ConfState{}, nil
		}
		return raftpb.ConfState{}, err
	}
	return raftpb.ConfState{
		Voters:         confState.GetVoters(),
		Learners:       confState.GetLearners(),
		VotersOutgoing: confState.GetVotersOutgoing(),
		LearnersNext:   confState.GetLearnersNext(),
		AutoLeave:      confState.GetAutoLeave(),
	}, nil
}

// setKey marshals and stores a given protocol buffer message into the database under the given key.
func (ps *persistentState) setKey(key []byte, value proto.Message) error {
	marshaledValue, err := proto.Marshal(value)
	if err != nil {
		return fmt.Errorf("marshal value: %w", err)
	}

	writeBatch := ps.db.NewWriteBatch()
	defer writeBatch.Cancel()

	if err := writeBatch.Set(key, marshaledValue); err != nil {
		return fmt.Errorf("set: %w", err)
	}

	return writeBatch.Flush()
}

// readKey reads a key from the database and unmarshals its value in to the destination protocol
// buffer message.
func (ps *persistentState) readKey(key []byte, destination proto.Message) error {
	return ps.db.View(func(txn keyvalue.ReadWriter) error {
		item, err := txn.Get(key)
		if err != nil {
			return fmt.Errorf("get: %w", err)
		}

		return item.Value(func(value []byte) error { return proto.Unmarshal(value, destination) })
	})
}

// Compile-time type check.
var _ = (raft.Storage)(&persistentState{})
