package raft

import (
	"context"
	"errors"

	"github.com/dgraph-io/badger/v4"
	"github.com/lni/dragonboat/v4"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/keyvalue"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/storagemgr"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
	"google.golang.org/protobuf/proto"
)

// storageManager is responsible for managing the Raft storage for a single storage. It provides a
// keyvalue.Transactioner for each Raft group, allowing the Raft groups to store their data in the
// underlying keyvalue store.
type storageManager struct {
	name          string
	ptnMgr        *storagemgr.PartitionManager
	db            dbAccessor
	nodeHost      *dragonboat.NodeHost
	persistedInfo *gitalypb.Storage
	replicator    *replicator
}

// newStorageManager returns an instance of storage manager.
func newStorageManager(name string, ptnMgr *storagemgr.PartitionManager, nodeHost *dragonboat.NodeHost) *storageManager {
	return &storageManager{
		name:     name,
		db:       dbForStorage(ptnMgr, name),
		ptnMgr:   ptnMgr,
		nodeHost: nodeHost,
	}
}

// Close closes the storage manager.
func (m *storageManager) Close() (returnedErr error) {
	if m.replicator != nil {
		if err := m.replicator.Close(); err != nil {
			returnedErr = err
		}
	}
	m.nodeHost.Close()
	return
}

// ID returns the ID of the storage from persistent storage.
func (m *storageManager) ID() raftID {
	if m.persistedInfo == nil {
		return 0
	}
	return raftID(m.persistedInfo.GetStorageId())
}

func (m *storageManager) loadStorageInfo(ctx context.Context) error {
	return m.db.read(ctx, func(txn keyvalue.ReadWriter) error {
		item, err := txn.Get([]byte("storage"))
		if err != nil {
			if errors.Is(err, badger.ErrKeyNotFound) {
				return nil
			}
			return err
		}
		return item.Value(func(value []byte) error {
			var persistedInfo gitalypb.Storage
			if err := proto.Unmarshal(value, &persistedInfo); err != nil {
				return err
			}
			m.persistedInfo = &persistedInfo
			return nil
		})
	})
}

func (m *storageManager) saveStorageInfo(ctx context.Context, storage *gitalypb.Storage) error {
	return m.db.write(ctx, func(txn keyvalue.ReadWriter) error {
		marshaled, err := proto.Marshal(storage)
		if err != nil {
			return err
		}
		if err := txn.Set([]byte("storage"), marshaled); err != nil {
			return err
		}
		m.persistedInfo = storage
		return nil
	})
}

func (m *storageManager) dbForMetadataGroup() dbAccessor {
	return dbForMetadataGroup(m.ptnMgr, m.name)
}

func (m *storageManager) dbForReplicationGroup() dbAccessor {
	return dbForReplicationGroup(m.ptnMgr, m.name)
}
