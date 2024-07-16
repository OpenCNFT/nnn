package raft

import (
	"context"
	"errors"
	"fmt"

	"github.com/dgraph-io/badger/v4"
	"github.com/lni/dragonboat/v4"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/keyvalue"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/storagemgr"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
	"google.golang.org/protobuf/proto"
)

type dbAccessor func(context.Context, bool, func(keyvalue.ReadWriter) error) error

// storageManager is responsible for managing the Raft storage for a single storage. It provides a
// keyvalue.Transactioner for each Raft group, allowing the Raft groups to store their data in the
// underlying keyvalue store.
type storageManager struct {
	id            raftID
	name          string
	ptnMgr        *storagemgr.PartitionManager
	nodeHost      *dragonboat.NodeHost
	persistedInfo *gitalypb.Storage
}

// newStorageManager returns an instance of storage manager.
func newStorageManager(name string, ptnMgr *storagemgr.PartitionManager, nodeHost *dragonboat.NodeHost) *storageManager {
	return &storageManager{
		name:     name,
		ptnMgr:   ptnMgr,
		nodeHost: nodeHost,
	}
}

// Close closes the storage manager.
func (m *storageManager) Close() { m.nodeHost.Close() }

func (m *storageManager) loadStorageInfo(ctx context.Context) error {
	db := m.dbForStorage()
	return db(ctx, false, func(txn keyvalue.ReadWriter) error {
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
			m.id = raftID(m.persistedInfo.StorageId)
			return nil
		})
	})
}

func (m *storageManager) saveStorageInfo(ctx context.Context, storage *gitalypb.Storage) error {
	db := m.dbForStorage()
	return db(ctx, false, func(txn keyvalue.ReadWriter) error {
		_, err := txn.Get([]byte("storage"))
		if err == nil {
			return fmt.Errorf("storage already exists")
		} else if !errors.Is(err, badger.ErrKeyNotFound) {
			return err
		}
		marshaled, err := proto.Marshal(storage)
		if err != nil {
			return err
		}
		if err := txn.Set([]byte("storage"), marshaled); err != nil {
			return err
		}
		m.persistedInfo = storage
		m.id = raftID(m.persistedInfo.StorageId)
		return nil
	})
}

// clearStorageInfo clears the storage info inside the in-memory storage of the storage manager. It
// does not clean the persisted info the DB.
func (m *storageManager) clearStorageInfo() {
	m.id = 0
	m.persistedInfo = nil
}

func (m *storageManager) dbForStorage() dbAccessor {
	return func(ctx context.Context, readOnly bool, fn func(keyvalue.ReadWriter) error) error {
		return m.ptnMgr.StorageKV(ctx, m.name, readOnly, func(rw keyvalue.ReadWriter) error {
			return fn(keyvalue.NewPrefixedReadWriter(rw, []byte("raft")))
		})
	}
}

func (m *storageManager) dbForMetadataGroup() dbAccessor {
	return dbForMetadataGroup(m.ptnMgr, m.name)
}

func dbForMetadataGroup(ptnMgr *storagemgr.PartitionManager, storageName string) dbAccessor {
	return func(ctx context.Context, readOnly bool, fn func(keyvalue.ReadWriter) error) error {
		return ptnMgr.StorageKV(ctx, storageName, readOnly, func(rw keyvalue.ReadWriter) error {
			return fn(keyvalue.NewPrefixedReadWriter(
				rw,
				[]byte(fmt.Sprintf("raft/%s", MetadataGroupID.MarshalBinary())),
			))
		})
	}
}
