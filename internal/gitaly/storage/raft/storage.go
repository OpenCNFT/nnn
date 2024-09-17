package raft

import (
	"context"
	"errors"
	"fmt"

	"github.com/dgraph-io/badger/v4"
	"github.com/lni/dragonboat/v4"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/keyvalue"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
	"google.golang.org/protobuf/proto"
)

// storageManager is responsible for managing the Raft storage for a single storage. It provides a
// keyvalue.Transactioner for each Raft group, allowing the Raft groups to store their data in the
// underlying keyvalue store.
type storageManager struct {
	id            raftID
	name          string
	node          storage.Node
	db            dbAccessor
	nodeHost      *dragonboat.NodeHost
	persistedInfo *gitalypb.Storage
}

// newStorageManager returns an instance of storage manager.
func newStorageManager(name string, node storage.Node, nodeHost *dragonboat.NodeHost) *storageManager {
	return &storageManager{
		name:     name,
		db:       dbForStorage(node, name),
		node:     node,
		nodeHost: nodeHost,
	}
}

// Close closes the storage manager.
func (m *storageManager) Close() { m.nodeHost.Close() }

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
			m.id = raftID(m.persistedInfo.GetStorageId())
			return nil
		})
	})
}

func (m *storageManager) saveStorageInfo(ctx context.Context, storage *gitalypb.Storage) error {
	return m.db.write(ctx, func(txn keyvalue.ReadWriter) error {
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
		m.id = raftID(m.persistedInfo.GetStorageId())
		return nil
	})
}

// clearStorageInfo clears the storage info inside the in-memory storage of the storage manager. It
// does not clean the persisted info the DB.
func (m *storageManager) clearStorageInfo() {
	m.id = 0
	m.persistedInfo = nil
}

func (m *storageManager) dbForMetadataGroup() dbAccessor {
	return dbForMetadataGroup(m.node, m.name)
}
