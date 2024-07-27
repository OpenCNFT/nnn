package raft

import (
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/keyvalue"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
	"google.golang.org/protobuf/proto"
)

func TestDbForMetadataGroup(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg := testcfg.Build(t, testcfg.WithStorages("node-1"))

	dbMgr, ptnManager := setupTestDBManagerAndPartitionManager(t, cfg)

	db := dbForMetadataGroup(ptnManager, "node-1")
	require.NoError(t, db.write(ctx, func(txn keyvalue.ReadWriter) error {
		require.NoError(t, txn.Set([]byte("data-1"), []byte("one")))
		return nil
	}))

	require.EqualError(t, db.read(ctx, func(txn keyvalue.ReadWriter) error {
		require.EqualError(
			t,
			txn.Set([]byte("data-2"), []byte("two")),
			"No sets or deletes are allowed in a read-only transaction",
		)
		return nil
	}), "key-value writes in a read-only transaction")

	require.NoError(t, db.read(ctx, func(txn keyvalue.ReadWriter) error {
		item, err := txn.Get([]byte("data-1"))
		require.NoError(t, err)

		require.NoError(t, item.Value(func(value []byte) error {
			require.Equal(t, value, []byte("one"))
			return nil
		}))
		return nil
	}))

	store, err := dbMgr.GetDB("node-1")
	require.NoError(t, err)

	lastAppliedLSN, err := proto.Marshal(storage.LSN(1).ToProto())
	require.NoError(t, err)

	expectedRawValues := map[string][]byte{
		"p/\x00\x00\x00\x00\x00\x00\x00\x01/applied_lsn":            lastAppliedLSN,
		"p/\x00\x00\x00\x00\x00\x00\x00\x01/kv/raft/cluster/data-1": []byte("one"),
	}

	require.NoError(t, store.View(func(txn keyvalue.ReadWriter) error {
		it := txn.NewIterator(keyvalue.IteratorOptions{
			Prefix: []byte("p/\x00\x00\x00\x00\x00\x00\x00\x01"),
		})
		defer it.Close()

		values := map[string][]byte{}
		for it.Rewind(); it.Valid(); it.Next() {
			k, err := it.Item().ValueCopy(nil)
			require.NoError(t, err)
			values[string(it.Item().Key())] = k
		}
		require.Equal(t, expectedRawValues, values)
		return nil
	}))
}

func TestDbForStorage(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg := testcfg.Build(t, testcfg.WithStorages("node-1"))

	dbMgr, ptnManager := setupTestDBManagerAndPartitionManager(t, cfg)
	storageInfo := &gitalypb.Storage{
		StorageId:         1,
		Name:              "node-1",
		ReplicationFactor: 3,
		NodeId:            1,
		ReplicaGroups:     []uint64{2, 3},
	}

	db := dbForStorage(ptnManager, "node-1")
	require.NoError(t, db.write(ctx, func(txn keyvalue.ReadWriter) error {
		storage, err := proto.Marshal(storageInfo)
		require.NoError(t, err)

		require.NoError(t, txn.Set([]byte("storage"), storage))
		return nil
	}))

	require.EqualError(t, db.read(ctx, func(txn keyvalue.ReadWriter) error {
		require.EqualError(
			t,
			txn.Set([]byte("data-2"), []byte("two")),
			"No sets or deletes are allowed in a read-only transaction",
		)
		return nil
	}), "key-value writes in a read-only transaction")

	require.NoError(t, db.read(ctx, func(txn keyvalue.ReadWriter) error {
		item, err := txn.Get([]byte("storage"))
		require.NoError(t, err)

		require.NoError(t, item.Value(func(value []byte) error {
			var actualStorageInfo gitalypb.Storage
			require.NoError(t, proto.Unmarshal(value, &actualStorageInfo))
			testhelper.ProtoEqual(t, storageInfo, &actualStorageInfo)
			return nil
		}))
		return nil
	}))

	store, err := dbMgr.GetDB("node-1")
	require.NoError(t, err)

	require.NoError(t, store.View(func(txn keyvalue.ReadWriter) error {
		it := txn.NewIterator(keyvalue.IteratorOptions{
			Prefix: []byte("p/\x00\x00\x00\x00\x00\x00\x00\x01"),
		})
		defer it.Close()

		var keys []string

		for it.Rewind(); it.Valid(); it.Next() {
			keys = append(keys, string(it.Item().Key()))
		}
		require.ElementsMatch(t, []string{
			"p/\x00\x00\x00\x00\x00\x00\x00\x01/applied_lsn",
			"p/\x00\x00\x00\x00\x00\x00\x00\x01/kv/raft/self/storage",
		}, keys)
		return nil
	}))
}

func TestDbForReplicationGroup(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg := testcfg.Build(t, testcfg.WithStorages("node-1"))

	dbMgr, ptnManager := setupTestDBManagerAndPartitionManager(t, cfg)

	db1 := dbForReplicationGroup(ptnManager, 1, "node-1", 1)
	db2 := dbForReplicationGroup(ptnManager, 1, "node-1", 2)
	db3 := dbForReplicationGroup(ptnManager, 1, "node-1", 3)

	require.NoError(t, db1.write(ctx, func(txn keyvalue.ReadWriter) error {
		require.NoError(t, txn.Set([]byte("partitions/1"), []byte("@hashed/aa/bb/aabb")))
		require.NoError(t, txn.Set([]byte("partitions/2"), []byte("@hashed/cc/dd/ccdd")))
		require.NoError(t, txn.Set([]byte("partitions/3"), []byte("@hashed/ii/jj/iijj")))
		return nil
	}))

	require.NoError(t, db2.write(ctx, func(txn keyvalue.ReadWriter) error {
		require.NoError(t, txn.Set([]byte("partitions/1"), []byte("@hashed/aa/bb/aabb")))
		require.NoError(t, txn.Set([]byte("partitions/2"), []byte("@hashed/ee/ff/eeff")))
		return nil
	}))

	require.NoError(t, db3.write(ctx, func(txn keyvalue.ReadWriter) error {
		require.NoError(t, txn.Set([]byte("partitions/1"), []byte("@hashed/aa/bb/aabb")))
		require.NoError(t, txn.Set([]byte("partitions/2"), []byte("@hashed/gg/hh/gghh")))
		return nil
	}))

	store, err := dbMgr.GetDB("node-1")
	require.NoError(t, err)

	require.NoError(t, store.View(func(txn keyvalue.ReadWriter) error {
		it := txn.NewIterator(keyvalue.IteratorOptions{
			Prefix: []byte("p/\x00\x00\x00\x00\x00\x00\x00\x01/kv/raft/"),
		})
		defer it.Close()

		values := map[string][]byte{}
		for it.Rewind(); it.Valid(); it.Next() {
			k, err := it.Item().ValueCopy(nil)
			require.NoError(t, err)
			values[string(it.Item().Key())] = k
		}

		require.Equal(t, map[string][]byte{
			"p/\x00\x00\x00\x00\x00\x00\x00\x01/kv/raft/authority/partitions/1":                                 []byte("@hashed/aa/bb/aabb"),
			"p/\x00\x00\x00\x00\x00\x00\x00\x01/kv/raft/authority/partitions/2":                                 []byte("@hashed/cc/dd/ccdd"),
			"p/\x00\x00\x00\x00\x00\x00\x00\x01/kv/raft/authority/partitions/3":                                 []byte("@hashed/ii/jj/iijj"),
			"p/\x00\x00\x00\x00\x00\x00\x00\x01/kv/raft/replicas/\x00\x00\x00\x00\x00\x00\x00\x02/partitions/1": []byte("@hashed/aa/bb/aabb"),
			"p/\x00\x00\x00\x00\x00\x00\x00\x01/kv/raft/replicas/\x00\x00\x00\x00\x00\x00\x00\x02/partitions/2": []byte("@hashed/ee/ff/eeff"),
			"p/\x00\x00\x00\x00\x00\x00\x00\x01/kv/raft/replicas/\x00\x00\x00\x00\x00\x00\x00\x03/partitions/1": []byte("@hashed/aa/bb/aabb"),
			"p/\x00\x00\x00\x00\x00\x00\x00\x01/kv/raft/replicas/\x00\x00\x00\x00\x00\x00\x00\x03/partitions/2": []byte("@hashed/gg/hh/gghh"),
		}, values)
		return nil
	}))
}
