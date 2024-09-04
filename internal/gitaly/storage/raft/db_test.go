package raft

import (
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/catfile"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/housekeeping"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/keyvalue"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/storagemgr"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/storagemgr/partition"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/storagemgr/snapshot"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
	"google.golang.org/protobuf/proto"
)

func TestDbForMetadataGroup(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg := testcfg.Build(t, testcfg.WithStorages("node-1"))

	logger := testhelper.NewLogger(t)
	dbMgr := setupTestDBManager(t, cfg)

	cmdFactory := gittest.NewCommandFactory(t, cfg)
	catfileCache := catfile.NewCache(cfg)
	t.Cleanup(catfileCache.Stop)

	localRepoFactory := localrepo.NewFactory(logger, config.NewLocator(cfg), cmdFactory, catfileCache)

	partitionManager, err := storagemgr.NewPartitionManager(testhelper.Context(t), cfg.Storages, logger, dbMgr, cfg.Prometheus, partition.NewFactory(
		cmdFactory,
		localRepoFactory,
		partition.NewTransactionManagerMetrics(housekeeping.NewMetrics(cfg.Prometheus), snapshot.NewMetrics()),
	), nil)
	require.NoError(t, err)
	t.Cleanup(partitionManager.Close)

	db := dbForMetadataGroup(partitionManager, "node-1")
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
	}), "commit: key-value writes in a read-only transaction\nrollback: transaction already committed")

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

	logger := testhelper.NewLogger(t)
	dbMgr := setupTestDBManager(t, cfg)

	cmdFactory := gittest.NewCommandFactory(t, cfg)
	catfileCache := catfile.NewCache(cfg)
	t.Cleanup(catfileCache.Stop)

	localRepoFactory := localrepo.NewFactory(logger, config.NewLocator(cfg), cmdFactory, catfileCache)

	partitionManager, err := storagemgr.NewPartitionManager(testhelper.Context(t), cfg.Storages, logger, dbMgr, cfg.Prometheus, partition.NewFactory(
		cmdFactory,
		localRepoFactory,
		partition.NewTransactionManagerMetrics(housekeeping.NewMetrics(cfg.Prometheus), snapshot.NewMetrics()),
	), nil)
	require.NoError(t, err)
	t.Cleanup(partitionManager.Close)

	storageInfo := &gitalypb.Storage{
		StorageId:         1,
		Name:              "node-1",
		ReplicationFactor: 3,
		NodeId:            1,
		ReplicaGroups:     []uint64{2, 3},
	}

	db := dbForStorage(partitionManager, "node-1")
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
	}), "commit: key-value writes in a read-only transaction\nrollback: transaction already committed")

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
