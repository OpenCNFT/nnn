package raft

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
)

func TestSimpleRingReplicaPlacement(t *testing.T) {
	t.Parallel()

	t.Run("0 storage", func(t *testing.T) {
		t.Parallel()

		storages := map[uint64]*gitalypb.Storage{}
		newSimpleRingReplicaPlacement().apply(storages)
		require.Equal(t, map[uint64]*gitalypb.Storage{}, storages)
	})

	t.Run("1 storage", func(t *testing.T) {
		t.Parallel()

		storages := map[uint64]*gitalypb.Storage{
			1: {StorageId: 1, Name: "storage-1", ReplicationFactor: 3, NodeId: 1},
		}
		newSimpleRingReplicaPlacement().apply(storages)
		require.Equal(t, map[uint64]*gitalypb.Storage{
			1: {StorageId: 1, Name: "storage-1", ReplicationFactor: 3, NodeId: 1, ReplicaGroups: []uint64{}},
		}, storages)
	})

	t.Run("2 storages", func(t *testing.T) {
		t.Parallel()

		storages := map[uint64]*gitalypb.Storage{
			1: {StorageId: 1, Name: "storage-1", ReplicationFactor: 3, NodeId: 1},
			2: {StorageId: 2, Name: "storage-2", ReplicationFactor: 3, NodeId: 2},
		}
		newSimpleRingReplicaPlacement().apply(storages)
		require.Equal(t, map[uint64]*gitalypb.Storage{
			1: {StorageId: 1, Name: "storage-1", ReplicationFactor: 3, NodeId: 1, ReplicaGroups: []uint64{2}},
			2: {StorageId: 2, Name: "storage-2", ReplicationFactor: 3, NodeId: 2, ReplicaGroups: []uint64{1}},
		}, storages)
	})

	t.Run("3 storages", func(t *testing.T) {
		t.Parallel()

		storages := map[uint64]*gitalypb.Storage{
			1: {StorageId: 1, Name: "storage-1", ReplicationFactor: 3, NodeId: 1},
			2: {StorageId: 2, Name: "storage-2", ReplicationFactor: 3, NodeId: 2},
			3: {StorageId: 3, Name: "storage-3", ReplicationFactor: 3, NodeId: 3},
		}
		newSimpleRingReplicaPlacement().apply(storages)
		require.Equal(t, map[uint64]*gitalypb.Storage{
			1: {StorageId: 1, Name: "storage-1", ReplicationFactor: 3, NodeId: 1, ReplicaGroups: []uint64{2, 3}},
			2: {StorageId: 2, Name: "storage-2", ReplicationFactor: 3, NodeId: 2, ReplicaGroups: []uint64{3, 1}},
			3: {StorageId: 3, Name: "storage-3", ReplicationFactor: 3, NodeId: 3, ReplicaGroups: []uint64{1, 2}},
		}, storages)
	})

	t.Run("more than 3 storages", func(t *testing.T) {
		t.Parallel()

		storages := map[uint64]*gitalypb.Storage{
			1: {StorageId: 1, Name: "storage-1", ReplicationFactor: 3, NodeId: 1},
			2: {StorageId: 2, Name: "storage-2", ReplicationFactor: 3, NodeId: 2},
			3: {StorageId: 3, Name: "storage-3", ReplicationFactor: 3, NodeId: 3},
			4: {StorageId: 4, Name: "storage-4", ReplicationFactor: 3, NodeId: 4},
			5: {StorageId: 5, Name: "storage-5", ReplicationFactor: 3, NodeId: 5},
		}
		newSimpleRingReplicaPlacement().apply(storages)
		require.Equal(t, map[uint64]*gitalypb.Storage{
			1: {StorageId: 1, Name: "storage-1", ReplicationFactor: 3, NodeId: 1, ReplicaGroups: []uint64{2, 3}},
			2: {StorageId: 2, Name: "storage-2", ReplicationFactor: 3, NodeId: 2, ReplicaGroups: []uint64{3, 4}},
			3: {StorageId: 3, Name: "storage-3", ReplicationFactor: 3, NodeId: 3, ReplicaGroups: []uint64{4, 5}},
			4: {StorageId: 4, Name: "storage-4", ReplicationFactor: 3, NodeId: 4, ReplicaGroups: []uint64{5, 1}},
			5: {StorageId: 5, Name: "storage-5", ReplicationFactor: 3, NodeId: 5, ReplicaGroups: []uint64{1, 2}},
		}, storages)
	})

	t.Run("more than 3 storages in random order", func(t *testing.T) {
		t.Parallel()

		storages := map[uint64]*gitalypb.Storage{
			4: {StorageId: 4, Name: "storage-4", ReplicationFactor: 3, NodeId: 4},
			1: {StorageId: 1, Name: "storage-1", ReplicationFactor: 3, NodeId: 1},
			5: {StorageId: 5, Name: "storage-5", ReplicationFactor: 3, NodeId: 5},
			2: {StorageId: 2, Name: "storage-2", ReplicationFactor: 3, NodeId: 2},
			3: {StorageId: 3, Name: "storage-3", ReplicationFactor: 3, NodeId: 3},
		}
		newSimpleRingReplicaPlacement().apply(storages)
		require.Equal(t, map[uint64]*gitalypb.Storage{
			1: {StorageId: 1, Name: "storage-1", ReplicationFactor: 3, NodeId: 1, ReplicaGroups: []uint64{2, 3}},
			2: {StorageId: 2, Name: "storage-2", ReplicationFactor: 3, NodeId: 2, ReplicaGroups: []uint64{3, 4}},
			3: {StorageId: 3, Name: "storage-3", ReplicationFactor: 3, NodeId: 3, ReplicaGroups: []uint64{4, 5}},
			4: {StorageId: 4, Name: "storage-4", ReplicationFactor: 3, NodeId: 4, ReplicaGroups: []uint64{5, 1}},
			5: {StorageId: 5, Name: "storage-5", ReplicationFactor: 3, NodeId: 5, ReplicaGroups: []uint64{1, 2}},
		}, storages)
	})

	t.Run("discontinued eligible storages", func(t *testing.T) {
		t.Parallel()

		storages := map[uint64]*gitalypb.Storage{
			1: {StorageId: 1, Name: "storage-1", ReplicationFactor: 3, NodeId: 1},
			2: {StorageId: 2, Name: "storage-2", ReplicationFactor: 3, NodeId: 2},
			3: {StorageId: 3, Name: "storage-3", ReplicationFactor: 3, NodeId: 3},
			5: {StorageId: 5, Name: "storage-5", ReplicationFactor: 3, NodeId: 5},
			6: {StorageId: 6, Name: "storage-6", ReplicationFactor: 3, NodeId: 6},
		}
		newSimpleRingReplicaPlacement().apply(storages)
		require.Equal(t, map[uint64]*gitalypb.Storage{
			1: {StorageId: 1, Name: "storage-1", ReplicationFactor: 3, NodeId: 1, ReplicaGroups: []uint64{2, 3}},
			2: {StorageId: 2, Name: "storage-2", ReplicationFactor: 3, NodeId: 2, ReplicaGroups: []uint64{3, 5}},
			3: {StorageId: 3, Name: "storage-3", ReplicationFactor: 3, NodeId: 3, ReplicaGroups: []uint64{5, 6}},
			5: {StorageId: 5, Name: "storage-5", ReplicationFactor: 3, NodeId: 5, ReplicaGroups: []uint64{6, 1}},
			6: {StorageId: 6, Name: "storage-6", ReplicationFactor: 3, NodeId: 6, ReplicaGroups: []uint64{1, 2}},
		}, storages)
	})
	t.Run("storages residing on the same node", func(t *testing.T) {
		t.Parallel()

		storages := map[uint64]*gitalypb.Storage{
			1: {StorageId: 1, Name: "storage-1", ReplicationFactor: 3, NodeId: 1},
			2: {StorageId: 2, Name: "storage-2", ReplicationFactor: 3, NodeId: 1},
			3: {StorageId: 3, Name: "storage-3", ReplicationFactor: 3, NodeId: 2},
			4: {StorageId: 4, Name: "storage-4", ReplicationFactor: 3, NodeId: 2},
			5: {StorageId: 5, Name: "storage-5", ReplicationFactor: 3, NodeId: 3},
		}
		newSimpleRingReplicaPlacement().apply(storages)
		require.Equal(t, map[uint64]*gitalypb.Storage{
			1: {StorageId: 1, Name: "storage-1", ReplicationFactor: 3, NodeId: 1, ReplicaGroups: []uint64{3, 4}},
			2: {StorageId: 2, Name: "storage-2", ReplicationFactor: 3, NodeId: 1, ReplicaGroups: []uint64{3, 4}},
			3: {StorageId: 3, Name: "storage-3", ReplicationFactor: 3, NodeId: 2, ReplicaGroups: []uint64{5, 1}},
			4: {StorageId: 4, Name: "storage-4", ReplicationFactor: 3, NodeId: 2, ReplicaGroups: []uint64{5, 1}},
			5: {StorageId: 5, Name: "storage-5", ReplicationFactor: 3, NodeId: 3, ReplicaGroups: []uint64{1, 2}},
		}, storages)
	})

	t.Run("storages have different replication factors", func(t *testing.T) {
		t.Parallel()

		storages := map[uint64]*gitalypb.Storage{
			1: {StorageId: 1, Name: "storage-1", ReplicationFactor: 3, NodeId: 1},
			2: {StorageId: 2, Name: "storage-2", ReplicationFactor: 5, NodeId: 2},
			3: {StorageId: 3, Name: "storage-3", ReplicationFactor: 3, NodeId: 3},
			4: {StorageId: 4, Name: "storage-4", ReplicationFactor: 3, NodeId: 4},
			5: {StorageId: 5, Name: "storage-5", ReplicationFactor: 1, NodeId: 5},
			6: {StorageId: 6, Name: "storage-6", ReplicationFactor: 1, NodeId: 6},
			7: {StorageId: 7, Name: "storage-7", ReplicationFactor: 3, NodeId: 7},
		}
		newSimpleRingReplicaPlacement().apply(storages)
		require.Equal(t, map[uint64]*gitalypb.Storage{
			1: {StorageId: 1, Name: "storage-1", ReplicationFactor: 3, NodeId: 1, ReplicaGroups: []uint64{2, 3}},
			2: {StorageId: 2, Name: "storage-2", ReplicationFactor: 5, NodeId: 2, ReplicaGroups: []uint64{3, 4, 5, 6}},
			3: {StorageId: 3, Name: "storage-3", ReplicationFactor: 3, NodeId: 3, ReplicaGroups: []uint64{4, 5}},
			4: {StorageId: 4, Name: "storage-4", ReplicationFactor: 3, NodeId: 4, ReplicaGroups: []uint64{5, 6}},
			5: {StorageId: 5, Name: "storage-5", ReplicationFactor: 1, NodeId: 5, ReplicaGroups: []uint64{}},
			6: {StorageId: 6, Name: "storage-6", ReplicationFactor: 1, NodeId: 6, ReplicaGroups: []uint64{}},
			7: {StorageId: 7, Name: "storage-7", ReplicationFactor: 3, NodeId: 7, ReplicaGroups: []uint64{1, 2}},
		}, storages)
	})

	t.Run("fixup existing replica groups", func(t *testing.T) {
		t.Parallel()

		storages := map[uint64]*gitalypb.Storage{
			// Result of assigning replica groups when there are 2 storages.
			1: {StorageId: 1, Name: "storage-1", ReplicationFactor: 3, NodeId: 1, ReplicaGroups: []uint64{2}},
			2: {StorageId: 2, Name: "storage-2", ReplicationFactor: 3, NodeId: 2, ReplicaGroups: []uint64{1}},
			// This one is new.
			3: {StorageId: 3, Name: "storage-3", ReplicationFactor: 3, NodeId: 3},
		}
		newSimpleRingReplicaPlacement().apply(storages)
		require.Equal(t, map[uint64]*gitalypb.Storage{
			1: {StorageId: 1, Name: "storage-1", ReplicationFactor: 3, NodeId: 1, ReplicaGroups: []uint64{2, 3}},
			2: {StorageId: 2, Name: "storage-2", ReplicationFactor: 3, NodeId: 2, ReplicaGroups: []uint64{3, 1}},
			3: {StorageId: 3, Name: "storage-3", ReplicationFactor: 3, NodeId: 3, ReplicaGroups: []uint64{1, 2}},
		}, storages)
	})

	t.Run("add new storages", func(t *testing.T) {
		t.Parallel()

		storages := map[uint64]*gitalypb.Storage{
			1: {StorageId: 1, Name: "storage-1", ReplicationFactor: 3, NodeId: 1},
		}
		for i := uint64(2); i <= 7; i++ {
			storages[i] = &gitalypb.Storage{
				StorageId:         i,
				Name:              fmt.Sprintf("storage-%d", i),
				ReplicationFactor: 3,
				NodeId:            i,
			}
			newSimpleRingReplicaPlacement().apply(storages)
		}
		require.Equal(t, map[uint64]*gitalypb.Storage{
			1: {StorageId: 1, Name: "storage-1", ReplicationFactor: 3, NodeId: 1, ReplicaGroups: []uint64{2, 3}},
			2: {StorageId: 2, Name: "storage-2", ReplicationFactor: 3, NodeId: 2, ReplicaGroups: []uint64{3, 4}},
			3: {StorageId: 3, Name: "storage-3", ReplicationFactor: 3, NodeId: 3, ReplicaGroups: []uint64{4, 5}},
			4: {StorageId: 4, Name: "storage-4", ReplicationFactor: 3, NodeId: 4, ReplicaGroups: []uint64{5, 6}},
			5: {StorageId: 5, Name: "storage-5", ReplicationFactor: 3, NodeId: 5, ReplicaGroups: []uint64{6, 7}},
			6: {StorageId: 6, Name: "storage-6", ReplicationFactor: 3, NodeId: 6, ReplicaGroups: []uint64{7, 1}},
			7: {StorageId: 7, Name: "storage-7", ReplicationFactor: 3, NodeId: 7, ReplicaGroups: []uint64{1, 2}},
		}, storages)
	})

	t.Run("remove storages", func(t *testing.T) {
		t.Parallel()

		storages := map[uint64]*gitalypb.Storage{
			1: {StorageId: 1, Name: "storage-1", ReplicationFactor: 3, NodeId: 1, ReplicaGroups: []uint64{2, 3}},
			2: {StorageId: 2, Name: "storage-2", ReplicationFactor: 3, NodeId: 2, ReplicaGroups: []uint64{3, 4}},
			3: {StorageId: 3, Name: "storage-3", ReplicationFactor: 3, NodeId: 3, ReplicaGroups: []uint64{4, 5}},
			4: {StorageId: 4, Name: "storage-4", ReplicationFactor: 3, NodeId: 4, ReplicaGroups: []uint64{5, 6}},
			5: {StorageId: 5, Name: "storage-5", ReplicationFactor: 3, NodeId: 5, ReplicaGroups: []uint64{6, 7}},
			6: {StorageId: 6, Name: "storage-6", ReplicationFactor: 3, NodeId: 6, ReplicaGroups: []uint64{7, 1}},
			7: {StorageId: 7, Name: "storage-7", ReplicationFactor: 3, NodeId: 7, ReplicaGroups: []uint64{1, 2}},
		}
		for i := uint64(4); i <= 7; i++ {
			delete(storages, i)
			newSimpleRingReplicaPlacement().apply(storages)
		}
		require.Equal(t, map[uint64]*gitalypb.Storage{
			1: {StorageId: 1, Name: "storage-1", ReplicationFactor: 3, NodeId: 1, ReplicaGroups: []uint64{2, 3}},
			2: {StorageId: 2, Name: "storage-2", ReplicationFactor: 3, NodeId: 2, ReplicaGroups: []uint64{3, 1}},
			3: {StorageId: 3, Name: "storage-3", ReplicationFactor: 3, NodeId: 3, ReplicaGroups: []uint64{1, 2}},
		}, storages)
	})
}
