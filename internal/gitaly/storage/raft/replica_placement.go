package raft

import (
	"slices"

	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
	"golang.org/x/exp/maps"
)

// replicaPlacement is an interface for a function that appoint replicas for
// all storages of a Raft cluster.
type replicaPlacement interface {
	apply(map[uint64]*gitalypb.Storage)
}

// simpleRingReplicaPlacement implements a deterministic replica placement. It implements a simple
// ring-based placement strategy where each storage is assigned replicas based on the next storage
// nodes in the ring, wrapping around if necessary. This approach ensures a balanced distribution of
// replicas across the available storage nodes. Storages are allowed to have different replication
// factors, the default value is 3.
//
// For example, consider a scenario with 5 storages: 1, 2, 3, 4, 5. The replica placement will be:
// - Storage 1 (authority), replicas on storage 2 and storage 3
// - Storage 2 (authority), replicas on storage 3 and storage 4
// - Storage 3 (authority): replicas on storage 4 and storage 5
// - Storage 4 (authority): replicas on storage 5 and storage 1
// - Storage 5 (authority): replicas on storage 1 and storage 2
//
// If the replication factor is more than the number of nodes, the strategy does its best to fill in
// the gaps. For example, with a replication factor of 5 and there are 3 storages, the replica
// placement will be:
// - Storage 1 (authority): replicas on storage 2 and storage 3
// - Storage 2 (authority): replicas on storage 3 and storage 1
// - Storage 3 (authority): replicas on storage 1 and storage 2
//
// This strategy also takes storage residence into account. It means storages residing on the same
// nodes don't replicate to each other.
//
// The storages are not necessarily contiguous. Replication factor of 0 or 1 means that the storage
// does not replicate.
type simpleRingReplicaPlacement struct{}

func (*simpleRingReplicaPlacement) apply(storages map[uint64]*gitalypb.Storage) {
	ids := maps.Keys(storages)
	slices.Sort(ids)

	for i := range ids {
		// Reset replica groups.
		storage := storages[ids[i]]
		storage.ReplicaGroups = []uint64{}
		j := i
		for k := storage.GetReplicationFactor() - 1; k >= 1; k-- {
			for {
				j = (j + 1) % len(ids)
				// Ensure the other storage is not on the same node.
				if j == i || storages[ids[j]].GetNodeId() != storage.GetNodeId() {
					break
				}
			}
			// Reach the examining storage. It means there are less eligible nodes than needed.
			if j == i {
				break
			}
			storage.ReplicaGroups = append(storage.ReplicaGroups, ids[j])
		}
	}
}

func newSimpleRingReplicaPlacement() replicaPlacement {
	return &simpleRingReplicaPlacement{}
}

// newDefaultReplicaPlacement defines a factory that returns the default replica placements strategy
// used for determining replica groups. At the moment, Gitaly supports a simple ring-based placement
// strategy. When we involve replica placement strategy in the future, all members of the metadata
// Raft group must sync up to ensure they have the same replica placement strategy.
var newDefaultReplicaPlacement = newSimpleRingReplicaPlacement
