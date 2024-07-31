package raft

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"gitlab.com/gitlab-org/gitaly/v16/internal/log"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
)

// defaultReplicatorTimeout defines the default timeout period used for replicator's operations.
const defaultReplicatorTimeout = 30 * time.Second

// replica is an interface that represents avavailable actions of a replica group.
type replica interface {
	PollPartitions() ([]*partitionOperation, error)
	WaitReady() error
	StorageID() raftID
	StorageName() string
	Close() error
}

// authority is an interface that represents the actions of an authority group.
type authority interface {
	replica

	// RegisterPartition registers a partition with the authority.
	RegisterPartition(partitionID raftID, relativePath string) (*gitalypb.Partition, error)
	// RegisterReplica registers a replica to the list of known replicas of the authority.
	RegisterReplica(ctx context.Context, nodeID raftID, replicaAddr string) error
}

// storageTracker tracks the partition deletion/creation activities of a replica.
type storageTracker struct {
	ctx    context.Context
	cancel context.CancelFunc
	done   chan struct{}
	poller replica
}

type replicatorConfig struct {
	initAuthorityGroup func(storage *gitalypb.Storage) (authority, error)
	initReplicaGroup   func(authorityID raftID, storage *gitalypb.Storage) (replica, error)
	initPartitionGroup func(authorityID raftID, partitionID raftID, relativePath string) error
	getNodeAddr        func(nodeID raftID) (string, error)
}

// replicator manages the replication activities of a storage, including:
// - Apply the list replica groups.
// - Poll the list of partitions under management and start/stop corresponding partition groups accordingly.
// - Manage the authority storage of this node.
// - Push any changes from partitions of that authority storages if any.
// - Manage the replicas assigned to this storage.
// - Poll and apply any changes from partitions of replicas if any.
//
/*
                                         ┌─►Partition Group N ──► Other nodes
                                         │
                                         ├─►Partition Group 2 ──► Other nodes
                            Push changes │
                                         ├─►Partition Group 1 ──► Other nodes
                                         │
                                         │
                               ┌─────► Authority Group
                               │
    Push authority changes     │                  Poll Changes
WAL ────────────────────► Replicator  ◄─────────────────────────────┐
 ▲                         │   │                                    │
 │                         │   │                                    │
 └─────────────────────────┘   ├────── Replica GroupsA              │
     Apply replica changes     │          │                         │
                               │          │                         │
                               │          ├──Partition Group 1  │   │
                               │          │                     │   │
                               │          ├──Partition Group 2  ├───┤
                               │          │                     │   │
                               │          └──Partition Group N  │   │
                               │                                    │
                               └────── Replica GroupsB              │
                                          │                         │
                                          │                         │
                                          ├──Partition Group 1  │   │
                                          │                     │   │
                                          ├──Partition Group 2  ├───┘
                                          │                     │
                                          └──Partition Group N  │
*/
type replicator struct {
	ctx             context.Context
	logger          log.Logger
	config          replicatorConfig
	authorityID     raftID
	authority       authority
	storageTrackers map[raftID]*storageTracker
}

// ApplyReplicaGroups applies the list of replica groups.
func (r *replicator) ApplyReplicaGroups(cluster *gitalypb.Cluster) (int, error) {
	var changes int

	authorityStorageInfo := cluster.Storages[r.authorityID.ToUint64()]
	if authorityStorageInfo == nil {
		return 0, fmt.Errorf("storage with ID %d does not exist", r.authorityID)
	}

	replicatingStorages := r.stringifyReplicatingStorages()
	storagesToReplicate := r.storagesToReplicate(cluster)

	// First, initialize the authority storage, which is the storage managed by this node.
	if r.authority == nil {
		authority, err := r.config.initAuthorityGroup(authorityStorageInfo)
		if err != nil {
			return changes, fmt.Errorf("creating authority Raft group: %w", err)
		}

		if err := authority.WaitReady(); err != nil {
			if closingErr := authority.Close(); closingErr != nil {
				return changes, errors.Join(err, closingErr)
			}
			return changes, fmt.Errorf("waiting authority Raft group: %w", err)
		}

		// Track this storage. As the group does not communicate with this replicator directly, we
		// need to poll from its statemachine.
		r.trackStorage(r.authorityID, authority)
		r.authority = authority
		changes++

		r.logger.WithFields(log.Fields{
			"raft.storage_id":   r.authorityID,
			"raft.storage_name": authorityStorageInfo.GetName(),
		}).Info("tracking authority storage")
	}

	// Register all replicas of this authority storage. The replication won't start until a
	// replication Raft groups on other nodes start. This node doesn't remove obsolete replicas on
	// other nodes although it could. It lets other nodes shutdown obsolete replicas themselves so
	// that they have a chance to clean up resources.
	for _, replicaID := range authorityStorageInfo.ReplicaGroups {
		if err := r.registerReplica(replicaID, cluster); err != nil {
			return changes, fmt.Errorf("registering replica: %w", err)
		}
	}

	// Untrack obsoleted replicas when the list of replica groups changes. At the moment, the
	// replicator doesn't clean up data. It simply stops listening to new changes. Data deletion
	// will be handled in https://gitlab.com/gitlab-org/gitaly/-/issues/6248.
	for storageID := range r.storageTrackers {
		if storageID == r.authorityID {
			continue
		}
		if _, exist := storagesToReplicate[storageID]; !exist {
			changes++
			if err := r.untrackStorage(storageID); err != nil {
				return 0, fmt.Errorf("untrack storage ID %d: %w", storageID, err)
			}
			r.logger.WithFields(log.Fields{"raft.storage_id": storageID}).Info("untracked storage")
		}
	}

	// Listen to existing/new partitions from tracked replica.
	for storageID, storageInfo := range storagesToReplicate {
		if _, exist := r.storageTrackers[storageID]; exist {
			continue
		}
		changes++
		replicated, err := r.config.initReplicaGroup(r.authorityID, storageInfo)
		if err != nil {
			return 0, fmt.Errorf("creating replication Raft group: %w", err)
		}
		r.trackStorage(storageID, replicated)
		r.logger.WithFields(log.Fields{
			"raft.storage_id":   storageID,
			"raft.storage_name": storageInfo.GetName(),
		}).Info("tracking storage")
	}

	if changes != 0 {
		r.logger.WithFields(log.Fields{
			"raft.replicating_storages":  replicatingStorages,
			"raft.storages_to_replicate": r.stringifyStorageMap(storagesToReplicate),
		}).Info(fmt.Sprintf("applied replica groups, %d changes", changes))
	}
	return changes, nil
}

// BroadcastNewPartition is triggered by a caller. It makes a call to the authority group to
// register the new partition. This partition is broadcasted to corresponding replica Raft groups.
func (r *replicator) BroadcastNewPartition(partitionID raftID, relativePath string) error {
	if _, err := r.authority.RegisterPartition(partitionID, relativePath); err != nil {
		return fmt.Errorf("registering partition: %w", err)
	}
	return nil
}

// Close stops all activities of the replicator. This function exits after background goroutines finish.
func (r *replicator) Close() error {
	var mergedErr error

	for storageID := range r.storageTrackers {
		if err := r.untrackStorage(storageID); err != nil {
			if mergedErr == nil {
				mergedErr = err
			} else {
				mergedErr = errors.Join(mergedErr, err)
			}
		}
	}
	return mergedErr
}

func (r *replicator) storagesToReplicate(cluster *gitalypb.Cluster) map[raftID]*gitalypb.Storage {
	replications := map[raftID]*gitalypb.Storage{}
	for storageID, storage := range cluster.Storages {
		if slices.Contains(storage.ReplicaGroups, r.authorityID.ToUint64()) {
			replications[raftID(storageID)] = storage
		}
	}
	return replications
}

func (r *replicator) stringifyStorageMap(m map[raftID]*gitalypb.Storage) map[raftID]string {
	result := map[raftID]string{}
	for k, v := range m {
		result[k] = v.GetName()
	}
	return result
}

func (r *replicator) stringifyReplicatingStorages() map[raftID]string {
	result := map[raftID]string{}
	for k, v := range r.storageTrackers {
		if k != r.authorityID {
			result[k] = v.poller.StorageName()
			continue
		}
	}
	return result
}

func (r *replicator) trackStorage(storageID raftID, poller replica) {
	ctx, cancel := context.WithCancel(r.ctx)
	r.storageTrackers[storageID] = &storageTracker{
		ctx:    ctx,
		cancel: cancel,
		poller: poller,
		done:   make(chan struct{}),
	}
	go r.pollPartitionsFromStorage(r.storageTrackers[storageID])
}

func (r *replicator) untrackStorage(storageID raftID) error {
	tracker := r.storageTrackers[storageID]
	tracker.cancel()
	defer func() {
		delete(r.storageTrackers, storageID)
	}()

	if err := tracker.poller.Close(); err != nil {
		return fmt.Errorf("closing poller: %w", err)
	}

	select {
	case <-time.After(defaultReplicatorTimeout):
		return fmt.Errorf("deadline exceeded while untracking storage")
	case <-tracker.done:
		return nil
	}
}

func (r *replicator) pollPartitionsFromStorage(tracker *storageTracker) {
	defer close(tracker.done)

	for {
		select {
		case <-tracker.ctx.Done():
			return
		default:
		}

		partitions, err := tracker.poller.PollPartitions()
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return
			}
			r.logger.WithFields(log.Fields{
				"raft.storage_id":   tracker.poller.StorageID(),
				"raft.storage_name": tracker.poller.StorageName(),
			}).WithError(err).Error("fail to poll partritions from storage")
			continue
		}
		if len(partitions) == 0 {
			continue
		}
		for _, operation := range partitions {
			partition := operation.partition
			if partition == nil {
				r.logger.Error("tracking unknown partition")
				continue
			}
			switch operation.op {
			case opTrackPartition:
				fields := log.Fields{
					"raft.authority_id":   partition.GetAuthorityId(),
					"raft.authority_name": partition.GetAuthorityName(),
					"raft.storage_id":     r.authority.StorageID(),
					"raft.storage_name":   r.authority.StorageName(),
					"raft.partition_id":   partition.GetPartitionId(),
					"raft.relative_path":  partition.GetRelativePath(),
				}
				if err := r.config.initPartitionGroup(
					raftID(partition.GetAuthorityId()),
					raftID(partition.GetPartitionId()),
					partition.GetRelativePath(),
				); err != nil {
					r.logger.WithFields(fields).WithError(err).Error("fail to initialize partition Raft group")
					continue
				}
				r.logger.WithFields(fields).Debug("tracking partition")
			default:
				r.logger.WithFields(log.Fields{
					"raft.partition_id":  partition.GetPartitionId(),
					"raft.relative_path": partition.GetRelativePath(),
				}).Error("unsupported op")
			}
		}
	}
}

func (r *replicator) registerReplica(replicaID uint64, cluster *gitalypb.Cluster) error {
	replicaInfo := cluster.Storages[replicaID]
	if replicaInfo == nil {
		return fmt.Errorf("replica ID %d of storage ID %d not found", replicaID, r.authorityID)
	}
	addr, err := r.config.getNodeAddr(raftID(replicaInfo.GetNodeId()))
	if err != nil {
		return fmt.Errorf("getting storage address: %w", err)
	}
	ctx, cancel := context.WithTimeout(r.ctx, defaultReplicatorTimeout)
	defer cancel()
	return r.authority.RegisterReplica(ctx, raftID(replicaInfo.GetNodeId()), addr)
}

func newReplicator(ctx context.Context, authorityID raftID, logger log.Logger, config replicatorConfig) *replicator {
	return &replicator{
		ctx:         ctx,
		authorityID: authorityID,
		logger: logger.WithFields(log.Fields{
			"raft.component":  "replicator",
			"raft.storage_id": authorityID,
		}),
		config:          config,
		storageTrackers: map[raftID]*storageTracker{},
	}
}
