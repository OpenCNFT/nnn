package raft

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/lni/dragonboat/v4"
	dragonboatConf "github.com/lni/dragonboat/v4/config"
	"github.com/lni/dragonboat/v4/statemachine"
	"gitlab.com/gitlab-org/gitaly/v16/internal/backoff"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v16/internal/log"
	"gitlab.com/gitlab-org/gitaly/v16/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
)

// replicationRaftGroup is a Raft group that either manages the partitions created by the input
// storage or manages the partitions replicated from another storage.
// This type of Raft group is supposted to be establish one-way connection between an authority to
// its replicas. Each related node starts a replication Raft group as follows:
// - The group member on authority storage always holds the "leader" role.
// - The group members on replicas always hold the "non-voting" role. They can sync the list of
// partitions created by the authority but don't involve in the election.
//
// All participants join the same group ID, generated from the authority's ID. This approach
// allows the authority to communicate partition's creation and deletion events to its replicas
// reliably. The list of partitions are persisted in the state machines of each replication Raft
// group member. If the authority if offline, no further partitions are created in that authority
// storage; the clients must find another authority.
//
// This group provides some functions that allow the outsider to access the list of partitions at
// startup as well as any partition updates along the way.
//
// The startup order of members of this group matters. The group leader, aka authority, must start
// the group first. It thens registers the list of replicas to the group. Finally, replica group
// members join the group.
type replicationRaftGroup struct {
	Group
	sync.Mutex
	cancel         context.CancelFunc
	backoffProfile *backoff.Exponential

	// authority tells if this Raft group is for an authority or not.
	authority   bool
	storageID   raftID
	storageName string

	initialized bool
	// pendingOperations contains the list of partitions waiting for processed.
	pendingOperations []*partitionOperation
	// newOperation is a channel telling if there is a new operation.
	newOperation chan struct{}
}

// newReplicationGroup starts an instance of the replication Raft group.
func newReplicationGroup(
	ctx context.Context,
	authority bool,
	storage *gitalypb.Storage,
	nodeHost *dragonboat.NodeHost,
	db dbAccessor,
	clusterCfg config.Raft,
	logger log.Logger,
) (*replicationRaftGroup, error) {
	ctx, cancel := context.WithCancel(ctx)

	// Both authority and replicas share the same group ID, which is generated from the authority ID.
	groupID := AuthorityGroupID(raftID(storage.GetStorageId()))
	groupCfg := dragonboatConf.Config{
		ReplicaID:    clusterCfg.NodeID,
		ShardID:      groupID.ToUint64(),
		ElectionRTT:  clusterCfg.ElectionTicks,
		HeartbeatRTT: clusterCfg.HeartbeatTicks,
		// Replicas must have non-voting role.
		IsNonVoting: !authority,
		WaitReady:   true,
	}

	backoffProfile := backoff.NewDefaultExponential(rand.New(rand.NewSource(time.Now().UnixNano())))
	backoffProfile.BaseDelay = time.Duration(clusterCfg.ElectionTicks) * time.Duration(clusterCfg.RTTMilliseconds) * time.Microsecond

	groupLogger := logger.WithFields(log.Fields{
		"raft.group":        "replication",
		"raft.group_id":     groupID,
		"raft.replica_id":   clusterCfg.NodeID,
		"raft.authority":    authority,
		"raft.storage_id":   storage.GetStorageId(),
		"raft.storage_name": storage.GetName(),
	})

	group := &replicationRaftGroup{
		Group: Group{
			ctx:           ctx,
			groupID:       groupID,
			replicaID:     raftID(clusterCfg.NodeID),
			clusterConfig: clusterCfg,
			groupConfig:   groupCfg,
			logger:        groupLogger,
			nodeHost:      nodeHost,
		},
		cancel:         cancel,
		storageID:      raftID(storage.GetStorageId()),
		storageName:    storage.GetName(),
		authority:      authority,
		backoffProfile: backoffProfile,
		newOperation:   make(chan struct{}, 1),
	}

	initialMembers := map[uint64]string{}
	// The authority group is the only initial member of this group. Replicated groups don't need the list of
	// initial members. They join the group as non-voting members.
	if authority {
		initialMembers[clusterCfg.NodeID] = clusterCfg.RaftAddr
	}

	if err := nodeHost.StartOnDiskReplica(initialMembers, !authority, func(_groupID, _replicaID uint64) statemachine.IOnDiskStateMachine {
		return newReplicationStateMachine(ctx, db, authority, raftID(storage.GetStorageId()), group.appendPartitionOps)
	}, groupCfg); err != nil {
		return nil, fmt.Errorf("joining replication group: %w", err)
	}

	return group, nil
}

// Stop stops the group by cancelling the context and closing connecting channels.
func (g *replicationRaftGroup) Close() (returnedErr error) {
	g.cancel()
	if err := g.nodeHost.StopReplica(g.groupConfig.ShardID, g.groupConfig.ReplicaID); err != nil {
		returnedErr = err
	}
	close(g.newOperation)
	return
}

// PollPartitions returns the list of pending operations. If there is any partition, the list
// returns immediately. Otherwise, the caller is blocked until new operation is added or the context
// is cancelled.
// If this function is called for the first time after the group has just started, it returns all
// partitions in the statemachine. Subsequent calls return new updates from the authority member.
func (g *replicationRaftGroup) PollPartitions() ([]*partitionOperation, error) {
	if err := g.initialize(); err != nil {
		return nil, fmt.Errorf("initializing replication Raft group")
	}

	for {
		g.Lock()
		// If there is any pending operation, returns immediately.
		if len(g.pendingOperations) > 0 {
			partitions := g.pendingOperations
			g.pendingOperations = []*partitionOperation{}
			g.Unlock()
			return partitions, nil
		}
		g.Unlock()

		// Other wise, block until then.
		select {
		case <-g.ctx.Done():
			return nil, nil
		case <-g.newOperation:
		}
	}
}

// WaitReady waits until the replication group is ready or its context is cancelled.
func (g *replicationRaftGroup) WaitReady() error {
	return WaitGroupReady(g.ctx, g.nodeHost, g.groupID)
}

// RegisterReplica adds another node as the replica of this authority storage. Without this call,
// the replication Raft group members on other nodes are not able to join the group. This function
// doesn't kick off replication. The replication starts when other nodes kick off their counterpart
// replication Raft groups.
func (g *replicationRaftGroup) RegisterReplica(ctx context.Context, nodeID raftID, replicaAddr string) error {
	if !g.authority {
		return fmt.Errorf("only authority storage could register its replica")
	}
	membership, err := g.nodeHost.SyncGetShardMembership(ctx, g.groupID.ToUint64())
	if err != nil {
		return fmt.Errorf("fetching shard membership: %w", err)
	}
	if _, exist := membership.NonVotings[nodeID.ToUint64()]; exist {
		return nil
	}

	if err := g.nodeHost.SyncRequestAddNonVoting(ctx, g.groupID.ToUint64(), nodeID.ToUint64(), replicaAddr, membership.ConfigChangeID); err != nil {
		return fmt.Errorf("requesting to add node %d (%s) as a replica of group %d (storage ID %d): %w", nodeID, replicaAddr, g.groupID, g.storageID, err)
	}
	return nil
}

// StorageName is a getter that returns the group's target storage name.
func (g *replicationRaftGroup) StorageName() string {
	return g.storageName
}

// StorageID is a getter that returns the group's target storage ID.
func (g *replicationRaftGroup) StorageID() raftID {
	return g.storageID
}

// initialize pushes all partitions in the state machine to the list of pending operations. It runs
// once when a caller gets the list of partitions after group has just started.
func (g *replicationRaftGroup) initialize() error {
	g.Lock()
	defer g.Unlock()

	if g.initialized {
		return nil
	}

	if err := g.WaitReady(); err != nil {
		return err
	}

	// Push all registered partitions to the pending list.
	partitions, err := g.GetRegisteredPartitions()
	if err != nil {
		return err
	}
	if len(partitions) > 0 {
		for _, partition := range partitions {
			g.pendingOperations = append(g.pendingOperations, &partitionOperation{
				op:        opTrackPartition,
				partition: partition,
			})
		}
		g.signalNewPartition()
	}
	g.initialized = true
	return nil
}

func (g *replicationRaftGroup) signalNewPartition() {
	select {
	case g.newOperation <- struct{}{}:
	default:
		// If the signal channel is full, no need to wait.
	}
}

func (g *replicationRaftGroup) appendPartitionOps(ops []*partitionOperation) {
	g.Lock()
	defer g.Unlock()

	g.pendingOperations = append(g.pendingOperations, ops...)
	g.signalNewPartition()
}

// RegisterPartition adds a partition to the tracking list of the replication Raft group.
func (g *replicationRaftGroup) RegisterPartition(partitionID raftID, relativePath string) (*gitalypb.Partition, error) {
	result, response, err := g.requestRegisterPartition(partitionID, relativePath)
	if err != nil {
		return nil, fmt.Errorf("sending RegisterPartitionRequest: %w", err)
	}

	switch result {
	case resultRegisterPartitionSuccessfully:
		return response.GetPartition(), nil
	case resultRegisterPartitionExisted:
		return nil, structerr.New("partition already existed").WithMetadataItems(
			structerr.MetadataItem{Key: "partition_id", Value: partitionID},
			structerr.MetadataItem{Key: "relative_path", Value: relativePath},
		)
	default:
		return nil, fmt.Errorf("unknown result code %d", result)
	}
}

// GetRegisteredPartitions returns the list of all registered partitions. Warning. This function is
// not suitable to be called too frequently.
func (g *replicationRaftGroup) GetRegisteredPartitions() ([]*gitalypb.Partition, error) {
	response, err := g.requestGetPartitions()
	if err != nil {
		return nil, fmt.Errorf("sending GetRegisteredPartitionsRequest: %w", err)
	}
	return response.GetPartitions(), nil
}

func (g *replicationRaftGroup) requestRegisterPartition(partitionID raftID, relativePath string) (updateResult, *gitalypb.RegisterPartitionResponse, error) {
	requester := NewRequester[*gitalypb.RegisterPartitionRequest, *gitalypb.RegisterPartitionResponse](
		g.nodeHost, g.groupID, g.logger, requestOption{
			retry:       defaultRetry,
			timeout:     g.maxNextElectionWait(),
			exponential: g.backoffProfile,
		},
	)
	return requester.SyncWrite(g.ctx, &gitalypb.RegisterPartitionRequest{
		Partition: &gitalypb.Partition{
			AuthorityName: g.storageName,
			AuthorityId:   g.storageID.ToUint64(),
			PartitionId:   partitionID.ToUint64(),
			RelativePath:  relativePath,
		},
	})
}

func (g *replicationRaftGroup) requestGetPartitions() (*gitalypb.GetRegisteredPartitionsResponse, error) {
	requester := NewRequester[*gitalypb.GetRegisteredPartitionsRequest, *gitalypb.GetRegisteredPartitionsResponse](
		g.nodeHost, g.groupID, g.logger, requestOption{
			retry:       defaultRetry,
			timeout:     g.maxNextElectionWait(),
			exponential: g.backoffProfile,
		},
	)
	return requester.SyncRead(g.ctx, &gitalypb.GetRegisteredPartitionsRequest{})
}

func (g *replicationRaftGroup) maxNextElectionWait() time.Duration {
	return time.Millisecond * time.Duration(g.groupConfig.ElectionRTT*g.nodeHost.NodeHostConfig().RTTMillisecond)
}
