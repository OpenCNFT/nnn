package raft

import (
	"context"
	"fmt"
	"path/filepath"
	"sync/atomic"

	"github.com/lni/dragonboat/v4"
	dragonboatConf "github.com/lni/dragonboat/v4/config"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/storagemgr"
	"gitlab.com/gitlab-org/gitaly/v16/internal/log"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
)

// Group is an abstract data structure that stores information of a Raft group.
type Group struct {
	ctx           context.Context
	groupID       raftID
	replicaID     raftID
	clusterConfig config.Raft
	groupConfig   dragonboatConf.Config
	logger        log.Logger
	nodeHost      *dragonboat.NodeHost
	statemachine  Statemachine
}

// ManagerConfig contains the configuration options for the Raft manager.
type ManagerConfig struct {
	// BootstrapCluster tells the manager to bootstrap the cluster when it starts.
	BootstrapCluster bool
	// expertConfig contains advanced configuration for dragonboat. Used for testing only.
	expertConfig dragonboatConf.ExpertConfig
	// testBeforeRegister triggers a callback before registering a storage. Used for testing only.
	testBeforeRegister func()
}

// Manager is responsible for managing the Raft cluster for all storages.
type Manager struct {
	ctx           context.Context
	clusterConfig config.Raft
	managerConfig ManagerConfig
	logger        log.Logger
	started       atomic.Bool
	closed        atomic.Bool

	storageManagers map[string]*storageManager
	firstStorage    *storageManager
	metadataGroup   *metadataRaftGroup
}

func walDir(storageConfig config.Storage) string {
	return filepath.Join(storageConfig.Path, config.GitalyDataPrefix, "raft", "wal")
}

func nodeHostDir(storageConfig config.Storage) string {
	return filepath.Join(storageConfig.Path, config.GitalyDataPrefix, "raft", "node")
}

// NewManager creates a new Raft manager that manages the Raft storage for all configured storages.
func NewManager(
	ctx context.Context,
	storages []config.Storage,
	clusterCfg config.Raft,
	managerCfg ManagerConfig,
	ptnMgr *storagemgr.PartitionManager,
	logger log.Logger,
) (*Manager, error) {
	SetLogger(logger, true)

	if len(storages) > 1 {
		return nil, fmt.Errorf("the support for multiple storages is temporarily disabled")
	}

	m := &Manager{
		ctx:           ctx,
		clusterConfig: clusterCfg,
		managerConfig: managerCfg,
		logger: logger.WithFields(log.Fields{
			"component":       "raft",
			"raft_component":  "manager",
			"raft_cluster_id": clusterCfg.ClusterID,
			"raft_node_id":    clusterCfg.NodeID,
		}),
		storageManagers: map[string]*storageManager{},
	}

	storage := storages[0]
	nodeHost, err := dragonboat.NewNodeHost(dragonboatConf.NodeHostConfig{
		WALDir:                     walDir(storage),
		NodeHostDir:                nodeHostDir(storage),
		RTTMillisecond:             m.clusterConfig.RTTMilliseconds,
		RaftAddress:                m.clusterConfig.RaftAddr,
		ListenAddress:              m.clusterConfig.RaftAddr,
		DefaultNodeRegistryEnabled: false,
		EnableMetrics:              true,
		RaftEventListener: &raftLogger{
			Logger: m.logger.WithField("raft_component", "system"),
		},
		Expert: managerCfg.expertConfig,
	})
	if err != nil {
		return nil, fmt.Errorf("creating dragonboat nodehost: %w", err)
	}

	m.storageManagers[storage.Name] = newStorageManager(storage.Name, ptnMgr, nodeHost)
	if m.firstStorage == nil {
		m.firstStorage = m.storageManagers[storage.Name]
	}

	return m, nil
}

// Start starts the Raft cluster by:
// - Initializing the node-level Raft object for each storage. It initializes underlying engines,
// networking Raft servers, log databases, etc.
// - Joining the metadata Raft group. This Raft group contains cluster-wide metadata, storage
// registry, replica groups, etc. In the first iteration, all initial members always participate
// in this group.
// - Bootstrapping the Raft cluster if configured to do so. Bootstrapping persists initial cluster
// information via metadata Raft group. These steps require a synchronization between initial nodes.
// The bootstrapping waits until the quorum reaches. Afterward, the cluster is ready; nodes
// (including initial members) are allowed to join. The bootstrapping step is skipped if the node
// detects an existing cluster.
// - Register the node's storage with the metadata Raft group. The metadata Raft group allocates a
// new storage ID for each of them. They persist in their IDs. This type of ID is used for future
// interaction with the cluster.
func (m *Manager) Start() (returnedErr error) {
	if m.started.Load() {
		return fmt.Errorf("raft manager already started")
	}
	defer func() {
		m.started.Store(true)
		if returnedErr != nil {
			m.Close()
		}
	}()

	m.logger.WithFields(log.Fields{
		"raft_config":       m.clusterConfig,
		"raft_manager_conf": m.managerConfig,
	}).Info("Raft cluster is starting")

	// A Gitaly node contains multiple independent storages, and each storage maps to a dragonboat
	// NodeHost instance. A Gitaly node must only host a single metadata group, so by default we use
	// the first storage of the node.
	// We also currently don't support new Gitaly nodes joining the cluster (see
	// https://gitlab.com/groups/gitlab-org/-/epics/13562 for more information), so the initial
	// members of the cluster are also the authority of the metadata group.
	if err := m.initMetadataGroup(m.firstStorage); err != nil {
		return fmt.Errorf("initializing Raft metadata group: %w", err)
	}

	if m.managerConfig.BootstrapCluster {
		cluster, err := m.metadataGroup.BootstrapIfNeeded()
		if err != nil {
			return fmt.Errorf("bootstrapping Raft cluster: %w", err)
		}
		m.logger.WithField("cluster", cluster).Info("Raft cluster bootstrapped")
	}

	// Temporarily, we fetch the cluster info from the metadata Raft group directly. In the future,
	// this node needs to contact a metadata authority.
	// For more information: https://gitlab.com/groups/gitlab-org/-/epics/10864
	cluster, err := m.metadataGroup.ClusterInfo()
	if err != nil {
		return fmt.Errorf("getting cluster info: %w", err)
	}
	if cluster.ClusterId != m.clusterConfig.ClusterID {
		return fmt.Errorf("joining the wrong cluster, expected to join %q but joined %q", m.clusterConfig.ClusterID, cluster.ClusterId)
	}

	if err := m.registerStorages(); err != nil {
		return err
	}

	m.logger.Info("Raft cluster has started")
	return nil
}

func (m *Manager) initMetadataGroup(storageMgr *storageManager) error {
	metadataGroup, err := newMetadataRaftGroup(
		m.ctx,
		storageMgr.nodeHost,
		storageMgr.dbForMetadataGroup(),
		m.clusterConfig,
		m.logger,
	)
	if err != nil {
		return err
	}
	m.metadataGroup = metadataGroup

	return m.metadataGroup.WaitReady()
}

func (m *Manager) registerStorages() error {
	if m.managerConfig.testBeforeRegister != nil {
		m.managerConfig.testBeforeRegister()
	}

	// Register storage ID if not exist. Similarly, this operation is handled by the metadata group.
	// It will be handled by the metadata authority in the future.
	for storageName, storageMgr := range m.storageManagers {
		if err := storageMgr.loadStorageInfo(m.ctx); err != nil {
			return fmt.Errorf("loading persisted storage info: %w", err)
		}
		if storageMgr.persistedInfo == nil || storageMgr.persistedInfo.GetStorageId() == 0 {
			storageInfo, err := m.metadataGroup.RegisterStorage(storageName)
			if err != nil {
				return fmt.Errorf("registering storage info: %w", err)
			}
			if err := storageMgr.saveStorageInfo(m.ctx, storageInfo); err != nil {
				return fmt.Errorf("saving storage info: %w", err)
			}
		} else if storageMgr.persistedInfo.NodeId != m.clusterConfig.NodeID || storageMgr.persistedInfo.ReplicationFactor != m.clusterConfig.ReplicationFactor {
			// Changes that gonna affect replication. Gitaly needs to sync up those changes to metadata
			// Raft group to shuffle the replication groups. We don't persit new info intentionally. The
			// replication of this node will be applied by the replicators later.
			if _, err := m.metadataGroup.UpdateStorage(
				raftID(storageMgr.persistedInfo.StorageId),
				raftID(m.clusterConfig.NodeID),
				m.clusterConfig.ReplicationFactor,
			); err != nil {
				return fmt.Errorf("updating storage info: %w", err)
			}
		}
		m.logger.WithFields(log.Fields{
			"storage_name":       storageName,
			"storage_id":         storageMgr.persistedInfo.GetStorageId(),
			"replication_factor": storageMgr.persistedInfo.GetReplicationFactor(),
		}).Info("storage joined the cluster")
	}

	return nil
}

// Ready returns if the Raft manager is ready.
func (m *Manager) Ready() bool {
	return m.started.Load()
}

// Close closes the Raft cluster by closing all Raft objects under management.
func (m *Manager) Close() {
	if m.closed.Load() {
		return
	}
	defer m.closed.Store(true)

	for _, storageMgr := range m.storageManagers {
		storageMgr.Close()
	}
	m.logger.Info("Raft cluster has stopped")
}

// ClusterInfo returns the cluster information.
func (m *Manager) ClusterInfo() (*gitalypb.Cluster, error) {
	if !m.started.Load() {
		return nil, fmt.Errorf("raft manager has not started")
	}
	if m.closed.Load() {
		return nil, fmt.Errorf("raft manager already closed")
	}
	return m.metadataGroup.ClusterInfo()
}
