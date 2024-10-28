package raftmgr

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/keyvalue"
	"gitlab.com/gitlab-org/gitaly/v16/internal/log"
	"gitlab.com/gitlab-org/gitaly/v16/internal/safe"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

const (
	defaultMaxSizePerMsg   = 10 * 1024 * 1024
	defaultMaxInflightMsgs = 256
)

// ready manages the readiness signaling. The caller polls from the channel.
type ready struct {
	c    chan struct{} // Channel to signal readiness
	once sync.Once     // Ensures readiness is signaled only once
}

// set signals readiness by closing the channel, ensuring it
// happens only once.
func (r *ready) set() {
	r.once.Do(func() { close(r.c) })
}

// ManagerOptions encapsulates optional parameters for configuring
// the Manager.
type ManagerOptions struct {
	// readyTimeout sets an timeout when waiting for Raft to be ready.
	readyTimeout time.Duration
	// opTimeout sets a timeout for propose, append, and commit operations. It's meaningful in tests so that we
	// could detect deadlocks and intolerant performance degrades.
	opTimeout time.Duration
	// entryRecorder indicates whether to store all log entries processed by Raft. It is primarily
	// used for testing purposes.
	entryRecorder *EntryRecorder
	// recordTransport indicates whether to store all messages supposed to sent over the network. It is
	// primarily used for testing purposes.
	recordTransport bool
}

// OptionFunc defines a function type for applying optional
// configuration to ManagerOptions.
type OptionFunc func(opt ManagerOptions) ManagerOptions

// WithReadyTimeout sets a timeout when waiting for Raft service to be ready.
// The default timeout is 5 * election timeout.
func WithReadyTimeout(t time.Duration) OptionFunc {
	return func(opt ManagerOptions) ManagerOptions {
		opt.readyTimeout = t
		return opt
	}
}

// WithOpTimeout sets a timeout for individual operation. It's supposed to be used in testing environment only.
func WithOpTimeout(t time.Duration) OptionFunc {
	return func(opt ManagerOptions) ManagerOptions {
		opt.opTimeout = t
		return opt
	}
}

// WithEntryRecorder configures ManagerOptions to store Raft log entries, useful for testing scenarios. It should
// not be enabled outside of testing environments.
func WithEntryRecorder(recorder *EntryRecorder) OptionFunc {
	return func(opt ManagerOptions) ManagerOptions {
		opt.entryRecorder = recorder
		return opt
	}
}

// WithRecordTransport configures ManagerOptions to store Raft transport messages, useful for testing scenarios. It
// should not be enabled outside of testing environments.
func WithRecordTransport() OptionFunc {
	return func(opt ManagerOptions) ManagerOptions {
		opt.recordTransport = true
		return opt
	}
}

// Manager orchestrates the Raft consensus protocol, managing configuration,
// state synchronization, and communication between distributed nodes.
type Manager struct {
	sync.Mutex
	ctx    context.Context
	cancel context.CancelCauseFunc

	// Configurations.
	ptnID      storage.PartitionID    // Unique identifier for the managed partition
	storage    string                 // Name of the storage this partition belong to.
	node       raft.Node              // etcd/raft node representation.
	raftCfg    config.Raft            // etcd/raft configurations.
	options    ManagerOptions         // Additional manager options
	logger     log.Logger             // Logger for internal activities.
	db         keyvalue.Transactioner // Key-value DB to resume Raft activities after restart.
	wal        storage.WriteAheadLog  // Write-ahead logging manager.
	state      *persistentState       // Persistent storage for Raft log entries and states.
	registry   *Registry              // Event registry, used to track events emitted by the manager.
	transport  Transport              // Manage data transportation between nodes.
	leadership *Leadership            // Store up-to-date leadership information.
	syncer     safe.Syncer
	wg         sync.WaitGroup
	ready      *ready
	started    bool

	// EntryRecorder stores the list of internal Raft log entries such as config changes, empty verification
	// entrires, etc. They are used in testing environments.
	EntryRecorder *EntryRecorder
}

// applyOptions creates a set of default manager options and then apply option setting functions. It also validates the
// resulting options.
func applyOptions(raftCfg config.Raft, opts []OptionFunc) (ManagerOptions, error) {
	baseRTT := time.Duration(raftCfg.RTTMilliseconds) * time.Millisecond
	options := ManagerOptions{
		// readyTimeout is set to 5 election timeout by default. The Raft manager waits for some couple of
		// initial self elections, just in case.
		readyTimeout: 5 * time.Duration(raftCfg.ElectionTicks) * baseRTT,
	}
	for _, opt := range opts {
		options = opt(options)
	}
	if options.readyTimeout == 0 {
		return options, fmt.Errorf("readyTimeout must not be zero")
	} else if options.readyTimeout < time.Duration(raftCfg.ElectionTicks)*baseRTT {
		return options, fmt.Errorf("readyTimeout must not be less than election timeout")
	}
	return options, nil
}

// NewManager creates an instance of Manager.
func NewManager(
	partitionID storage.PartitionID,
	storageName string,
	raftCfg config.Raft,
	db keyvalue.Transactioner,
	logger log.Logger,
	opts ...OptionFunc,
) (*Manager, error) {
	options, err := applyOptions(raftCfg, opts)
	if err != nil {
		return nil, fmt.Errorf("invalid raft manager option: %w", err)
	}

	logger = logger.WithFields(log.Fields{
		"component":      "raft",
		"raft.partition": partitionID,
		"raft.authority": storageName,
	})

	// We haven't taken care of network transportation at this point. Let's use a no-op transportation to swallow
	// external messages.
	transport := NewNoopTransport(logger.WithField("raft.component", "transportation"), options.recordTransport)
	return &Manager{
		storage:       storageName,
		ptnID:         partitionID,
		raftCfg:       raftCfg,
		options:       options,
		db:            db,
		logger:        logger,
		registry:      NewRegistry(),
		transport:     transport,
		syncer:        safe.NewSyncer(),
		leadership:    NewLeadership(),
		ready:         &ready{c: make(chan struct{})},
		EntryRecorder: options.entryRecorder,
	}, nil
}

// Run starts the Raft manager, including etcd/raft Node instance and internal processing goroutine.
func (mgr *Manager) Run(ctx context.Context, wal storage.WriteAheadLog) error {
	if !mgr.raftCfg.Enabled {
		return fmt.Errorf("raft is not enabled")
	}

	mgr.Lock()
	defer mgr.Unlock()

	if mgr.started {
		return fmt.Errorf("raft manager for partition %q is re-used", mgr.ptnID)
	}
	mgr.started = true

	mgr.ctx, mgr.cancel = context.WithCancelCause(ctx)
	mgr.wal = wal
	mgr.state = newPersistentState(mgr.db, wal)

	bootstrapped, err := mgr.state.Bootstrapped()
	if err != nil {
		return fmt.Errorf("failed to load raft initial state: %w", err)
	}

	// A partition is identified globally by (authorityName, partitionID) tuple. A node (including primary) is
	// identified by (authorityName, partitionID, nodeID). Instead of setting the node ID statically in the Raft
	// config, Raft manager uses the LSN of config change log entry that contains node joining event. This approach
	// yields some benefits:
	// - No need for setting node ID statically, thus avoiding dual storage name/node ID identity system.
	// - No need for a global node registration system.
	// - Work better in the scenario where a node leaves and then re-join the cluster. Each joining event leads to a
	//   new unique node ID.
	// At this stage, Gitaly supports single-node clusters only. Thus, the node ID is always equal to 1. When
	// networking replication is implemented, newly joint replicas are aware of their own node ID by examining
	// messages at the transportation layer. When so, a node could self-bootstrap replicas.
	// Issue: https://gitlab.com/gitlab-org/gitaly/-/issues/6304
	var nodeID uint64 = 1

	config := &raft.Config{
		ID:              nodeID,
		ElectionTick:    int(mgr.raftCfg.ElectionTicks),
		HeartbeatTick:   int(mgr.raftCfg.HeartbeatTicks),
		Storage:         mgr.state,
		MaxSizePerMsg:   defaultMaxSizePerMsg,
		MaxInflightMsgs: defaultMaxInflightMsgs,
		Logger:          &raftLogger{logger: mgr.logger.WithFields(log.Fields{"raft.component": "manager"})},
		// etcd/raft supports Proposal Forwarding. When a node receives a proposal, and it does not hold the
		// primary role, it forwards the entry to the current leader. Unfortunately, it doesn't work for Gitaly.
		//
		// In Gitaly WAL, a transaction is verified, committed, and then applied in order. Transactions are serialized.
		// The next transaction is verified based on the latest state after applying the previous one. Raft is
		// involved in the process at the committing phase. The log entry is broadcasted after an entry is
		// verified and admitted. In addition, a transaction depends on a snapshot. This snapshot is essentially
		// the point-of-time state of a partition that a transaction reads the data from.
		//
		// If Proposal Forwarding is used, two nodes are allowed to accept mutator requests simultaneously and
		// then commit transactions independently. Although the resulting log entries are handled by the
		// primary, the latter entry is not verified against the prior one.
		//
		// Replica: A -> B -> C -> Start D -> Forward to Primary
		// Primary: A -> B -> C -> Start E -> Commit E -> Receive D -> Commit D
		// => D is not verified against E even though E commits before D.
		//
		// As a result, we disable proposal forwarding in favor of request forwarding/proxying instead.
		// For more information: https://gitlab.com/gitlab-org/gitaly/-/issues/6465
		DisableProposalForwarding: true,
	}

	if !bootstrapped {
		// When the raft group for this partition bootstraps for the first time, it is typically the next step
		// after that partition is created by the storage manager. At this stage, the only peer is itself.
		peers := []raft.Peer{{ID: nodeID}}
		mgr.node = raft.StartNode(config, peers)
	} else {
		// Applied index is set to the latest committed log entry index processed by the application. WAL
		// doesn't wait until the application phase. As soon as a log entry is committed, it unlocks clients
		// return results. Thus, the Applied index (from etcd/raft point of view) is essentially WAL's
		// committedLSN.
		config.Applied = uint64(mgr.wal.LastLSN())
		mgr.node = raft.RestartNode(config)
	}

	go mgr.run(bootstrapped)
	return nil
}

func (mgr *Manager) run(bootstrapped bool) {
	defer func() {
		if p := recover(); p != nil {
			err, ok := p.(error)
			if !ok {
				err = fmt.Errorf("raft loop panic: %v", p)
			}
			mgr.logger.WithError(err).Error("raft event loop panic")
			mgr.cancel(err)

		}
	}()

	mgr.wg.Add(1)
	defer mgr.wg.Done()

	ticker := time.NewTicker(time.Duration(mgr.raftCfg.RTTMilliseconds) * time.Millisecond)
	defer ticker.Stop()

	// If the Raft group for this partition has not been bootstrapped, Raft needs to wait until complete the first
	// conf change event before able to handle the first log entry. After each restart, the current Raft state is
	// resumed from the persistent storage. A single cluster doesn't need to wait until the next election. Thus, it
	// could be marked as ready instantly.
	if bootstrapped {
		mgr.ready.set()
	}

	// Main event processing loop.
	for {
		select {
		case <-ticker.C:
			// etcd/raft library implements an internal tick clock. The application must increase the tick
			// manually. Election and timeout depends on the number of ticks.
			mgr.node.Tick()
		case rd := <-mgr.node.Ready():
			if err := mgr.handleReady(&rd); err != nil {
				mgr.ready.set()
				if errors.Is(err, context.Canceled) {
					mgr.logger.WithError(err).Info("raft event loop stopped")
					mgr.cancel(nil)
				} else {
					mgr.logger.WithError(err).Error("raft event loop failed")
					mgr.cancel(err)
				}
				return
			}
			mgr.node.Advance()
		case <-mgr.ctx.Done():
			return
		}
	}
}

// WaitReady waits until the Raft manager is ready to service or exceeds readyTimeout.
func (mgr *Manager) WaitReady() error {
	tick := time.NewTicker(mgr.options.readyTimeout)
	defer tick.Stop()

	select {
	case <-tick.C:
		return fmt.Errorf("ready timeout exceeded")
	case <-mgr.ready.c:
		return nil
	}
}

// Done returns a channel for polling running status of the Raft manager. The caller is expected to call Err() to fetch
// underlying error (if any).
func (mgr *Manager) Done() <-chan struct{} {
	return mgr.ctx.Done()
}

// Err returns the running error of the Raft manager. If the returned value is nil, it means either Raft is still
// running or it exits susccessully. Hence, it should only be called after polling from Done().
func (mgr *Manager) Err() error {
	err := mgr.ctx.Err()
	if err != nil && context.Cause(mgr.ctx) != nil {
		err = context.Cause(mgr.ctx)
	}
	return err
}

// Stop terminates all activities of Raft.
func (mgr *Manager) Stop() {
	// The node has not started yet, properly it fails during initialization.
	if mgr.node != nil {
		mgr.node.Stop()
	}
	// If the context has been cancelled beforehand, the event processing loop failed with an error. Skip
	// cancellation in that case.
	if mgr.ctx != nil && mgr.ctx.Err() == nil {
		mgr.cancel(nil)
	}
}

// Propose proposes a log entry change to the cluster. The caller is blocked until either the log entry is committed,
// timeout exceeded, or the request is rejected by the cluster.
func (mgr *Manager) Propose(ctx context.Context, logEntry *gitalypb.LogEntry, logEntryPath string) error {
	mgr.wg.Add(1)
	defer mgr.wg.Done()

	w := mgr.registry.Register()
	defer mgr.registry.Untrack(w.ID)

	message := &gitalypb.RaftMessageV1{
		Id:            uint64(w.ID),
		ClusterId:     mgr.raftCfg.ClusterID,
		AuthorityName: mgr.storage,
		PartitionId:   uint64(mgr.ptnID),
		LogEntry:      logEntry,
		LogData: &gitalypb.RaftMessageV1_Referenced{
			Referenced: &gitalypb.RaftMessageV1_ReferencedLogData{
				Path: []byte(logEntryPath),
			},
		},
	}
	data, err := proto.Marshal(message)
	if err != nil {
		return fmt.Errorf("marshaling Raft message: %w", err)
	}

	// Set an optional timeout to prevent proposal processing takes forever. This option is
	// more useful in testing environments.
	if mgr.options.opTimeout != 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, mgr.options.opTimeout)
		defer cancel()
	}

	if err := mgr.node.Propose(ctx, data); err != nil {
		return fmt.Errorf("proposing Raft message: %w", err)
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-mgr.ctx.Done():
		return fmt.Errorf("raft manager is closed: %w", mgr.ctx.Err())
	case <-w.C:
		return w.Err
	}
}

// handleReady processes the next state signaled by etcd/raft. It includes 3 main stepsS:
// - Persist states, including SoftState, HardState, and Entries (uncommitted).
// - Send messages to other nodes via Transport.
// - Publish commit entries. Committed entries are entries acknowledged by the majority of group members.
// Those steps are noted in https://pkg.go.dev/go.etcd.io/etcd/raft/v3#section-readme.
func (mgr *Manager) handleReady(rd *raft.Ready) error {
	// Save current volatile state of a Node. It is used for observability and quick determining the current
	// leadership of the group.
	if err := mgr.handleSoftState(rd); err != nil {
		return fmt.Errorf("handling soft state: %w", err)
	}

	// Save essential state to resume its activities after a restart.
	if err := mgr.handleHardState(rd); err != nil {
		return fmt.Errorf("handling hard state: %w", err)
	}

	// Save entries log entries to disk. Those entries haven't been committed yet. They could be overridden by
	// entries with the same LSN but with higher term. WAL must cleans up subsequent appended entries if the
	// inserting position overlaps.
	if err := mgr.saveEntries(rd); err != nil {
		return fmt.Errorf("saving entries: %w", err)
	}

	// Send outbound messages. From the thesis (10.2) and etcd/raft doc, this process could be pipelined so that
	// entries could be sent in parallel as soon as they are persisted to the disk first. However, as WAL serializes
	// transactions, only one log entry is processed at a time. Thus, that optimization is not needed. This
	// assumption is subject to change in the future if the WAL can process multiple transactions simultaneously.
	// Source: https://github.com/ongardie/dissertation/blob/master/stanford.pdf
	//
	// At this stage, auto-compaction feature is completely ignored. In theory, the application creates "snapshots"
	// occasionally. A snapshot is a point-of-time persistent state of the state machine. After a snapshot is
	// created, the application can remove all log entries til the point where the snapshot is created. A new
	// joining node loads the latest snapshot and replays WAL entries from there. We skipped snapshotting in the
	// first iteration because the current node acts as the only primary for a single-node Gitaly cluster. It hasn't
	// replicated log entries anywhere else
	//
	// For more information: https://gitlab.com/gitlab-org/gitaly/-/issues/6463
	if err := mgr.sendMessages(rd); err != nil {
		return fmt.Errorf("sending messages: %w", err)
	}

	// Mark committed entries in WAL. If the Raft group has only one member, there's a good chance the rd.Entries
	// and rd.CommittedEntries overlap (partially or completely). It means those entries are sent to the committed
	// state without going the need for sending external messages.
	if err := mgr.commitEntries(rd); err != nil {
		return fmt.Errorf("processing committed entries: %w", err)
	}
	return nil
}

func (mgr *Manager) saveEntries(rd *raft.Ready) error {
	if len(rd.Entries) == 0 {
		return nil
	}

	// Discard all in-flight events having duplicated LSN but with lower term. The corresponding on-disk log entries
	// will be wiped by WAL. Events without associated LSN haven't advanced to this stage yet, hence stay instact.
	firstLSN := storage.LSN(rd.Entries[0].Index)
	mgr.registry.UntrackSince(firstLSN)

	for i := range rd.Entries {
		switch rd.Entries[i].Type {
		case raftpb.EntryNormal:
			lsn := storage.LSN(rd.Entries[i].Index)

			if len(rd.Entries[i].Data) == 0 {
				if err := mgr.insertEmptyLogEntry(lsn); err != nil {
					return fmt.Errorf("inserting empty log entry: %w", err)
				}
			} else {
				var message gitalypb.RaftMessageV1
				if err := proto.Unmarshal(rd.Entries[i].Data, &message); err != nil {
					return fmt.Errorf("unmarshalling entry type: %w", err)
				}

				var logEntryPath string
				switch m := message.GetLogData().(type) {
				case *gitalypb.RaftMessageV1_Packed:
					// Log entry data is packed and unpacked in the Transport layer. Before sending
					// messages, it packages and compresses log entry directory. Likely, after
					// receiving, the data is converted to identical on-disk files. The whole
					// process is transparent to Raft manager.
					return fmt.Errorf("packed log entry data must be unpacked in Transport layer")
				case *gitalypb.RaftMessageV1_Referenced:
					logEntryPath = string(m.Referenced.GetPath())
				}

				logEntry := message.GetLogEntry()
				// The Raft term is carved into the log entry. LSN and term are only available at this
				// stage. Thus, we could not set this value beforehand.
				logEntry.Metadata = &gitalypb.LogEntry_Metadata{
					RaftTerm: rd.Entries[i].Index,
					RaftType: uint64(gitalypb.RaftMessageType_NORMAL),
				}
				if err := mgr.wal.AppendLogEntry(mgr.ctx, lsn, logEntry, logEntryPath); err != nil {
					return fmt.Errorf("appending log entry: %w", err)
				}
				mgr.recordEntryIfNeeded(false, lsn, logEntry)

				// Associate the LSN of the log entry to the event.
				mgr.registry.AssignLSN(EventID(message.GetId()), lsn)
			}
		case raftpb.EntryConfChange, raftpb.EntryConfChangeV2:
			if err := mgr.saveConfigChange(rd.Entries[i]); err != nil {
				return fmt.Errorf("saving config change: %w", err)
			}
		default:
			return fmt.Errorf("raft entry type not supported: %s", rd.Entries[i].Type)
		}
	}
	return nil
}

func (mgr *Manager) saveConfigChange(entry raftpb.Entry) error {
	if entry.Type == raftpb.EntryConfChange {
		var cc raftpb.ConfChange
		if err := cc.Unmarshal(entry.Data); err != nil {
			return fmt.Errorf("unmarshalling EntryConfChange: %w", err)
		}
	} else {
		var cc raftpb.ConfChangeV2
		if err := cc.Unmarshal(entry.Data); err != nil {
			return fmt.Errorf("unmarshalling EntryConfChangeV2: %w", err)
		}
	}

	ctx := mgr.ctx
	if mgr.options.opTimeout != 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, mgr.options.opTimeout)
		defer cancel()
	}

	// Raft manager haven't supported membership management, yet. The only member of the group is the one who
	// initiates the Raft group. Thus, there is no need to maintain the list of nodes inside DB. We now place an
	// entry log entry, similar to empty verification message. This placement is temporary.
	lsn := storage.LSN(entry.Index)
	logEntry, err := mgr.wal.InsertLogEntry(ctx, lsn, nil, &gitalypb.LogEntry_Metadata{
		RaftTerm: uint64(lsn),
		RaftType: uint64(gitalypb.RaftMessageType_CONFIG_CHANGE),
	})
	if err != nil {
		return fmt.Errorf("inserting config change log entry: %w", err)
	}
	mgr.recordEntryIfNeeded(true, lsn, logEntry)
	return nil
}

// insertEmptyLogEntry inserts an empty log entry at the desirable position in WAL.
func (mgr *Manager) insertEmptyLogEntry(lsn storage.LSN) error {
	ctx := mgr.ctx
	if mgr.options.opTimeout != 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, mgr.options.opTimeout)
		defer cancel()
	}
	logEntry, err := mgr.wal.InsertLogEntry(ctx, lsn, nil, &gitalypb.LogEntry_Metadata{
		RaftTerm: uint64(lsn),
		RaftType: uint64(gitalypb.RaftMessageType_VERIFICATION),
	})
	if err != nil {
		return fmt.Errorf("inserting empty verification log entry: %w", err)
	}
	mgr.recordEntryIfNeeded(true, lsn, logEntry)
	return nil
}

func (mgr *Manager) commitEntries(rd *raft.Ready) error {
	var shouldNotify bool
	for i := range rd.Entries {
		switch rd.Entries[i].Type {
		case raftpb.EntryNormal:
			lsn := storage.LSN(rd.Entries[i].Index)

			var message gitalypb.RaftMessageV1
			if err := proto.Unmarshal(rd.Entries[i].Data, &message); err != nil {
				return fmt.Errorf("unmarshalling entry type: %w", err)
			}
			if err := mgr.wal.CommitLogEntry(mgr.ctx, lsn); err != nil {
				return fmt.Errorf("processing committed entry: %w", err)
			}

			// When Raft injects internal log entries, there are two main scenarios:
			// - Those entries are piggy-backed when WAL processes a transaction.
			// - They are injected independently while WAL loop is idle.
			// In the first scenario, after complete a transaction, WAL circles back to process entries
			// since the last committed LSN. Internal log entries are then processed as normal.
			// In the second scenario, Raft needs to notify WAL about new committed entries. Otherwise, they
			// are not applied until the next incoming transaction.
			selfSubmit := mgr.registry.Untrack(EventID(message.GetId()))
			metadata := message.GetLogEntry().GetMetadata()
			if metadata.GetRaftType() != uint64(gitalypb.RaftMessageType_NORMAL) || !selfSubmit {
				shouldNotify = true
			}
		case raftpb.EntryConfChange, raftpb.EntryConfChangeV2:
			if err := mgr.commitConfChange(rd.Entries[i]); err != nil {
				return fmt.Errorf("processing config change: %w", err)
			}
			shouldNotify = true
		default:
			return fmt.Errorf("raft entry type not supported: %s", rd.Entries[i].Type)
		}
	}
	if shouldNotify {
		mgr.wal.NotifyNewCommittedEntry()
	}
	return nil
}

func (mgr *Manager) commitConfChange(entry raftpb.Entry) error {
	var cc raftpb.ConfChangeI
	if entry.Type == raftpb.EntryConfChange {
		var cc1 raftpb.ConfChange
		if err := cc1.Unmarshal(entry.Data); err != nil {
			return fmt.Errorf("unmarshalling EntryConfChange: %w", err)
		}
		cc = cc1
	} else {
		var cc2 raftpb.ConfChangeV2
		if err := cc2.Unmarshal(entry.Data); err != nil {
			return fmt.Errorf("unmarshalling EntryConfChangeV2: %w", err)
		}
		cc = cc2
	}
	if err := mgr.wal.CommitLogEntry(mgr.ctx, storage.LSN(entry.Index)); err != nil {
		return fmt.Errorf("committing log entry: %w", err)
	}
	confState := mgr.node.ApplyConfChange(cc)
	if err := mgr.state.SaveConfState(confState); err != nil {
		return fmt.Errorf("saving config state: %w", err)
	}

	// Mark Raft ready for service after the first commit change entry is applied. After restart,
	// we don't need this anymore.
	mgr.ready.set()
	return nil
}

func (mgr *Manager) sendMessages(rd *raft.Ready) error {
	return mgr.transport.Send(mgr.ctx, mgr.wal.LSNDirPath, rd.Messages)
}

func (mgr *Manager) handleSoftState(rd *raft.Ready) error {
	state := rd.SoftState
	if state == nil {
		return nil
	}
	prevLeader := mgr.leadership.GetLeaderID()
	changed, duration := mgr.leadership.SetLeader(state.Lead, state.RaftState == raft.StateLeader)

	if changed {
		mgr.logger.WithFields(log.Fields{
			"raft.leader_id":           mgr.leadership.GetLeaderID(),
			"raft.is_leader":           mgr.leadership.IsLeader(),
			"raft.previous_leader_id":  prevLeader,
			"raft.leadership_duration": duration,
		}).Info("leadership updated")
	}
	return nil
}

func (mgr *Manager) handleHardState(rd *raft.Ready) error {
	if raft.IsEmptyHardState(rd.HardState) {
		return nil
	}
	if err := mgr.state.SaveHardState(rd.HardState); err != nil {
		return fmt.Errorf("saving hard state: %w", err)
	}
	return nil
}

func (mgr *Manager) recordEntryIfNeeded(fromRaft bool, lsn storage.LSN, logEntry *gitalypb.LogEntry) {
	if mgr.EntryRecorder != nil {
		mgr.EntryRecorder.Record(fromRaft, lsn, logEntry)
	}
}
