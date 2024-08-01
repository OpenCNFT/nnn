package raft

import (
	"container/list"
	"context"
	"fmt"
	"sync"

	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/storagemgr"
	"gitlab.com/gitlab-org/gitaly/v16/internal/log"
)

type notification struct {
	partitionID   storage.PartitionID
	lowWaterMark  storage.LSN
	highWaterMark storage.LSN
}

type partitionState struct {
	nextLSN storage.LSN
}

type notifications struct {
	storageName     string
	list            *list.List
	signal          chan struct{}
	partitionStates map[storage.PartitionID]*partitionState
}

type replicatorPusher struct {
	finish     chan struct{}
	done       chan struct{}
	replicator *replicator
}

// LogConsumer acts as a bridge that connects WAL and Raft replicator. It implements WAL's
// storagemgr.LogConsumer interface, listens for new notifications, and then pushes new notifications
// to the corresponding Raft replicator.
type LogConsumer struct {
	sync.Mutex

	ctx           context.Context
	logger        log.Logger
	logManager    storagemgr.LogManagerAccessor
	pushers       map[string]*replicatorPusher
	notifications map[string]*notifications
}

// Push starts a goroutine that pipe the notifications from WAL to the corresponding replicator of a storage.
func (l *LogConsumer) Push(storageName string, replicator *replicator) error {
	l.Lock()
	defer l.Unlock()

	if _, exist := l.pushers[storageName]; exist {
		return fmt.Errorf("storage %q already registered", storageName)
	}
	l.pushers[storageName] = &replicatorPusher{
		finish:     make(chan struct{}),
		done:       make(chan struct{}),
		replicator: replicator,
	}
	go l.pushNotifications(l.initializeNotifications(storageName), l.pushers[storageName])

	return nil
}

// NotifyNewTransactions is called by WAL's transaction manager when there are new appended logs or
// when the manager restarts. It appends the notification to a linked list. In the background, that
// linked list is consumed by another goroutine initiated by (*LogConsumer).Push.
func (l *LogConsumer) NotifyNewTransactions(storageName string, partitionID storage.PartitionID, lowWaterMark storage.LSN, highWaterMark storage.LSN) {
	l.Lock()
	defer l.Unlock()

	notifications := l.initializeNotifications(storageName)
	notifications.list.PushBack(&notification{
		partitionID:   partitionID,
		lowWaterMark:  lowWaterMark,
		highWaterMark: highWaterMark,
	})
	select {
	case notifications.signal <- struct{}{}:
	default:
	}
}

// Close stops all activities of this log consumer. The function exits when all background goroutines finish.
func (l *LogConsumer) Close() {
	for storageName := range l.pushers {
		close(l.pushers[storageName].finish)
	}
	for storageName := range l.pushers {
		<-l.pushers[storageName].done
	}
	l.pushers = map[string]*replicatorPusher{}
	l.notifications = map[string]*notifications{}
}

func (l *LogConsumer) pushNotifications(notifications *notifications, pusher *replicatorPusher) {
	defer close(pusher.done)

	for {
		select {
		case <-pusher.finish:
			return
		case <-notifications.signal:
		}

		for notifications.list.Front() != nil {
			select {
			case <-pusher.finish:
				return
			default:
			}
			item := notifications.list.Front()
			notifications.list.Remove(item)
			n := item.Value.(*notification)

			state, ok := notifications.partitionStates[n.partitionID]
			if !ok {
				state = &partitionState{nextLSN: n.lowWaterMark}
				notifications.partitionStates[n.partitionID] = state
			}

			// All log entries are already handled.
			if state.nextLSN > n.highWaterMark {
				continue
			} else if state.nextLSN < n.lowWaterMark {
				state.nextLSN = n.lowWaterMark
			}

			if err := l.logManager.CallLogManager(l.ctx, notifications.storageName, n.partitionID, func(manager storagemgr.LogManager) {
				for lsn := state.nextLSN; lsn <= n.highWaterMark; lsn++ {
					entry, err := manager.ReadEntry(lsn)
					if err != nil {
						l.logger.WithError(err).WithFields(log.Fields{
							"raft.authority_name": notifications.storageName,
							"raft.partition_id":   n.partitionID,
							"raft.lsn":            lsn,
						}).Error("fail to read log entry")
						continue
					}
					if entry.GetRepositoryCreation() != nil {
						if err := pusher.replicator.BroadcastNewPartition(raftID(n.partitionID), entry.RelativePath); err != nil {
							l.logger.WithError(err).WithFields(log.Fields{
								"raft.authority_name": notifications.storageName,
								"raft.partition_id":   n.partitionID,
							}).Error("fail to broadcast new partition")
						}
					} else {
						var shouldBroadcast bool
						if len(entry.ReferenceTransactions) > 0 || entry.Housekeeping != nil {
							shouldBroadcast = true
						}
						for _, o := range entry.Operations {
							if o.GetCreateHardLink() != nil || o.GetRemoveDirectoryEntry() != nil || o.GetCreateDirectory() != nil {
								shouldBroadcast = true
							}
						}
						if shouldBroadcast {
							if err := pusher.replicator.BroadcastLogEntry(raftID(n.partitionID), entry.RelativePath, entry); err != nil {
								l.logger.WithError(err).WithFields(log.Fields{
									"raft.authority_name": notifications.storageName,
									"raft.partition_id":   n.partitionID,
								}).Error("fail to broadcast new entry")
							}
						}
					}
					manager.AcknowledgeTransaction(l, lsn)
				}
				state.nextLSN = n.highWaterMark + 1
			}); err != nil {
				l.logger.WithError(err).Error("failed to acknowledge log entry")
			}
		}
	}
}

func (l *LogConsumer) initializeNotifications(storageName string) *notifications {
	if _, ok := l.notifications[storageName]; !ok {
		l.notifications[storageName] = &notifications{
			storageName:     storageName,
			list:            &list.List{},
			signal:          make(chan struct{}, 1),
			partitionStates: map[storage.PartitionID]*partitionState{},
		}
	}
	return l.notifications[storageName]
}

// NewLogConsumer is a factory that returns new LogConsumer object for the input LogManagerAccessor.
func NewLogConsumer(ctx context.Context, lma storagemgr.LogManagerAccessor, logger log.Logger) storagemgr.LogConsumer {
	return &LogConsumer{
		ctx:        ctx,
		logManager: lma,
		logger: logger.WithFields(log.Fields{
			"component":      "raft",
			"raft.component": "log_consumer",
		}),
		pushers:       map[string]*replicatorPusher{},
		notifications: map[string]*notifications{},
	}
}
