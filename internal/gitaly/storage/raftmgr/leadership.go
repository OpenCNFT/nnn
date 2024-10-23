package raftmgr

import (
	"sync"
	"time"
)

// Leadership manages the state of leadership in a distributed system using the Raft consensus algorithm. It tracks the
// current leader's ID, whether this node is the leader, and the time since the last leadership change. It also provides
// a notification mechanism for leadership changes through a channel.
type Leadership struct {
	sync.Mutex
	leaderID   uint64
	isLeader   bool
	lastChange time.Time
	newLeaderC chan struct{}
}

// NewLeadership initializes a new Leadership instance with the current time and a buffered channel.
func NewLeadership() *Leadership {
	return &Leadership{
		lastChange: time.Now(),
		newLeaderC: make(chan struct{}, 1),
	}
}

// SetLeader updates the leadership information if there is a change in the leaderID.
// It returns a boolean indicating whether a change occurred and the duration of the last leadership.
func (l *Leadership) SetLeader(leaderID uint64, isLeader bool) (changed bool, lastDuration time.Duration) {
	l.Lock()
	defer l.Unlock()

	if l.leaderID == leaderID && l.isLeader == isLeader {
		return false, 0
	}

	l.leaderID = leaderID
	l.isLeader = isLeader
	now := time.Now()
	lastDuration = now.Sub(l.lastChange)
	l.lastChange = now

	select {
	case l.newLeaderC <- struct{}{}:
	default:
	}

	return true, lastDuration
}

// IsLeader returns true if the current instance is the leader.
func (l *Leadership) IsLeader() bool {
	l.Lock()
	defer l.Unlock()
	return l.isLeader
}

// GetLeaderID retrieves the current leader's ID.
func (l *Leadership) GetLeaderID() uint64 {
	l.Lock()
	defer l.Unlock()
	return l.leaderID
}
