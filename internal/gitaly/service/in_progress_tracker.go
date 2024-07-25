package service

import (
	"sync"
)

// InProgressTracker can be used to keep track of processes that are in flight
type InProgressTracker interface {
	GetInProgress(key string) int
	IncrementInProgress(key string)
	DecrementInProgress(key string)
}

type inProgressTracker struct {
	inProgress map[string]int
	l          sync.RWMutex
}

// NewInProgressTracker instantiates a new inProgressTracker.
func NewInProgressTracker() *inProgressTracker {
	return &inProgressTracker{
		inProgress: make(map[string]int),
	}
}

// GetInProgress gets the number of inflight processes for a given key.
func (p *inProgressTracker) GetInProgress(key string) int {
	p.l.RLock()
	defer p.l.RUnlock()

	return p.inProgress[key]
}

// IncrementInProgress increments the number of inflight processes for a given key.
func (p *inProgressTracker) IncrementInProgress(key string) {
	p.l.Lock()
	defer p.l.Unlock()

	p.inProgress[key]++
}

// DecrementInProgress decrements the number of inflight processes for a given key.
func (p *inProgressTracker) DecrementInProgress(key string) {
	p.l.Lock()
	defer p.l.Unlock()

	p.inProgress[key]--
}
