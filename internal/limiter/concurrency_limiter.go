package limiter

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"gitlab.com/gitlab-org/gitaly/v16/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v16/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v16/internal/tracing"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
	"google.golang.org/protobuf/types/known/durationpb"
)

const (
	// TypePerRPC is a concurrency limiter whose key is the full method of gRPC server. All
	// requests of the same method shares the concurrency limit.
	TypePerRPC = "per-rpc"
	// TypePackObjects is a dedicated concurrency limiter for pack-objects. It uses request
	// information (RemoteIP/Repository/User) as the limiting key.
	TypePackObjects = "pack-objects"
)

// ErrMaxQueueTime indicates a request has reached the maximum time allowed to wait in the
// concurrency queue.
var ErrMaxQueueTime = errors.New("maximum time in concurrency queue reached")

// ErrMaxQueueSize indicates the concurrency queue has reached its maximum size
var ErrMaxQueueSize = errors.New("maximum queue size reached")

// QueueTickerCreator is a function that provides a ticker
type QueueTickerCreator func() helper.Ticker

// keyedConcurrencyLimiter is a concurrency limiter that applies to a specific keyed resource.
type keyedConcurrencyLimiter struct {
	refcount               int
	monitor                ConcurrencyMonitor
	maxQueuedTickerCreator QueueTickerCreator

	// concurrencyTokens is the channel of available concurrency tokens, where every token
	// allows one concurrent call to the concurrency-limited function.
	concurrencyTokens *resizableSemaphore
	// queueTokens is the channel of available queue tokens, where every token allows one
	// concurrent call to be admitted to the queue.
	queueTokens *resizableSemaphore
}

// acquire tries to acquire the semaphore. It may fail if the admission queue is full or if the max
// queue-time ticker ticks before acquiring a concurrency token.
func (sem *keyedConcurrencyLimiter) acquire(ctx context.Context, limitingKey string) (returnedErr error) {
	if sem.queueTokens != nil {
		// Try to acquire the queueing token. The queueing token is used to control how many
		// callers may wait for the concurrency token at the same time. If there are no more
		// queueing tokens then this indicates that the queue is full and we thus return an
		// error immediately.
		if !sem.queueTokens.TryAcquire() {
			return ErrMaxQueueSize
		}

		// We have acquired a queueing token, so we need to release it if acquiring
		// the concurrency token fails. If we successfully acquire the concurrency
		// token though then we retain the queueing token until the caller signals
		// that the concurrency-limited function has finished. As a consequence the
		// queue token is returned together with the concurrency token.
		//
		// A simpler model would be to just have `maxQueueLength` many queueing
		// tokens. But this would add concurrency-limiting when acquiring the queue
		// token itself, which is not what we want to do. Instead, we want to admit
		// as many callers into the queue as the queue length permits plus the
		// number of available concurrency tokens allows.
		defer func() {
			if returnedErr != nil {
				sem.queueTokens.Release()
			}
		}()
	}

	// We are queued now, so let's tell the monitor. Furthermore, even though we're still
	// holding the queueing token when this function exits successfully we also tell the monitor
	// that we have exited the queue. It is only an implementation detail anyway that we hold on
	// to the token, so the monitor shouldn't care about that.
	sem.monitor.Queued(ctx, limitingKey, sem.queueLength())
	defer sem.monitor.Dequeued(ctx)

	// Set up the ticker that keeps us from waiting indefinitely on the concurrency token.
	var ticker helper.Ticker
	if sem.maxQueuedTickerCreator != nil {
		ticker = sem.maxQueuedTickerCreator()
	} else {
		ticker = helper.Ticker(helper.NewManualTicker())
	}

	defer ticker.Stop()
	ticker.Reset()

	// Try to acquire the concurrency token now that we're in the queue.
	select {
	case acquired := <-sem.concurrencyTokens.Acquire():
		// When the semaphore returns false, the semaphore was stopped. It's likely due to the context is
		// cancelled. Hence, we should return the error here.
		if !acquired {
			return sem.concurrencyTokens.Err()
		}
		return nil
	case <-ticker.C():
		return ErrMaxQueueTime
	case <-ctx.Done():
		return ctx.Err()
	}
}

// release releases the acquired tokens.
func (sem *keyedConcurrencyLimiter) release() {
	if sem.queueTokens != nil {
		sem.queueTokens.Release()
	}
	sem.concurrencyTokens.Release()
}

// queueLength returns the length of token queue
func (sem *keyedConcurrencyLimiter) queueLength() int {
	if sem.queueTokens == nil {
		return 0
	}
	return int(sem.queueTokens.Current())
}

// ConcurrencyLimiter contains rate limiter state.
type ConcurrencyLimiter struct {
	// ctx stores the context at initialization. This context is used as a stopping condition
	// for some internal goroutines.
	ctx context.Context
	// limit is the adaptive maximum number of concurrent calls to the limited function. This limit is
	// calculated adaptively from an outside calculator.
	limit *AdaptiveLimit
	// maxQueueLength is the maximum number of operations allowed to wait in a queued state.
	// This limit is global and applies before the concurrency limit. Subsequent incoming
	// operations will be rejected with an error immediately.
	maxQueueLength int64
	// maxQueuedTickerCreator is a function that creates a ticker used to determine how long a
	// call may be queued.
	maxQueuedTickerCreator QueueTickerCreator

	// monitor is a monitor that will get notified of the state of concurrency-limited RPC
	// calls.
	monitor ConcurrencyMonitor

	m sync.RWMutex
	// limitsByKey tracks all concurrency limits per key. Its per-key entries are lazily created
	// and will get evicted once there are no concurrency-limited calls for any such key
	// anymore.
	limitsByKey map[string]*keyedConcurrencyLimiter
}

// NewConcurrencyLimiter creates a new concurrency rate limiter.
func NewConcurrencyLimiter(ctx context.Context, limit *AdaptiveLimit, maxQueueLength int, maxQueuedTickerCreator QueueTickerCreator, monitor ConcurrencyMonitor) *ConcurrencyLimiter {
	if monitor == nil {
		monitor = NewNoopConcurrencyMonitor()
	}

	limiter := &ConcurrencyLimiter{
		ctx:                    ctx,
		limit:                  limit,
		maxQueueLength:         int64(maxQueueLength),
		maxQueuedTickerCreator: maxQueuedTickerCreator,
		monitor:                monitor,
		limitsByKey:            make(map[string]*keyedConcurrencyLimiter),
	}

	// When the capacity of the limiter is updated we also need to update the size of both the queuing tokens as
	// well as the concurrency tokens to match the new size.
	limit.AfterUpdate(func(val int) {
		for _, keyedLimiter := range limiter.limitsByKey {
			if keyedLimiter.queueTokens != nil {
				keyedLimiter.queueTokens.Resize(int64(val) + limiter.maxQueueLength)
			}
			keyedLimiter.concurrencyTokens.Resize(int64(val))
		}
	})
	return limiter
}

// Limit will limit the concurrency of the limited function f. There are two distinct mechanisms
// that limit execution of the function:
//
//  1. First, every call will enter the per-key queue. This queue limits how many callers may try to
//     acquire their per-key semaphore at the same time. If the queue is full the caller will be
//     rejected.
//  2. Second, when the caller has successfully entered the queue, they try to acquire their per-key
//     semaphore. If this takes longer than the maximum queueing limit then the caller will be
//     dequeued and gets an error.
func (c *ConcurrencyLimiter) Limit(ctx context.Context, limitingKey string, f LimitedFunc) (interface{}, error) {
	span, ctx := tracing.StartSpanIfHasParent(
		ctx,
		"limiter.ConcurrencyLimiter.Limit",
		tracing.Tags{"key": limitingKey},
	)
	defer span.Finish()

	if c.currentLimit() <= 0 {
		return f()
	}

	sem := c.getConcurrencyLimit(limitingKey)
	defer c.putConcurrencyLimit(limitingKey)

	start := time.Now()

	if err := sem.acquire(ctx, limitingKey); err != nil {
		queueTime := time.Since(start)
		switch err {
		case ErrMaxQueueSize:
			c.monitor.Dropped(ctx, limitingKey, sem.queueLength(), queueTime, "max_size")
			return nil, structerr.NewResourceExhausted("%w", ErrMaxQueueSize).WithDetail(&gitalypb.LimitError{
				ErrorMessage: err.Error(),
				RetryAfter:   durationpb.New(0),
			})
		case ErrMaxQueueTime:
			c.monitor.Dropped(ctx, limitingKey, sem.queueLength(), queueTime, "max_time")
			return nil, structerr.NewResourceExhausted("%w", ErrMaxQueueTime).WithDetail(&gitalypb.LimitError{
				ErrorMessage: err.Error(),
				RetryAfter:   durationpb.New(0),
			})
		default:
			c.monitor.Dropped(ctx, limitingKey, sem.queueLength(), queueTime, "other")
			return nil, fmt.Errorf("unexpected error when dequeueing request: %w", err)
		}
	}
	defer sem.release()

	c.monitor.Enter(ctx, time.Since(start))
	defer c.monitor.Exit(ctx)
	return f()
}

// getConcurrencyLimit retrieves the concurrency limit for the given key. If no such limiter exists
// it will be lazily constructed.
func (c *ConcurrencyLimiter) getConcurrencyLimit(limitingKey string) *keyedConcurrencyLimiter {
	c.m.Lock()
	defer c.m.Unlock()

	if c.limitsByKey[limitingKey] == nil {
		// Set up the queue tokens in case a maximum queue length was requested. As the
		// queue tokens are kept during the whole lifetime of the concurrency-limited
		// function we add the concurrency tokens to the number of available token.
		var queueTokens *resizableSemaphore
		if c.maxQueueLength > 0 {
			queueTokens = NewResizableSemaphore(c.ctx, c.currentLimit()+c.maxQueueLength)
		}

		c.limitsByKey[limitingKey] = &keyedConcurrencyLimiter{
			monitor:                c.monitor,
			maxQueuedTickerCreator: c.maxQueuedTickerCreator,
			concurrencyTokens:      NewResizableSemaphore(c.ctx, c.currentLimit()),
			queueTokens:            queueTokens,
		}
	}

	c.limitsByKey[limitingKey].refcount++

	return c.limitsByKey[limitingKey]
}

// putConcurrencyLimit drops the reference to the concurrency limit identified by the given key.
// This must only ever be called after `getConcurrencyLimit()` for the same key. If the reference
// count of the concurrency limit drops to zero then it will be destroyed.
func (c *ConcurrencyLimiter) putConcurrencyLimit(limitingKey string) {
	c.m.Lock()
	defer c.m.Unlock()

	ref := c.limitsByKey[limitingKey]
	if ref == nil {
		panic("semaphore should be in the map")
	}

	if ref.refcount <= 0 {
		panic(fmt.Sprintf("bad semaphore ref refcount %d", ref.refcount))
	}

	ref.refcount--
	if ref.refcount == 0 {
		delete(c.limitsByKey, limitingKey)
	}
}

func (c *ConcurrencyLimiter) countSemaphores() int {
	c.m.RLock()
	defer c.m.RUnlock()

	return len(c.limitsByKey)
}

func (c *ConcurrencyLimiter) currentLimit() int64 {
	return int64(c.limit.Current())
}
