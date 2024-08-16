package limiter

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"gitlab.com/gitlab-org/gitaly/v16/internal/featureflag"
	"gitlab.com/gitlab-org/gitaly/v16/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v16/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v16/internal/tracing"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
	"google.golang.org/protobuf/types/known/durationpb"
)

// semaphorer is an interface of a specific-purpose semaphore used for concurrency control.
type semaphorer interface {
	Acquire(ctx context.Context) error
	TryAcquire() error
	Release()
	Count() int
}

// staticSemaphore implements semaphorer interface. It is a wrapper for a buffer channel. When a caller wants to acquire
// the semaphore, a token is pushed to the channel. In contrast, when it wants to release the semaphore, it pulls one
// token from the channel.
type staticSemaphore struct {
	queue chan struct{}
}

// newStaticSemaphore initializes and returns a staticSemaphore instance.
func newStaticSemaphore(size uint) *staticSemaphore {
	return &staticSemaphore{queue: make(chan struct{}, size)}
}

// Acquire acquires the semaphore. The caller is blocked until there is a available resource or the context is cancelled.
func (s *staticSemaphore) Acquire(ctx context.Context) error {
	select {
	case s.queue <- struct{}{}:
		return nil
	case <-ctx.Done():
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			return ErrMaxQueueTime
		}
		return ctx.Err()
	}
}

// TryAcquire tries to acquire the semaphore. If it fails to do so, it will immediately return ErrMaxQueueSize and no change is made to the semaphore.
func (s *staticSemaphore) TryAcquire() error {
	select {
	case s.queue <- struct{}{}:
		return nil
	default:
		return ErrMaxQueueSize
	}
}

// Release releases the semaphore by pushing a token back to the underlying channel.
func (s *staticSemaphore) Release() {
	<-s.queue
}

// Count returns the amount of current concurrent access to the semaphore.
func (s *staticSemaphore) Count() int {
	return len(s.queue)
}

const (
	// TypePerRPC is a concurrency limiter whose key is the full method of gRPC server. All
	// requests of the same method shares the concurrency limit.
	TypePerRPC = "per-rpc"
	// TypePackObjects is a dedicated concurrency limiter for pack-objects. It uses request
	// information (RemoteIP/Repository/User) as the limiting key.
	TypePackObjects = "pack-objects"
	// TypeBundleGeneration is a dedicated concurrency limiter for
	// bundle-generation.
	TypeBundleGeneration = "bundle-generation"
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
	refcount              int
	monitor               ConcurrencyMonitor
	maxQueueWait          time.Duration
	setWaitTimeoutContext func() context.Context

	// concurrencyTokens is the counting semaphore to control available concurrency tokens, where every token
	// allows one concurrent call to the concurrency-limited function.
	concurrencyTokens semaphorer
	// queueTokens is the counting semaphore to control available queue tokens, where every token allows one
	// concurrent call to be admitted to the queue.
	queueTokens semaphorer
}

// acquire tries to acquire the semaphore. It may fail if the admission queue is full or if the max
// queue-time ticker ticks before acquiring a concurrency token.
func (sem *keyedConcurrencyLimiter) acquire(ctx context.Context, limitingKey string) (returnedErr error) {
	if sem.queueTokens != nil {
		// Try to acquire the queueing token. The queueing token is used to control how many
		// callers may wait for the concurrency token at the same time. If there are no more
		// queueing tokens then this indicates that the queue is full and we thus return an
		// error immediately.
		if err := sem.queueTokens.TryAcquire(); err != nil {
			return err
		}
		// We have acquired a queueing token, so we need to release it if acquiring
		// the concurrency token fails. If we succeed to acquire the concurrency
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

	if sem.maxQueueWait != 0 {
		if sem.setWaitTimeoutContext != nil {
			ctx = sem.setWaitTimeoutContext()
		} else {
			var cancel context.CancelFunc
			ctx, cancel = context.WithTimeout(ctx, sem.maxQueueWait)
			defer cancel()
		}
	}

	// Try to acquire the concurrency token now that we're in the queue.
	return sem.concurrencyTokens.Acquire(ctx)
}

// release releases the acquired tokens.
func (sem *keyedConcurrencyLimiter) release() {
	if sem.queueTokens != nil {
		sem.queueTokens.Release()
	}
	sem.concurrencyTokens.Release()
}

// queueLength returns the length of the queue waiting for tokens.
func (sem *keyedConcurrencyLimiter) queueLength() int {
	if sem.queueTokens == nil {
		return 0
	}
	return sem.queueTokens.Count() - sem.concurrencyTokens.Count()
}

// inProgress returns the number of in-progress tokens.
func (sem *keyedConcurrencyLimiter) inProgress() int {
	return sem.concurrencyTokens.Count()
}

// ConcurrencyLimiter contains rate limiter state.
type ConcurrencyLimiter struct {
	// limit is the adaptive maximum number of concurrent calls to the limited function. This limit is
	// calculated adaptively from an outside calculator.
	limit *AdaptiveLimit
	// maxQueueLength is the maximum number of operations allowed to wait in a queued state.
	// This limit is global and applies before the concurrency limit. Subsequent incoming
	// operations will be rejected with an error immediately.
	maxQueueLength int
	// maxQueueWait is a time duration of an operation allowed to wait in the queue.
	maxQueueWait time.Duration
	// SetWaitTimeoutContext is a function for setting up timeout context. If this is nill, context.WithTimeout is
	// used. This function is for internal testing purpose only.
	SetWaitTimeoutContext func() context.Context

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
func NewConcurrencyLimiter(limit *AdaptiveLimit, maxQueueLength int, maxQueueWait time.Duration, monitor ConcurrencyMonitor) *ConcurrencyLimiter {
	if monitor == nil {
		monitor = NewNoopConcurrencyMonitor()
	}

	limiter := &ConcurrencyLimiter{
		limit:          limit,
		maxQueueLength: maxQueueLength,
		maxQueueWait:   maxQueueWait,
		monitor:        monitor,
		limitsByKey:    make(map[string]*keyedConcurrencyLimiter),
	}

	// When the capacity of the limiter is updated we also need to update the size of both the queuing tokens as
	// well as the concurrency tokens to match the new size.
	limit.AfterUpdate(func(val int) {
		for _, keyedLimiter := range limiter.limitsByKey {
			if keyedLimiter.queueTokens != nil {
				if semaphore, ok := keyedLimiter.queueTokens.(*resizableSemaphore); ok {
					semaphore.Resize(uint(val + limiter.maxQueueLength))
				}
			}
			if semaphore, ok := keyedLimiter.concurrencyTokens.(*resizableSemaphore); ok {
				semaphore.Resize(uint(val))
			}
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

	if c.currentLimit(ctx) <= 0 {
		return f()
	}

	sem := c.getConcurrencyLimit(ctx, limitingKey)
	defer c.putConcurrencyLimit(limitingKey)

	start := time.Now()

	if err := sem.acquire(ctx, limitingKey); err != nil {
		queueTime := time.Since(start)
		switch {
		case errors.Is(err, ErrMaxQueueSize):
			c.monitor.Dropped(ctx, limitingKey, sem.queueLength(), sem.inProgress(), queueTime, "max_size")
			return nil, structerr.NewResourceExhausted("%w", ErrMaxQueueSize).WithDetail(&gitalypb.LimitError{
				ErrorMessage: err.Error(),
				RetryAfter:   durationpb.New(0),
			})
		case errors.Is(err, ErrMaxQueueTime):
			c.monitor.Dropped(ctx, limitingKey, sem.queueLength(), sem.inProgress(), queueTime, "max_time")
			return nil, structerr.NewResourceExhausted("%w", ErrMaxQueueTime).WithDetail(&gitalypb.LimitError{
				ErrorMessage: err.Error(),
				RetryAfter:   durationpb.New(0),
			})
		default:
			c.monitor.Dropped(ctx, limitingKey, sem.queueLength(), sem.inProgress(), queueTime, "other")
			return nil, fmt.Errorf("unexpected error when dequeueing request: %w", err)
		}
	}
	defer sem.release()

	c.monitor.Enter(ctx, sem.inProgress(), time.Since(start))
	defer c.monitor.Exit(ctx)
	return f()
}

// getConcurrencyLimit retrieves the concurrency limit for the given key. If no such limiter exists
// it will be lazily constructed.
func (c *ConcurrencyLimiter) getConcurrencyLimit(ctx context.Context, limitingKey string) *keyedConcurrencyLimiter {
	c.m.Lock()
	defer c.m.Unlock()

	// We use `gitaly_use_resizable_semaphore_in_concurrency_limiter` feature flag to swap between
	// staticSemaphore and resizableSemaphore. By default, staticSemaphore is used. After the flag is
	// enabled, this map keeps the reference to the prior semaphore system as soon as there are further operations
	// using the same limiting key. During this period, they continue to use the old semaphore type. After there is
	// no more operation and the limiter is removed, the subsequent operations gonna use the new type. The same
	// behavior occurs when the flag is reverted.
	// This "stickiness" allows two semaphore systems to co-exist during partial rollout while still ensures
	// all operations of the same limiting key use the same semaphore. Otherwise, we need to detect feature
	// flag change in the current request, then migrate all counting to the new alternative semaphore. The
	// later approach is error-prone. It can also make the counting go back and forth constantly if the flag
	// actor does not align with the limiting key.
	if c.limitsByKey[limitingKey] == nil {
		// Set up the queue tokens in case a maximum queue length was requested. As the
		// queue tokens are kept during the whole lifetime of the concurrency-limited
		// function we add the concurrency tokens to the number of available token.
		var queueTokens semaphorer
		if c.maxQueueLength > 0 {
			queueTokens = c.createSemaphore(ctx, uint(c.currentLimit(ctx)+c.maxQueueLength))
		}

		c.limitsByKey[limitingKey] = &keyedConcurrencyLimiter{
			monitor:               c.monitor,
			maxQueueWait:          c.maxQueueWait,
			setWaitTimeoutContext: c.SetWaitTimeoutContext,
			concurrencyTokens:     c.createSemaphore(ctx, uint(c.currentLimit(ctx))),
			queueTokens:           queueTokens,
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

func (c *ConcurrencyLimiter) currentLimit(ctx context.Context) int {
	// When `gitaly_use_resizable_semaphore_in_concurrency_limiter` flag is enabled, the resizable semaphore should
	// use the current value of the adaptive limit. This limit is constantly calibrated by the adaptive calculator
	// if the adaptiveness is enabled. In contrast, when the flag is disabled, the static semaphore should use the
	// initial limit instead of the floating current limit.
	// This situation is temporary during the rollout phase where the adaptive limiting is experimental. The
	// aforementioned feature flag is used as an escape hatch to fallback to use static limiting if something goes
	// wrong. When the feature enters a mature state, this feature flag and the static semaphore will be removed.
	// The resizable semaphore can handle both static and adaptive limiting.
	if featureflag.UseResizableSemaphoreInConcurrencyLimiter.IsEnabled(ctx) {
		return c.limit.Current()
	}
	return c.limit.Initial()
}

func (c *ConcurrencyLimiter) createSemaphore(ctx context.Context, size uint) semaphorer {
	if featureflag.UseResizableSemaphoreInConcurrencyLimiter.IsEnabled(ctx) {
		return NewResizableSemaphore(size)
	}
	return newStaticSemaphore(size)
}
