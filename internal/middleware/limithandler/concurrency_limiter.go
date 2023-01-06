package limithandler

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus/ctxlogrus"
	"github.com/prometheus/client_golang/prometheus"
	"gitlab.com/gitlab-org/gitaly/v15/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v15/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v15/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v15/proto/go/gitalypb"
	"google.golang.org/protobuf/types/known/durationpb"
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
	tokens    chan struct{}
	count     int
	newTicker QueueTickerCreator
}

func (sem *keyedConcurrencyLimiter) acquire(ctx context.Context) error {
	var ticker helper.Ticker

	if sem.newTicker != nil {
		ticker = sem.newTicker()
	} else {
		ticker = helper.Ticker(helper.NewManualTicker())
	}

	defer ticker.Stop()
	ticker.Reset()

	select {
	case sem.tokens <- struct{}{}:
		return nil
	case <-ticker.C():
		return ErrMaxQueueTime
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (sem *keyedConcurrencyLimiter) release() { <-sem.tokens }

// ConcurrencyLimiter contains rate limiter state
type ConcurrencyLimiter struct {
	limitsByKey map[string]*keyedConcurrencyLimiter
	// maxPerKey is the maximum number of concurrent operations
	// per lockKey
	maxPerKey int64
	// queued tracks the current number of operations waiting to be picked up
	queued int64
	// queuedLimit is the maximum number of operations allowed to wait in a queued state.
	// subsequent incoming operations will fail with an error.
	queuedLimit         int64
	monitor             ConcurrencyMonitor
	mux                 sync.RWMutex
	maxWaitTickerGetter QueueTickerCreator
}

// NewConcurrencyLimiter creates a new concurrency rate limiter
func NewConcurrencyLimiter(perKeyLimit, globalLimit int, maxWaitTickerGetter QueueTickerCreator, monitor ConcurrencyMonitor) *ConcurrencyLimiter {
	if monitor == nil {
		monitor = NewNoopConcurrencyMonitor()
	}

	return &ConcurrencyLimiter{
		limitsByKey:         make(map[string]*keyedConcurrencyLimiter),
		maxPerKey:           int64(perKeyLimit),
		queuedLimit:         int64(globalLimit),
		monitor:             monitor,
		maxWaitTickerGetter: maxWaitTickerGetter,
	}
}

// Limit will limit the concurrency of f
func (c *ConcurrencyLimiter) Limit(ctx context.Context, lockKey string, f LimitedFunc) (interface{}, error) {
	if c.maxPerKey <= 0 {
		return f()
	}

	var decremented bool

	log := ctxlogrus.Extract(ctx).WithField("limiting_key", lockKey)
	if err := c.queueInc(ctx); err != nil {
		if errors.Is(err, ErrMaxQueueSize) {
			return nil, structerr.NewResourceExhausted("%w", ErrMaxQueueSize).WithDetail(
				&gitalypb.LimitError{
					ErrorMessage: err.Error(),
					RetryAfter:   durationpb.New(0),
				},
			)
		}

		log.WithError(err).Error("unexpected error when queueing request")
		return nil, err
	}
	defer c.queueDec(&decremented)

	start := time.Now()
	c.monitor.Queued(ctx)

	sem := c.getConcurrencyLimit(lockKey)
	defer c.putConcurrencyLimit(lockKey)

	err := sem.acquire(ctx)
	c.queueDec(&decremented)

	c.monitor.Dequeued(ctx)
	if err != nil {
		if errors.Is(err, ErrMaxQueueTime) {
			c.monitor.Dropped(ctx, "max_time")

			return nil, structerr.NewResourceExhausted("%w", ErrMaxQueueTime).WithDetail(&gitalypb.LimitError{
				ErrorMessage: err.Error(),
				RetryAfter:   durationpb.New(0),
			})
		}

		log.WithError(err).Error("unexpected error when dequeueing request")
		return nil, err
	}
	defer sem.release()

	c.monitor.Enter(ctx, time.Since(start))
	defer c.monitor.Exit(ctx)

	return f()
}

// Lazy create a semaphore for the given key
func (c *ConcurrencyLimiter) getConcurrencyLimit(lockKey string) *keyedConcurrencyLimiter {
	c.mux.Lock()
	defer c.mux.Unlock()

	if c.limitsByKey[lockKey] == nil {
		c.limitsByKey[lockKey] = &keyedConcurrencyLimiter{
			tokens:    make(chan struct{}, c.maxPerKey),
			newTicker: c.maxWaitTickerGetter,
		}
	}

	c.limitsByKey[lockKey].count++
	return c.limitsByKey[lockKey]
}

func (c *ConcurrencyLimiter) putConcurrencyLimit(lockKey string) {
	c.mux.Lock()
	defer c.mux.Unlock()

	ref := c.limitsByKey[lockKey]
	if ref == nil {
		panic("semaphore should be in the map")
	}

	if ref.count <= 0 {
		panic(fmt.Sprintf("bad semaphore ref count %d", ref.count))
	}

	ref.count--
	if ref.count == 0 {
		delete(c.limitsByKey, lockKey)
	}
}

func (c *ConcurrencyLimiter) countSemaphores() int {
	c.mux.RLock()
	defer c.mux.RUnlock()

	return len(c.limitsByKey)
}

func (c *ConcurrencyLimiter) queueInc(ctx context.Context) error {
	c.mux.Lock()
	defer c.mux.Unlock()

	if c.queuedLimit > 0 &&
		c.queued >= c.queuedLimit {
		c.monitor.Dropped(ctx, "max_size")
		return ErrMaxQueueSize
	}

	c.queued++
	return nil
}

func (c *ConcurrencyLimiter) queueDec(decremented *bool) {
	if decremented == nil || *decremented {
		return
	}
	*decremented = true
	c.mux.Lock()
	defer c.mux.Unlock()

	c.queued--
}

// WithConcurrencyLimiters sets up middleware to limit the concurrency of
// requests based on RPC and repository
func WithConcurrencyLimiters(cfg config.Cfg, middleware *LimiterMiddleware) {
	acquiringSecondsMetric := prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "gitaly",
			Subsystem: "concurrency_limiting",
			Name:      "acquiring_seconds",
			Help:      "Histogram of time calls are rate limited (in seconds)",
			Buckets:   cfg.Prometheus.GRPCLatencyBuckets,
		},
		[]string{"system", "grpc_service", "grpc_method"},
	)
	inProgressMetric := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "gitaly",
			Subsystem: "concurrency_limiting",
			Name:      "in_progress",
			Help:      "Gauge of number of concurrent in-progress calls",
		},
		[]string{"system", "grpc_service", "grpc_method"},
	)
	queuedMetric := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "gitaly",
			Subsystem: "concurrency_limiting",
			Name:      "queued",
			Help:      "Gauge of number of queued calls",
		},
		[]string{"system", "grpc_service", "grpc_method"},
	)

	middleware.collect = func(metrics chan<- prometheus.Metric) {
		acquiringSecondsMetric.Collect(metrics)
		inProgressMetric.Collect(metrics)
		queuedMetric.Collect(metrics)
	}

	result := make(map[string]Limiter)

	newTickerFunc := func() helper.Ticker {
		return helper.NewManualTicker()
	}

	for _, limit := range cfg.Concurrency {
		if limit.MaxQueueWait > 0 {
			limit := limit
			newTickerFunc = func() helper.Ticker {
				return helper.NewTimerTicker(limit.MaxQueueWait.Duration())
			}
		}

		result[limit.RPC] = NewConcurrencyLimiter(
			limit.MaxPerRepo,
			limit.MaxQueueSize,
			newTickerFunc,
			newPerRPCPromMonitor("gitaly", limit.RPC, queuedMetric, inProgressMetric,
				acquiringSecondsMetric, middleware.requestsDroppedMetric),
		)
	}

	// Set default for ReplicateRepository.
	replicateRepositoryFullMethod := "/gitaly.RepositoryService/ReplicateRepository"
	if _, ok := result[replicateRepositoryFullMethod]; !ok {
		result[replicateRepositoryFullMethod] = NewConcurrencyLimiter(
			1,
			0,
			func() helper.Ticker {
				return helper.NewManualTicker()
			},
			newPerRPCPromMonitor("gitaly", replicateRepositoryFullMethod, queuedMetric,
				inProgressMetric, acquiringSecondsMetric, middleware.requestsDroppedMetric))
	}

	middleware.methodLimiters = result
}
