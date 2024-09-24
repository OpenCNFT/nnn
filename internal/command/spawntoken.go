package command

import (
	"context"
	"fmt"
	"time"

	"github.com/kelseyhightower/envconfig"
	"github.com/prometheus/client_golang/prometheus"
	"gitlab.com/gitlab-org/gitaly/v16/internal/log"
	"gitlab.com/gitlab-org/gitaly/v16/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v16/internal/tracing"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
	"google.golang.org/protobuf/types/known/durationpb"
)

// SpawnConfig holds configuration for command spawning timeouts and parallelism.
type SpawnConfig struct {
	// This default value (10 seconds) is very high. Spawning should take
	// milliseconds or less. If we hit 10 seconds, something is wrong, and
	// failing the request will create breathing room.
	Timeout time.Duration `default:"10s" split_words:"true"`

	// MaxSpawnParallel limits the number of goroutines that can spawn a
	// process at the same time. These parallel spawns will contend for a
	// single lock (syscall.ForkLock) in exec.Cmd.Start(). Can be modified at
	// runtime with the GITALY_COMMAND_SPAWN_MAX_PARALLEL variable.
	//
	// Note that this does not limit the total number of child processes that
	// can be attached to Gitaly at the same time. It only limits the rate at
	// which we can create new child processes.
	MaxParallel int `default:"10" split_words:"true"`
}

// SpawnTokenManager limits the number of goroutines that can spawn a process at a time.
type SpawnTokenManager struct {
	spawnTokens               chan struct{}
	spawnConfig               SpawnConfig
	spawnTimeoutCount         prometheus.Counter
	spawnTokenWaitingLength   prometheus.Gauge
	spawnWaitingTimeHistogram prometheus.Histogram
	spawnForkingTimeHistogram prometheus.Histogram
}

// Describe is used to describe Prometheus metrics.
func (m *SpawnTokenManager) Describe(descs chan<- *prometheus.Desc) {
	prometheus.DescribeByCollect(m, descs)
}

// Collect is used to collect Prometheus metrics.
func (m *SpawnTokenManager) Collect(metrics chan<- prometheus.Metric) {
	m.spawnTimeoutCount.Collect(metrics)
	m.spawnTokenWaitingLength.Collect(metrics)
	m.spawnWaitingTimeHistogram.Collect(metrics)
	m.spawnForkingTimeHistogram.Collect(metrics)
}

// NewSpawnTokenManager creates a SpawnTokenManager object from the input config
func NewSpawnTokenManager(config SpawnConfig) *SpawnTokenManager {
	return &SpawnTokenManager{
		spawnTokens: make(chan struct{}, config.MaxParallel),
		spawnConfig: config,
		spawnTimeoutCount: prometheus.NewCounter(
			prometheus.CounterOpts{
				Name: "gitaly_spawn_timeouts_total",
				Help: "Number of process spawn timeouts",
			},
		),
		spawnTokenWaitingLength: prometheus.NewGauge(
			prometheus.GaugeOpts{
				Name: "gitaly_spawn_token_waiting_length",
				Help: "The current length of the queue waiting for spawn tokens",
			},
		),
		spawnWaitingTimeHistogram: prometheus.NewHistogram(
			prometheus.HistogramOpts{
				Name:    "gitaly_spawn_waiting_time_seconds",
				Help:    "Histogram of time waiting for spawn tokens",
				Buckets: []float64{0.001, 0.005, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 5.0, 10.0},
			},
		),
		spawnForkingTimeHistogram: prometheus.NewHistogram(
			prometheus.HistogramOpts{
				Name:    "gitaly_spawn_forking_time_seconds",
				Help:    "Histogram of actual forking time after spawn tokens are acquired",
				Buckets: []float64{0.001, 0.005, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 5.0, 10.0},
			},
		),
	}
}

// NewSpawnTokenManagerFromEnv creates a SpawnTokenManager object with the config parsed from environment variables
// - GITALY_COMMAND_SPAWN_TIMEOUT for spawn token `Timeout` config
// - GITALY_COMMAND_SPAWN_MAX_PARALLEL for spawn token `MaxParallel` config
func NewSpawnTokenManagerFromEnv() (*SpawnTokenManager, error) {
	var spawnConfig SpawnConfig
	err := envconfig.Process("gitaly_command_spawn", &spawnConfig)
	if err != nil {
		return nil, err
	}
	return NewSpawnTokenManager(spawnConfig), nil
}

// GetSpawnToken blocks until the caller either acquires a token or timeout. The caller is expected to call returned
// function to put the token back to the queue.
func (m *SpawnTokenManager) GetSpawnToken(ctx context.Context) (putToken func(), err error) {
	// Go has a global lock (syscall.ForkLock) for spawning new processes.
	// This select statement is a safety valve to prevent lots of Gitaly
	// requests from piling up behind the ForkLock if forking for some reason
	// slows down. This has happened in real life, see
	// https://gitlab.com/gitlab-org/gitaly/issues/823.
	startWaiting := time.Now()
	m.spawnTokenWaitingLength.Inc()
	defer m.spawnTokenWaitingLength.Dec()

	span, ctx := tracing.StartSpanIfHasParent(ctx, "command.getSpawnToken", nil)
	defer span.Finish()

	select {
	case m.spawnTokens <- struct{}{}:
		m.recordQueuingTime(ctx, startWaiting, "")
		return func() {
			<-m.spawnTokens
		}, nil
	case <-time.After(m.spawnConfig.Timeout):
		m.recordQueuingTime(ctx, startWaiting, "spawn token timeout")
		m.spawnTimeoutCount.Inc()

		msg := fmt.Sprintf("process spawn timed out after %v", m.spawnConfig.Timeout)
		return nil, structerr.NewResourceExhausted("%s", msg).WithDetail(&gitalypb.LimitError{
			ErrorMessage: msg,
			RetryAfter:   durationpb.New(0),
		})
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (m *SpawnTokenManager) recordQueuingTime(ctx context.Context, start time.Time, msg string) {
	delta := time.Since(start)
	m.spawnWaitingTimeHistogram.Observe(delta.Seconds())

	if customFields := log.CustomFieldsFromContext(ctx); customFields != nil {
		customFields.RecordSum("command.spawn_token_wait_ms", int(delta.Milliseconds()))
		if len(msg) != 0 {
			customFields.RecordMetadata("command.spawn_token_error", msg)
		}
	}
}

func (m *SpawnTokenManager) recordForkTime(ctx context.Context, start time.Time) {
	delta := time.Since(start)
	m.spawnForkingTimeHistogram.Observe(delta.Seconds())

	if customFields := log.CustomFieldsFromContext(ctx); customFields != nil {
		customFields.RecordSum("command.spawn_token_fork_ms", int(delta.Milliseconds()))
	}
}
