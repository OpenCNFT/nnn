package bundleuri

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"gitlab.com/gitlab-org/gitaly/v16/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v16/internal/limiter"
)

const globalLimitingKey = "bundle-uri-generation"

var (
	// ErrBundleGenerationInProgress indicates there is a bundle already
	// being generated for this repository.
	ErrBundleGenerationInProgress = errors.New("a bundle is already beng generated for this repository")
	// ErrBundleGenerationConcurrencyReached indicates the global concurrency
	// limit has been reached.
	ErrBundleGenerationConcurrencyReached = errors.New("bundle generation concurrency reached")
	// ErrTerminatedEarly indicates the context was cancelled, or the
	// goroutine handling the bundle generation was stopped.
	ErrTerminatedEarly = errors.New("bundle generatio terminated early")
)

// BundleGenerationManager manages bundle generation. It handles requests to
// generate bundles for a repository, and enforces concurrency by limiting one
// bundle generation per repo at any given time as well as a global limit across
// all repositories.
type BundleGenerationManager struct {
	limiter                    limiter.Limiter
	sink                       *Sink
	bundleGenerationInProgress sync.Map
	stopChan                   chan struct{}
	wg                         sync.WaitGroup
}

// NewBundleGenerationManager creates a new BundleGenerationManager
func NewBundleGenerationManager(sink *Sink, concurrencyLimiter limiter.Limiter) *BundleGenerationManager {
	return &BundleGenerationManager{
		limiter:  concurrencyLimiter,
		sink:     sink,
		stopChan: make(chan struct{}),
	}
}

// StopAll blocks until all of the goroutines that are generating bundles are finished.
func (b *BundleGenerationManager) StopAll() {
	close(b.stopChan)
	b.wg.Wait()
}

// generateOneAtATime generates a bundle for a repository, but only if there is not already
// one in flight.
func (b *BundleGenerationManager) generateOneAtATime(ctx context.Context, repo *localrepo.Repo) (string, error) {
	b.wg.Add(1)
	defer b.wg.Done()

	bundlePath := b.sink.relativePath(repo, defaultBundle)

	_, loaded := b.bundleGenerationInProgress.LoadOrStore(
		bundlePath,
		struct{}{},
	)
	if loaded {
		return "", ErrBundleGenerationInProgress
	}
	defer b.bundleGenerationInProgress.Delete(bundlePath)

	errChan := make(chan error)

	go func() {
		errChan <- b.sink.Generate(ctx, repo)
	}()

	select {
	case err := <-errChan:
		return bundlePath, err
	case <-b.stopChan:
		return "", ErrTerminatedEarly
	}
}

func (b *BundleGenerationManager) generateOneAtATimeLimitFunc(ctx context.Context, repo *localrepo.Repo) limiter.LimitedFunc {
	return func() (interface{}, error) {
		var bundlePath string
		var err error
		if bundlePath, err = b.generateOneAtATime(ctx, repo); err != nil {
			return nil, err
		}

		return bundlePath, nil
	}
}

// Generate generates a bundle for a given repository while enforcing
// concurrency limits
func (b *BundleGenerationManager) Generate(ctx context.Context, repo *localrepo.Repo) error {
	if _, err := b.limiter.Limit(ctx, globalLimitingKey, b.generateOneAtATimeLimitFunc(ctx, repo)); err != nil {
		return fmt.Errorf("error generating bundle: %w", err)
	}

	return nil
}
