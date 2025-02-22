package catfile

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v16/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/labkit/correlation"
	"google.golang.org/grpc/metadata"
)

func TestProcesses_add(t *testing.T) {
	ctx := testhelper.Context(t)

	const maxLen = 3
	p := &processes{maxLen: maxLen}

	cfg := testcfg.Build(t)
	repo, _ := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
		SkipCreationViaService: true,
	})

	key0 := mustCreateKey(t, "0", repo)
	value0, cancel := mustCreateCacheable(t, cfg, repo)
	p.Add(key0, value0, time.Now().Add(time.Hour), cancel)
	requireProcessesValid(t, p)

	key1 := mustCreateKey(t, "1", repo)
	value1, cancel := mustCreateCacheable(t, cfg, repo)
	p.Add(key1, value1, time.Now().Add(time.Hour), cancel)
	requireProcessesValid(t, p)

	key2 := mustCreateKey(t, "2", repo)
	value2, cancel := mustCreateCacheable(t, cfg, repo)
	p.Add(key2, value2, time.Now().Add(time.Hour), cancel)
	requireProcessesValid(t, p)

	// Because maxLen is 3, and key0 is oldest, we expect that adding key3
	// will kick out key0.
	key3 := mustCreateKey(t, "3", repo)
	value3, cancel := mustCreateCacheable(t, cfg, repo)
	p.Add(key3, value3, time.Now().Add(time.Hour), cancel)
	requireProcessesValid(t, p)

	require.Equal(t, maxLen, p.EntryCount(), "length should be maxLen")
	require.True(t, value0.isClosed(), "value0 should be closed")
	require.Equal(t, []key{key1, key2, key3}, keys(t, p))
}

func TestProcesses_addTwice(t *testing.T) {
	ctx := testhelper.Context(t)

	p := &processes{maxLen: 10}

	cfg := testcfg.Build(t)
	repo, _ := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
		SkipCreationViaService: true,
	})

	key0 := mustCreateKey(t, "0", repo)
	value0, cancel := mustCreateCacheable(t, cfg, repo)
	p.Add(key0, value0, time.Now().Add(time.Hour), cancel)
	requireProcessesValid(t, p)

	key1 := mustCreateKey(t, "1", repo)
	value1, cancel := mustCreateCacheable(t, cfg, repo)
	p.Add(key1, value1, time.Now().Add(time.Hour), cancel)
	requireProcessesValid(t, p)

	require.Equal(t, key0, p.head().key, "key0 should be oldest key")

	value2, cancel := mustCreateCacheable(t, cfg, repo)
	p.Add(key0, value2, time.Now().Add(time.Hour), cancel)
	requireProcessesValid(t, p)

	require.Equal(t, key1, p.head().key, "key1 should be oldest key")
	require.Equal(t, value1, p.head().value)

	require.True(t, value0.isClosed(), "value0 should be closed")
}

func TestProcesses_Checkout(t *testing.T) {
	ctx := testhelper.Context(t)

	p := &processes{maxLen: 10}

	cfg := testcfg.Build(t)
	repo, _ := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
		SkipCreationViaService: true,
	})

	key0 := mustCreateKey(t, "0", repo)
	value0, cancel := mustCreateCacheable(t, cfg, repo)
	p.Add(key0, value0, time.Now().Add(time.Hour), cancel)

	entry, ok := p.Checkout(key{sessionID: "foo"})
	requireProcessesValid(t, p)
	require.Nil(t, entry, "expect nil value when key not found")
	require.False(t, ok, "ok flag")

	entry, ok = p.Checkout(key0)
	requireProcessesValid(t, p)

	require.Equal(t, value0, entry.value)
	require.True(t, ok, "ok flag")

	require.False(t, entry.value.isClosed(), "value should not be closed after checkout")

	entry, ok = p.Checkout(key0)
	require.False(t, ok, "ok flag after second checkout")
	require.Nil(t, entry, "value from second checkout")
}

func TestProcesses_EnforceTTL(t *testing.T) {
	ctx := testhelper.Context(t)

	p := &processes{maxLen: 10}

	cfg := testcfg.Build(t)
	repo, _ := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
		SkipCreationViaService: true,
	})

	cutoff := time.Now()

	key0 := mustCreateKey(t, "0", repo)
	value0, cancel := mustCreateCacheable(t, cfg, repo)
	p.Add(key0, value0, cutoff.Add(-time.Hour), cancel)

	key1 := mustCreateKey(t, "1", repo)
	value1, cancel := mustCreateCacheable(t, cfg, repo)
	p.Add(key1, value1, cutoff.Add(-time.Millisecond), cancel)

	key2 := mustCreateKey(t, "2", repo)
	value2, cancel := mustCreateCacheable(t, cfg, repo)
	p.Add(key2, value2, cutoff.Add(time.Millisecond), cancel)

	key3 := mustCreateKey(t, "3", repo)
	value3, cancel := mustCreateCacheable(t, cfg, repo)
	p.Add(key3, value3, cutoff.Add(time.Hour), cancel)

	requireProcessesValid(t, p)

	// We expect this cutoff to cause eviction of key0 and key1 but no other keys.
	p.EnforceTTL(cutoff)

	requireProcessesValid(t, p)

	for i, v := range []cacheable{value0, value1} {
		require.True(t, v.isClosed(), "value %d %v should be closed", i, v)
	}

	require.Equal(t, []key{key2, key3}, keys(t, p), "remaining keys after EnforceTTL")

	p.EnforceTTL(cutoff)

	requireProcessesValid(t, p)
	require.Equal(t, []key{key2, key3}, keys(t, p), "remaining keys after second EnforceTTL")
}

func TestCache_autoExpiry(t *testing.T) {
	ctx := testhelper.Context(t)

	monitorTicker := helper.NewManualTicker()

	c := newCache(time.Hour, 10, monitorTicker)
	defer c.Stop()

	cfg := testcfg.Build(t)
	repo, _ := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
		SkipCreationViaService: true,
	})

	// Add a process that has expired already.
	key0 := mustCreateKey(t, "0", repo)
	value0, cancel := mustCreateCacheable(t, cfg, repo)
	c.objectReaders.Add(key0, value0, time.Now().Add(-time.Millisecond), cancel)
	requireProcessesValid(t, &c.objectReaders)

	require.Contains(t, keys(t, &c.objectReaders), key0, "key should still be in map")
	require.False(t, value0.isClosed(), "value should not have been closed")

	// We need to tick thrice to get deterministic results: the first tick is discarded before
	// the monitor enters the loop, the second tick will be consumed and kicks off the eviction
	// but doesn't yet guarantee that the eviction has finished, and the third tick will then
	// start another eviction, which means that the previous eviction is done.
	monitorTicker.Tick()
	monitorTicker.Tick()
	monitorTicker.Tick()

	require.Empty(t, keys(t, &c.objectReaders), "key should no longer be in map")
	require.True(t, value0.isClosed(), "value should be closed after eviction")
}

func TestRoundToNearestFiveMinutes(t *testing.T) {
	testCases := []struct {
		minute           int
		expectedInterval int
	}{
		{
			minute:           2,
			expectedInterval: 5,
		},
		{
			minute:           5,
			expectedInterval: 10,
		},
		{
			minute:           6,
			expectedInterval: 10,
		},
		{
			minute:           11,
			expectedInterval: 15,
		},
		{
			minute:           22,
			expectedInterval: 25,
		},
		{
			minute:           34,
			expectedInterval: 35,
		},
		{
			minute:           49,
			expectedInterval: 50,
		},
		{
			minute:           54,
			expectedInterval: 55,
		},
		{
			minute:           58,
			expectedInterval: 60,
		},
	}

	base := time.Date(2024, 9, 25, 1, 0, 0, 0, time.UTC)
	for _, tc := range testCases {
		t.Run(fmt.Sprintf("%d minutes", tc.minute), func(t *testing.T) {
			require.Equal(t,
				tc.expectedInterval,
				roundToNearestFiveMinute(base.Add(time.Duration(tc.minute)*time.Minute)),
			)
		})
	}
}

func TestCache_ObjectReader(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg := testcfg.Build(t)

	repo, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
		SkipCreationViaService: true,
	})
	gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("main"))

	repoExecutor := newRepoExecutor(t, cfg, repo)

	cache := newCache(time.Hour, 10, helper.NewManualTicker())
	defer cache.Stop()

	t.Run("cached", func(t *testing.T) {
		defer cache.Evict()

		ctx := correlation.ContextWithCorrelation(ctx, "1")
		ctx = testhelper.MergeIncomingMetadata(ctx,
			metadata.Pairs(SessionIDField, "1"),
		)

		reader, cancel, err := cache.ObjectReader(ctx, repoExecutor)
		require.NoError(t, err)

		// Cancel the context such that the process will be considered for return to the
		// cache and wait for the cache to collect it.
		cancel()

		allKeys := keys(t, &cache.objectReaders)

		expectedSessionID := fmt.Sprintf("%d", roundToNearestFiveMinute(time.Now()))
		require.Equal(t, []key{{
			sessionID:   expectedSessionID,
			repoStorage: repo.GetStorageName(),
			repoRelPath: repo.GetRelativePath(),
		}}, allKeys)

		// Assert that we can still read from the cached process.
		_, err = reader.Object(ctx, "refs/heads/main")
		require.NoError(t, err)
	})

	t.Run("dirty process does not get cached", func(t *testing.T) {
		defer cache.Evict()

		ctx := testhelper.MergeIncomingMetadata(ctx,
			metadata.Pairs(SessionIDField, "1"),
		)

		reader, cancel, err := cache.ObjectReader(ctx, repoExecutor)
		require.NoError(t, err)

		// While we request object data, we do not consume it at all. The reader is thus
		// dirty and cannot be reused and shouldn't be returned to the cache.
		object, err := reader.Object(ctx, "refs/heads/main")
		require.NoError(t, err)

		// Cancel the process such that it will be considered for return to the cache.
		cancel()

		require.Empty(t, keys(t, &cache.objectReaders))

		// The process should be killed now, so reading the object must fail.
		_, err = io.ReadAll(object)
		require.True(t, errors.Is(err, os.ErrClosed))
	})

	t.Run("closed process does not get cached", func(t *testing.T) {
		defer cache.Evict()

		ctx := testhelper.MergeIncomingMetadata(ctx,
			metadata.Pairs(SessionIDField, "1"),
		)

		reader, cancel, err := cache.ObjectReader(ctx, repoExecutor)
		require.NoError(t, err)

		// Closed processes naturally cannot be reused anymore and thus shouldn't ever get
		// cached.
		reader.close()

		// Cancel the process such that it will be considered for return to the cache.
		cancel()

		require.Empty(t, keys(t, &cache.objectReaders))
	})
}

func TestCache_ObjectReaderWithoutMailmap(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg := testcfg.Build(t)

	repo, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
		SkipCreationViaService: true,
	})
	gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("main"))

	repoExecutor := newRepoExecutor(t, cfg, repo)

	cache := newCache(time.Hour, 10, helper.NewManualTicker())
	defer cache.Stop()

	t.Run("cached", func(t *testing.T) {
		defer cache.Evict()

		ctx := correlation.ContextWithCorrelation(ctx, "1")
		ctx = testhelper.MergeIncomingMetadata(ctx,
			metadata.Pairs(SessionIDField, "1"),
		)

		reader, cancel, err := cache.ObjectReaderWithoutMailmap(ctx, repoExecutor)
		require.NoError(t, err)

		// Cancel the context such that the process will be considered for return to the
		// cache and wait for the cache to collect it.
		cancel()

		allKeys := keys(t, &cache.objectReadersWithoutMailmap)

		expectedSessionID := fmt.Sprintf("%d", roundToNearestFiveMinute(time.Now()))

		require.Equal(t, []key{{
			sessionID:   expectedSessionID,
			repoStorage: repo.GetStorageName(),
			repoRelPath: repo.GetRelativePath(),
		}}, allKeys)

		// Assert that we can still read from the cached process.
		_, err = reader.Object(ctx, "refs/heads/main")
		require.NoError(t, err)
	})

	t.Run("dirty process does not get cached", func(t *testing.T) {
		defer cache.Evict()

		ctx := testhelper.MergeIncomingMetadata(ctx,
			metadata.Pairs(SessionIDField, "1"),
		)

		reader, cancel, err := cache.ObjectReaderWithoutMailmap(ctx, repoExecutor)
		require.NoError(t, err)

		// While we request object data, we do not consume it at all. The reader is thus
		// dirty and cannot be reused and shouldn't be returned to the cache.
		object, err := reader.Object(ctx, "refs/heads/main")
		require.NoError(t, err)

		// Cancel the process such that it will be considered for return to the cache.
		cancel()

		require.Empty(t, keys(t, &cache.objectReadersWithoutMailmap))

		// The process should be killed now, so reading the object must fail.
		_, err = io.ReadAll(object)
		require.True(t, errors.Is(err, os.ErrClosed))
	})

	t.Run("closed process does not get cached", func(t *testing.T) {
		defer cache.Evict()

		ctx := testhelper.MergeIncomingMetadata(ctx,
			metadata.Pairs(SessionIDField, "1"),
		)

		reader, cancel, err := cache.ObjectReaderWithoutMailmap(ctx, repoExecutor)
		require.NoError(t, err)

		// Closed processes naturally cannot be reused anymore and thus shouldn't ever get
		// cached.
		reader.close()

		// Cancel the process such that it will be considered for return to the cache.
		cancel()

		require.Empty(t, keys(t, &cache.objectReadersWithoutMailmap))
	})
}

func requireProcessesValid(t *testing.T, p *processes) {
	p.entriesMutex.Lock()
	defer p.entriesMutex.Unlock()

	for _, ent := range p.entries {
		v := ent.value
		require.False(t, v.isClosed(), "values in cache should not be closed: %v %v", ent, v)
	}
}

func mustCreateCacheable(t *testing.T, cfg config.Cfg, repo storage.Repository) (cacheable, func()) {
	t.Helper()

	ctx, cancel := context.WithCancel(testhelper.Context(t))

	batch, err := newObjectReader(ctx, newRepoExecutor(t, cfg, repo), nil)
	require.NoError(t, err)

	return batch, cancel
}

func mustCreateKey(t *testing.T, sessionID string, repo storage.Repository) key {
	t.Helper()

	key, cacheable := newCacheKey(sessionID, repo)
	require.True(t, cacheable)

	return key
}

func keys(t *testing.T, p *processes) []key {
	t.Helper()

	p.entriesMutex.Lock()
	defer p.entriesMutex.Unlock()

	var result []key
	for _, ent := range p.entries {
		result = append(result, ent.key)
	}

	return result
}
