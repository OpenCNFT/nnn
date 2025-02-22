package maintenance

import (
	"context"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/catfile"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/gittest"
	housekeepingmgr "gitlab.com/gitlab-org/gitaly/v16/internal/git/housekeeping/manager"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/transaction"
	"gitlab.com/gitlab-org/gitaly/v16/internal/grpc/backchannel"
	"gitlab.com/gitlab-org/gitaly/v16/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v16/internal/log"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
)

type mockOptimizer struct {
	t      testing.TB
	actual []storage.Repository
	cfg    config.Cfg
}

func (mo *mockOptimizer) OptimizeRepository(ctx context.Context, logger log.Logger, repository storage.Repository) error {
	mo.actual = append(mo.actual, repository)
	l := config.NewLocator(mo.cfg)
	gitCmdFactory := gittest.NewCommandFactory(mo.t, mo.cfg)
	catfileCache := catfile.NewCache(mo.cfg)
	mo.t.Cleanup(catfileCache.Stop)
	txManager := transaction.NewManager(mo.cfg, logger, backchannel.NewRegistry())
	housekeepingManager := housekeepingmgr.New(mo.cfg.Prometheus, logger, txManager, nil)

	return housekeepingManager.OptimizeRepository(ctx, localrepo.New(logger, l, gitCmdFactory, catfileCache, repository))
}

func TestOptimizeReposRandomly(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfgBuilder := testcfg.NewGitalyCfgBuilder(testcfg.WithStorages("0", "1", "2"))
	cfg := cfgBuilder.Build(t)

	for _, storage := range cfg.Storages {
		gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
			SkipCreationViaService: true,
			Storage:                storage,
			RelativePath:           "a",
		})

		gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
			SkipCreationViaService: true,
			Storage:                storage,
			RelativePath:           "b",
		})
	}

	cfg.Storages = append(cfg.Storages, config.Storage{
		Name: "duplicate",
		Path: cfg.Storages[0].Path,
	})

	for _, tc := range []struct {
		desc     string
		storages []string
		expected []*gitalypb.Repository
	}{
		{
			desc:     "two storages",
			storages: []string{"0", "1"},
			expected: []*gitalypb.Repository{
				{RelativePath: "a", StorageName: "0"},
				{RelativePath: "a", StorageName: "1"},
				{RelativePath: "b", StorageName: "0"},
				{RelativePath: "b", StorageName: "1"},
			},
		},
		{
			desc:     "duplicate storages",
			storages: []string{"0", "1", "duplicate"},
			expected: []*gitalypb.Repository{
				{RelativePath: "a", StorageName: "0"},
				{RelativePath: "a", StorageName: "1"},
				{RelativePath: "b", StorageName: "0"},
				{RelativePath: "b", StorageName: "1"},
			},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			tickerDone := false
			tickerCount := 0

			ticker := helper.NewManualTicker()
			ticker.ResetFunc = func() {
				tickerCount++
				ticker.Tick()
			}
			ticker.StopFunc = func() {
				tickerDone = true
			}

			logger := testhelper.SharedLogger(t)

			mo := &mockOptimizer{
				t:   t,
				cfg: cfg,
			}
			walker := OptimizeReposRandomly(cfg, mo, ticker, rand.New(rand.NewSource(1)))

			require.NoError(t, walker(ctx, logger, tc.storages))
			require.ElementsMatch(t, tc.expected, mo.actual)
			require.True(t, tickerDone)
			// We expect one more tick than optimized repositories because of the
			// initial tick up front to re-start the timer.
			require.Equal(t, len(tc.expected)+1, tickerCount)
		})
	}
}
