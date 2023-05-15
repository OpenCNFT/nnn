package housekeeping

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/stats"
	"gitlab.com/gitlab-org/gitaly/v16/internal/metadata/featureflag"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper"
)

func TestHeuristicalOptimizationStrategy_ShouldRepackObjects(t *testing.T) {
	t.Parallel()
	testhelper.NewFeatureSets(featureflag.GeometricRepacking).Run(t, testHeuristicalOptimizationStrategyShouldRepackObjects)
}

func testHeuristicalOptimizationStrategyShouldRepackObjects(t *testing.T, ctx context.Context) {
	t.Parallel()

	for _, tc := range []struct {
		desc           string
		strategy       HeuristicalOptimizationStrategy
		expectedNeeded bool
		expectedConfig RepackObjectsConfig
	}{
		{
			desc:     "empty repo does nothing",
			strategy: HeuristicalOptimizationStrategy{},
		},
		{
			desc: "missing bitmap",
			strategy: HeuristicalOptimizationStrategy{
				info: stats.RepositoryInfo{
					Packfiles: stats.PackfilesInfo{
						Count: 1,
						Bitmap: stats.BitmapInfo{
							Exists: false,
						},
					},
				},
			},
			expectedNeeded: true,
			expectedConfig: RepackObjectsConfig{
				Strategy: geometricOrIncremental(ctx,
					RepackObjectsStrategyGeometric,
					RepackObjectsStrategyIncremental,
				),
				WriteBitmap:         true,
				WriteMultiPackIndex: true,
			},
		},
		{
			desc: "missing bitmap with alternate",
			strategy: HeuristicalOptimizationStrategy{
				info: stats.RepositoryInfo{
					Packfiles: stats.PackfilesInfo{
						Count: 1,
						Bitmap: stats.BitmapInfo{
							Exists: false,
						},
					},
					Alternates: stats.AlternatesInfo{
						ObjectDirectories: []string{"something"},
					},
				},
			},
			// If we have no bitmap in the repository we'd normally want to fully repack
			// the repository. But because we have an alternates file we know that the
			// repository must not have a bitmap anyway, so we can skip the repack here.
			//
			// This changes though with multi-pack-indices, which allow for bitmaps to
			// exist in pooled repositories.
			expectedNeeded: true,
			expectedConfig: RepackObjectsConfig{
				Strategy:            RepackObjectsStrategyIncremental,
				WriteMultiPackIndex: true,
			},
		},
		{
			desc: "no repack needed without multi-pack-index",
			strategy: HeuristicalOptimizationStrategy{
				info: stats.RepositoryInfo{
					Packfiles: stats.PackfilesInfo{
						Count: 1,
						Bitmap: stats.BitmapInfo{
							Exists: true,
						},
					},
				},
			},
			expectedNeeded: true,
			expectedConfig: RepackObjectsConfig{
				Strategy: geometricOrIncremental(ctx,
					RepackObjectsStrategyGeometric,
					RepackObjectsStrategyIncremental,
				),
				WriteBitmap:         true,
				WriteMultiPackIndex: true,
			},
		},
		{
			desc: "no repack needed with multi-pack-index",
			strategy: HeuristicalOptimizationStrategy{
				info: stats.RepositoryInfo{
					Packfiles: stats.PackfilesInfo{
						Count: 1,
						MultiPackIndex: stats.MultiPackIndexInfo{
							Exists: true,
						},
					},
				},
			},
			expectedNeeded: false,
			expectedConfig: RepackObjectsConfig{},
		},
		{
			desc: "recently packed with tracked packfiles will not be repacked again",
			strategy: HeuristicalOptimizationStrategy{
				info: stats.RepositoryInfo{
					Packfiles: stats.PackfilesInfo{
						// We have multiple tracked packfiles, but did not
						// yet cross the 24 hour boundary. So we don't
						// expect a repack.
						Count:          2,
						LastFullRepack: time.Now().Add(-FullRepackCooldownPeriod + time.Hour),
						MultiPackIndex: stats.MultiPackIndexInfo{
							Exists:        true,
							PackfileCount: 2,
						},
					},
				},
			},
			expectedNeeded: false,
			expectedConfig: RepackObjectsConfig{},
		},
		{
			desc: "old tracked packfiles will be repacked",
			strategy: HeuristicalOptimizationStrategy{
				info: stats.RepositoryInfo{
					Packfiles: stats.PackfilesInfo{
						// We have multiple tracked packfiles and have
						// crossed the 24 hour boundary, so we should
						// perform a full repack.
						Count:          2,
						LastFullRepack: time.Now().Add(-FullRepackCooldownPeriod),
						MultiPackIndex: stats.MultiPackIndexInfo{
							Exists:        true,
							PackfileCount: 2,
						},
					},
				},
			},
			expectedNeeded: geometricOrIncremental(ctx, true, false),
			expectedConfig: geometricOrIncremental(ctx,
				RepackObjectsConfig{
					Strategy:            RepackObjectsStrategyFullWithCruft,
					WriteBitmap:         true,
					WriteMultiPackIndex: true,
				},
				RepackObjectsConfig{},
			),
		},
		{
			desc: "old tracked packfiles with cruft pack will not be repacked",
			strategy: HeuristicalOptimizationStrategy{
				info: stats.RepositoryInfo{
					Packfiles: stats.PackfilesInfo{
						// We have multiple tracked packfiles and have
						// crossed the 24 hour boundary, so we should
						// perform a full repack.
						Count:          2,
						CruftCount:     1,
						LastFullRepack: time.Now().Add(-FullRepackCooldownPeriod),
						MultiPackIndex: stats.MultiPackIndexInfo{
							Exists:        true,
							PackfileCount: 2,
						},
					},
				},
			},
			expectedNeeded: false,
			expectedConfig: RepackObjectsConfig{},
		},
		{
			desc: "recent tracked packfiles in pool repository will be repacked",
			strategy: HeuristicalOptimizationStrategy{
				info: stats.RepositoryInfo{
					IsObjectPool: true,
					Packfiles: stats.PackfilesInfo{
						// Pool repositories follow the same schema as
						// normal repositories, but have a longer grace
						// period for the next repack.
						Count:          2,
						LastFullRepack: time.Now().Add(-FullRepackCooldownPeriod + time.Hour),
						MultiPackIndex: stats.MultiPackIndexInfo{
							Exists:        true,
							PackfileCount: 2,
						},
					},
				},
			},
			expectedNeeded: geometricOrIncremental(ctx, false, true),
			expectedConfig: geometricOrIncremental(ctx,
				RepackObjectsConfig{},
				RepackObjectsConfig{
					Strategy:            RepackObjectsStrategyFullWithLooseUnreachable,
					WriteBitmap:         true,
					WriteMultiPackIndex: true,
				},
			),
		},
		{
			desc: "old tracked packfiles in pool repository will be repacked",
			strategy: HeuristicalOptimizationStrategy{
				info: stats.RepositoryInfo{
					IsObjectPool: true,
					Packfiles: stats.PackfilesInfo{
						// Once we have crossed the grace period, pool
						// repositories should get a full repack in case
						// they have more than a single packfile.
						Count:          2,
						LastFullRepack: time.Now().Add(-FullRepackCooldownPeriod),
						MultiPackIndex: stats.MultiPackIndexInfo{
							Exists:        true,
							PackfileCount: 2,
						},
					},
				},
			},
			expectedNeeded: true,
			expectedConfig: geometricOrIncremental(ctx,
				RepackObjectsConfig{
					Strategy:            RepackObjectsStrategyFullWithUnreachable,
					WriteBitmap:         true,
					WriteMultiPackIndex: true,
				},
				RepackObjectsConfig{
					Strategy:            RepackObjectsStrategyFullWithLooseUnreachable,
					WriteBitmap:         true,
					WriteMultiPackIndex: true,
				},
			),
		},
		{
			desc: "few untracked packfiles will not get repacked",
			strategy: HeuristicalOptimizationStrategy{
				info: stats.RepositoryInfo{
					Packfiles: stats.PackfilesInfo{
						// We have 10 packfiles, of which 8 are tracked via
						// the multi-pack-index. This is "good enough", so
						// we don't expect a repack.
						Count:          10,
						Size:           10 * 1024 * 1024,
						LastFullRepack: time.Now(),
						MultiPackIndex: stats.MultiPackIndexInfo{
							Exists:        true,
							PackfileCount: 8,
						},
					},
				},
			},
			expectedNeeded: geometricOrIncremental(ctx, false, true),
			expectedConfig: geometricOrIncremental(ctx,
				RepackObjectsConfig{},
				RepackObjectsConfig{
					Strategy:            RepackObjectsStrategyFullWithCruft,
					WriteBitmap:         true,
					WriteMultiPackIndex: true,
				},
			),
		},
		{
			desc: "many untracked packfiles will get repacked",
			strategy: HeuristicalOptimizationStrategy{
				info: stats.RepositoryInfo{
					Packfiles: stats.PackfilesInfo{
						// Once we have more than a certain number of
						// untracked packfiles we want to see a repack
						// though.
						Count:          10,
						Size:           10 * 1024 * 1024,
						LastFullRepack: time.Now(),
						MultiPackIndex: stats.MultiPackIndexInfo{
							Exists:        true,
							PackfileCount: 5,
						},
					},
				},
			},
			expectedNeeded: true,
			expectedConfig: geometricOrIncremental(ctx,
				RepackObjectsConfig{
					Strategy:            RepackObjectsStrategyGeometric,
					WriteBitmap:         true,
					WriteMultiPackIndex: true,
				},
				RepackObjectsConfig{
					Strategy:            RepackObjectsStrategyFullWithCruft,
					WriteBitmap:         true,
					WriteMultiPackIndex: true,
				},
			),
		},
		{
			desc: "larger packfiles allow more untracked packfiles",
			strategy: HeuristicalOptimizationStrategy{
				info: stats.RepositoryInfo{
					Packfiles: stats.PackfilesInfo{
						// The number of allowed untracked packfiles scales
						// with the size of the repository.
						Count:          20,
						Size:           1000 * 1024 * 1024,
						LastFullRepack: time.Now(),
						MultiPackIndex: stats.MultiPackIndexInfo{
							Exists:        true,
							PackfileCount: 9,
						},
					},
				},
			},
			expectedNeeded: false,
			expectedConfig: RepackObjectsConfig{},
		},
		{
			desc: "larger packfiles with many untracked packfiles eventually repack",
			strategy: HeuristicalOptimizationStrategy{
				info: stats.RepositoryInfo{
					Packfiles: stats.PackfilesInfo{
						// For large repositories, the threshold of
						// untracked packfiles will eventually be reached.
						Count:          20,
						Size:           1000 * 1024 * 1024,
						LastFullRepack: time.Now(),
						MultiPackIndex: stats.MultiPackIndexInfo{
							Exists:        true,
							PackfileCount: 8,
						},
					},
				},
			},
			expectedNeeded: geometricOrIncremental(ctx, true, false),
			expectedConfig: geometricOrIncremental(ctx,
				RepackObjectsConfig{
					Strategy:            RepackObjectsStrategyGeometric,
					WriteBitmap:         true,
					WriteMultiPackIndex: true,
				},
				RepackObjectsConfig{},
			),
		},
		{
			desc: "more tracked packfiles than exist will repack to update MIDX",
			strategy: HeuristicalOptimizationStrategy{
				info: stats.RepositoryInfo{
					Packfiles: stats.PackfilesInfo{
						// We have a single packfile, but 8 tracked
						// packfiles in the multi-pack-index. We shouldn't
						// ever get here, but it's nice to verify we don't
						// misbehave. Repacking is the best thing we can do
						// to fix the MIDX.
						Count:          1,
						Size:           10 * 1024,
						LastFullRepack: time.Now(),
						MultiPackIndex: stats.MultiPackIndexInfo{
							Exists:        true,
							PackfileCount: 8,
						},
					},
				},
			},
			expectedNeeded: geometricOrIncremental(ctx, true, false),
			expectedConfig: geometricOrIncremental(ctx,
				RepackObjectsConfig{
					Strategy:            RepackObjectsStrategyGeometric,
					WriteBitmap:         true,
					WriteMultiPackIndex: true,
				},
				RepackObjectsConfig{},
			),
		},
		{
			desc: "no geometric repack in object pool member with old Git version",
			strategy: HeuristicalOptimizationStrategy{
				gitVersion: git.NewVersion(2, 39, 0, 0),
				info: stats.RepositoryInfo{
					Packfiles: stats.PackfilesInfo{
						Count:          9,
						LastFullRepack: time.Now(),
						MultiPackIndex: stats.MultiPackIndexInfo{
							Exists:        true,
							PackfileCount: 1,
						},
					},
					Alternates: stats.AlternatesInfo{
						ObjectDirectories: []string{"object-pool"},
					},
				},
			},
			expectedNeeded: true,
			expectedConfig: RepackObjectsConfig{
				Strategy:            RepackObjectsStrategyFullWithCruft,
				WriteBitmap:         false,
				WriteMultiPackIndex: true,
			},
		},
		{
			desc: "geometric repack in object pool member with recent Git version",
			strategy: HeuristicalOptimizationStrategy{
				gitVersion: git.NewVersion(2, 40, 0, 1),
				info: stats.RepositoryInfo{
					Packfiles: stats.PackfilesInfo{
						Count:          9,
						LastFullRepack: time.Now(),
						MultiPackIndex: stats.MultiPackIndexInfo{
							Exists:        true,
							PackfileCount: 1,
						},
					},
					Alternates: stats.AlternatesInfo{
						ObjectDirectories: []string{"object-pool"},
					},
				},
			},
			expectedNeeded: true,
			expectedConfig: geometricOrIncremental(ctx,
				RepackObjectsConfig{
					Strategy:            RepackObjectsStrategyGeometric,
					WriteBitmap:         false,
					WriteMultiPackIndex: true,
				},
				RepackObjectsConfig{
					Strategy:            RepackObjectsStrategyFullWithCruft,
					WriteBitmap:         false,
					WriteMultiPackIndex: true,
				},
			),
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			repackNeeded, repackCfg := tc.strategy.ShouldRepackObjects(ctx)
			require.Equal(t, tc.expectedNeeded, repackNeeded)
			require.Equal(t, tc.expectedConfig, repackCfg)
		})
	}

	for _, outerTC := range []struct {
		packfileSizeInMB         uint64
		requiredPackfiles        uint64
		requiredPackfilesForPool uint64
	}{
		{
			packfileSizeInMB:         1,
			requiredPackfiles:        5,
			requiredPackfilesForPool: 2,
		},
		{
			packfileSizeInMB:         5,
			requiredPackfiles:        6,
			requiredPackfilesForPool: 2,
		},
		{
			packfileSizeInMB:         10,
			requiredPackfiles:        8,
			requiredPackfilesForPool: 2,
		},
		{
			packfileSizeInMB:         50,
			requiredPackfiles:        14,
			requiredPackfilesForPool: 2,
		},
		{
			packfileSizeInMB:         100,
			requiredPackfiles:        17,
			requiredPackfilesForPool: 2,
		},
		{
			packfileSizeInMB:         500,
			requiredPackfiles:        23,
			requiredPackfilesForPool: 2,
		},
		{
			packfileSizeInMB:         1001,
			requiredPackfiles:        26,
			requiredPackfilesForPool: 3,
		},
	} {
		// These tests don't really make any sense in the context of geometric repacking as
		// we don't care for the size of packfiles anymore. The test can be deleted once the
		// old strategy has been removed.
		if featureflag.GeometricRepacking.IsEnabled(ctx) {
			break
		}

		t.Run(fmt.Sprintf("packfile with %dMB", outerTC.packfileSizeInMB), func(t *testing.T) {
			for _, tc := range []struct {
				desc              string
				isPool            bool
				alternates        []string
				requiredPackfiles uint64
			}{
				{
					desc:              "normal repository",
					isPool:            false,
					requiredPackfiles: outerTC.requiredPackfiles,
				},
				{
					desc:              "pooled repository",
					isPool:            false,
					alternates:        []string{"something"},
					requiredPackfiles: outerTC.requiredPackfiles,
				},
				{
					desc:              "object pool",
					isPool:            true,
					requiredPackfiles: outerTC.requiredPackfilesForPool,
				},
			} {
				t.Run(tc.desc, func(t *testing.T) {
					expireBefore := time.Now()
					strategy := HeuristicalOptimizationStrategy{
						info: stats.RepositoryInfo{
							IsObjectPool: tc.isPool,
							Packfiles: stats.PackfilesInfo{
								Size:  outerTC.packfileSizeInMB * 1024 * 1024,
								Count: tc.requiredPackfiles - 1,
								Bitmap: stats.BitmapInfo{
									Exists: true,
								},
								MultiPackIndex: stats.MultiPackIndexInfo{
									Exists: true,
								},
							},
							Alternates: stats.AlternatesInfo{
								ObjectDirectories: tc.alternates,
							},
						},
						expireBefore: expireBefore,
					}

					if tc.isPool {
						expireBefore = time.Time{}
					}

					repackNeeded, _ := strategy.ShouldRepackObjects(ctx)
					require.False(t, repackNeeded)

					// Now we add the last packfile that should bring us across
					// the boundary of having to repack.
					strategy.info.Packfiles.Count++

					repackNeeded, repackCfg := strategy.ShouldRepackObjects(ctx)
					require.True(t, repackNeeded)
					require.Equal(t, RepackObjectsConfig{
						Strategy: func() RepackObjectsStrategy {
							if !tc.isPool {
								return RepackObjectsStrategyFullWithCruft
							}
							return RepackObjectsStrategyFullWithLooseUnreachable
						}(),
						WriteBitmap:         len(tc.alternates) == 0,
						WriteMultiPackIndex: true,
						CruftExpireBefore:   expireBefore,
					}, repackCfg)
				})
			}
		})
	}

	for _, outerTC := range []struct {
		desc           string
		looseObjects   uint64
		expectedRepack bool
	}{
		{
			desc:           "no objects",
			looseObjects:   0,
			expectedRepack: false,
		},
		{
			desc:           "single object",
			looseObjects:   1,
			expectedRepack: false,
		},
		{
			desc:           "boundary",
			looseObjects:   1024,
			expectedRepack: false,
		},
		{
			desc:           "exceeding boundary should cause repack",
			looseObjects:   1025,
			expectedRepack: true,
		},
	} {
		for _, tc := range []struct {
			desc   string
			isPool bool
		}{
			{
				desc:   "normal repository",
				isPool: false,
			},
			{
				desc:   "object pool",
				isPool: true,
			},
		} {
			t.Run(tc.desc, func(t *testing.T) {
				strategy := HeuristicalOptimizationStrategy{
					info: stats.RepositoryInfo{
						IsObjectPool: tc.isPool,
						LooseObjects: stats.LooseObjectsInfo{
							Count: outerTC.looseObjects,
						},
						Packfiles: stats.PackfilesInfo{
							// We need to pretend that we have a bitmap,
							// otherwise we aways do a full repack.
							Bitmap: stats.BitmapInfo{
								Exists: true,
							},
							MultiPackIndex: stats.MultiPackIndexInfo{
								Exists: true,
							},
						},
					},
				}

				repackNeeded, repackCfg := strategy.ShouldRepackObjects(ctx)
				require.Equal(t, outerTC.expectedRepack, repackNeeded)
				require.Equal(t, RepackObjectsConfig{
					Strategy: func() RepackObjectsStrategy {
						if repackNeeded {
							return geometricOrIncremental(ctx,
								RepackObjectsStrategyIncrementalWithUnreachable,
								RepackObjectsStrategyIncremental,
							)
						}
						return ""
					}(),
					WriteBitmap:         repackNeeded && geometricOrIncremental(ctx, false, true),
					WriteMultiPackIndex: repackNeeded && geometricOrIncremental(ctx, false, true),
				}, repackCfg)
			})
		}
	}
}

func TestHeuristicalOptimizationStrategy_ShouldPruneObjects(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	expireBefore := time.Now()

	for _, tc := range []struct {
		desc                       string
		strategy                   HeuristicalOptimizationStrategy
		expectedShouldPruneObjects bool
		expectedPruneObjectsConfig PruneObjectsConfig
	}{
		{
			desc: "empty repository",
			strategy: HeuristicalOptimizationStrategy{
				expireBefore: expireBefore,
			},
			expectedShouldPruneObjects: false,
		},
		{
			desc: "only recent object",
			strategy: HeuristicalOptimizationStrategy{
				info: stats.RepositoryInfo{
					LooseObjects: stats.LooseObjectsInfo{
						Count: 10000,
					},
				},
				expireBefore: expireBefore,
			},
			expectedShouldPruneObjects: false,
		},
		{
			desc: "few stale objects",
			strategy: HeuristicalOptimizationStrategy{
				info: stats.RepositoryInfo{
					LooseObjects: stats.LooseObjectsInfo{
						StaleCount: 1000,
					},
				},
				expireBefore: expireBefore,
			},
			expectedShouldPruneObjects: false,
		},
		{
			desc: "too many stale objects",
			strategy: HeuristicalOptimizationStrategy{
				info: stats.RepositoryInfo{
					LooseObjects: stats.LooseObjectsInfo{
						StaleCount: 1025,
					},
				},
				expireBefore: expireBefore,
			},
			expectedShouldPruneObjects: true,
			expectedPruneObjectsConfig: PruneObjectsConfig{
				ExpireBefore: expireBefore,
			},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			t.Run("normal repository", func(t *testing.T) {
				shouldPrune, pruneCfg := tc.strategy.ShouldPruneObjects(ctx)
				require.Equal(t, tc.expectedShouldPruneObjects, shouldPrune)
				require.Equal(t, tc.expectedPruneObjectsConfig, pruneCfg)
			})

			t.Run("object pool", func(t *testing.T) {
				strategy := tc.strategy
				strategy.info.IsObjectPool = true

				shouldPrune, pruneCfg := tc.strategy.ShouldPruneObjects(ctx)
				require.Equal(t, tc.expectedShouldPruneObjects, shouldPrune)
				require.Equal(t, tc.expectedPruneObjectsConfig, pruneCfg)
			})
		})
	}
}

func TestHeuristicalOptimizationStrategy_ShouldRepackReferences(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)

	const kiloByte = 1024

	for _, tc := range []struct {
		packedRefsSize uint64
		requiredRefs   uint64
	}{
		{
			packedRefsSize: 1,
			requiredRefs:   16,
		},
		{
			packedRefsSize: 1 * kiloByte,
			requiredRefs:   16,
		},
		{
			packedRefsSize: 10 * kiloByte,
			requiredRefs:   33,
		},
		{
			packedRefsSize: 100 * kiloByte,
			requiredRefs:   49,
		},
		{
			packedRefsSize: 1000 * kiloByte,
			requiredRefs:   66,
		},
		{
			packedRefsSize: 10000 * kiloByte,
			requiredRefs:   82,
		},
		{
			packedRefsSize: 100000 * kiloByte,
			requiredRefs:   99,
		},
	} {
		t.Run("packed-refs with %d bytes", func(t *testing.T) {
			strategy := HeuristicalOptimizationStrategy{
				info: stats.RepositoryInfo{
					References: stats.ReferencesInfo{
						PackedReferencesSize: tc.packedRefsSize,
						LooseReferencesCount: tc.requiredRefs - 1,
					},
				},
			}

			require.False(t, strategy.ShouldRepackReferences(ctx))

			strategy.info.References.LooseReferencesCount++

			require.True(t, strategy.ShouldRepackReferences(ctx))
		})
	}
}

func TestHeuristicalOptimizationStrategy_NeedsWriteCommitGraph(t *testing.T) {
	t.Parallel()

	testhelper.NewFeatureSets(featureflag.GeometricRepacking).Run(t, testHeuristicalOptimizationStrategyNeedsWriteCommitGraph)
}

func testHeuristicalOptimizationStrategyNeedsWriteCommitGraph(t *testing.T, ctx context.Context) {
	t.Parallel()

	for _, tc := range []struct {
		desc           string
		strategy       HeuristicalOptimizationStrategy
		expectedNeeded bool
		expectedCfg    WriteCommitGraphConfig
	}{
		{
			desc:           "empty repository",
			expectedNeeded: false,
		},
		{
			desc: "repository with objects but no refs",
			strategy: HeuristicalOptimizationStrategy{
				info: stats.RepositoryInfo{
					LooseObjects: stats.LooseObjectsInfo{
						Count: 9000,
					},
				},
			},
			expectedNeeded: false,
		},
		{
			desc: "repository without bloom filters",
			strategy: HeuristicalOptimizationStrategy{
				info: stats.RepositoryInfo{
					References: stats.ReferencesInfo{
						LooseReferencesCount: 1,
					},
				},
			},
			expectedNeeded: true,
			expectedCfg: WriteCommitGraphConfig{
				ReplaceChain: true,
			},
		},
		{
			desc: "repository without bloom filters with repack",
			strategy: HeuristicalOptimizationStrategy{
				info: stats.RepositoryInfo{
					References: stats.ReferencesInfo{
						LooseReferencesCount: 1,
					},
					LooseObjects: stats.LooseObjectsInfo{
						Count: 9000,
					},
				},
			},
			// When we have a valid commit-graph, but objects have been repacked, we
			// assume that there are new objects in the repository. So consequentially,
			// we should write the commit-graphs.
			expectedNeeded: true,
			expectedCfg: WriteCommitGraphConfig{
				ReplaceChain: true,
			},
		},
		{
			desc: "repository with split commit-graph with bitmap without repack",
			strategy: HeuristicalOptimizationStrategy{
				info: stats.RepositoryInfo{
					References: stats.ReferencesInfo{
						LooseReferencesCount: 1,
					},
					CommitGraph: stats.CommitGraphInfo{
						CommitGraphChainLength: 1,
						HasBloomFilters:        true,
					},
				},
			},
			// If we have no generation data then we want to rewrite the commit-graph,
			// but only if the feature flag is enabled.
			expectedNeeded: true,
			expectedCfg: WriteCommitGraphConfig{
				ReplaceChain: true,
			},
		},
		{
			desc: "repository with split commit-graph and generation data with bitmap without repack",
			strategy: HeuristicalOptimizationStrategy{
				info: stats.RepositoryInfo{
					References: stats.ReferencesInfo{
						LooseReferencesCount: 1,
					},
					CommitGraph: stats.CommitGraphInfo{
						CommitGraphChainLength: 1,
						HasBloomFilters:        true,
						HasGenerationData:      true,
					},
				},
			},
			// We use the information about whether we repacked objects as an indicator
			// whether something has changed in the repository. If it didn't, then we
			// assume no new objects exist and thus we don't rewrite the commit-graph.
			expectedNeeded: false,
		},
		{
			desc: "repository with monolithic commit-graph with bloom filters with repack",
			strategy: HeuristicalOptimizationStrategy{
				info: stats.RepositoryInfo{
					LooseObjects: stats.LooseObjectsInfo{
						Count: 9000,
					},
					References: stats.ReferencesInfo{
						LooseReferencesCount: 1,
					},
					CommitGraph: stats.CommitGraphInfo{
						HasBloomFilters: true,
					},
				},
			},
			// When we have a valid commit-graph, but objects have been repacked, we
			// assume that there are new objects in the repository. So consequentially,
			// we should write the commit-graphs.
			expectedNeeded: true,
			expectedCfg: WriteCommitGraphConfig{
				ReplaceChain: true,
			},
		},
		{
			desc: "repository with monolithic commit-graph with bloom filters with pruned objects",
			strategy: HeuristicalOptimizationStrategy{
				info: stats.RepositoryInfo{
					LooseObjects: stats.LooseObjectsInfo{
						StaleCount: 9000,
					},
					References: stats.ReferencesInfo{
						LooseReferencesCount: 1,
					},
					CommitGraph: stats.CommitGraphInfo{
						HasBloomFilters: true,
					},
				},
			},
			// When we have a valid commit-graph, but objects have been repacked, we
			// assume that there are new objects in the repository. So consequentially,
			// we should write the commit-graphs.
			expectedNeeded: true,
			expectedCfg: WriteCommitGraphConfig{
				ReplaceChain: true,
			},
		},
		{
			desc: "writing cruft packs with expiry rewrites commit graph chain",
			strategy: HeuristicalOptimizationStrategy{
				info: stats.RepositoryInfo{
					Packfiles: stats.PackfilesInfo{
						Count: 9000,
					},
					References: stats.ReferencesInfo{
						LooseReferencesCount: 1,
					},
					CommitGraph: stats.CommitGraphInfo{
						CommitGraphChainLength: 1,
						HasBloomFilters:        true,
						HasGenerationData:      true,
					},
				},
				expireBefore: time.Now(),
			},
			// When we have a valid commit-graph, but objects are expired via cruft
			// packs, then some objects may be deleted and thus cause us to end up with
			// a stale commit-graph. We thus need to replace the whole chain.
			expectedNeeded: true,
			expectedCfg: WriteCommitGraphConfig{
				ReplaceChain: true,
			},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			needed, writeCommitGraphCfg := tc.strategy.ShouldWriteCommitGraph(ctx)
			require.Equal(t, tc.expectedNeeded, needed)
			require.Equal(t, tc.expectedCfg, writeCommitGraphCfg)
		})
	}
}

func TestEagerOptimizationStrategy(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	expireBefore := time.Now()

	for _, tc := range []struct {
		desc                     string
		strategy                 EagerOptimizationStrategy
		expectWriteBitmap        bool
		expectShouldPruneObjects bool
		pruneObjectsCfg          PruneObjectsConfig
	}{
		{
			desc: "no alternate",
			strategy: EagerOptimizationStrategy{
				expireBefore: expireBefore,
			},
			expectWriteBitmap:        true,
			expectShouldPruneObjects: true,
			pruneObjectsCfg: PruneObjectsConfig{
				ExpireBefore: expireBefore,
			},
		},
		{
			desc: "alternate",
			strategy: EagerOptimizationStrategy{
				info: stats.RepositoryInfo{
					Alternates: stats.AlternatesInfo{
						ObjectDirectories: []string{"path/to/alternate"},
					},
				},
				expireBefore: expireBefore,
			},
			expectWriteBitmap:        false,
			expectShouldPruneObjects: true,
			pruneObjectsCfg: PruneObjectsConfig{
				ExpireBefore: expireBefore,
			},
		},
		{
			desc: "object pool",
			strategy: EagerOptimizationStrategy{
				info: stats.RepositoryInfo{
					IsObjectPool: true,
				},
				expireBefore: expireBefore,
			},
			expectWriteBitmap:        true,
			expectShouldPruneObjects: false,
		},
		{
			desc: "object pool with alternate",
			strategy: EagerOptimizationStrategy{
				info: stats.RepositoryInfo{
					IsObjectPool: true,
					Alternates: stats.AlternatesInfo{
						ObjectDirectories: []string{"path/to/alternate"},
					},
				},
				expireBefore: expireBefore,
			},
			expectWriteBitmap:        false,
			expectShouldPruneObjects: false,
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			var expectedExpireBefore time.Time
			if !tc.strategy.info.IsObjectPool {
				expectedExpireBefore = expireBefore
			}

			shouldRepackObjects, repackObjectsCfg := tc.strategy.ShouldRepackObjects(ctx)
			require.True(t, shouldRepackObjects)
			require.Equal(t, RepackObjectsConfig{
				Strategy: func() RepackObjectsStrategy {
					if !tc.strategy.info.IsObjectPool {
						return RepackObjectsStrategyFullWithCruft
					}
					return RepackObjectsStrategyFullWithUnreachable
				}(),
				WriteBitmap:         tc.expectWriteBitmap,
				WriteMultiPackIndex: true,
				CruftExpireBefore:   expectedExpireBefore,
			}, repackObjectsCfg)

			shouldWriteCommitGraph, writeCommitGraphCfg := tc.strategy.ShouldWriteCommitGraph(ctx)
			require.True(t, shouldWriteCommitGraph)
			require.Equal(t, WriteCommitGraphConfig{
				ReplaceChain: true,
			}, writeCommitGraphCfg)

			shouldPruneObjects, pruneObjectsCfg := tc.strategy.ShouldPruneObjects(ctx)
			require.Equal(t, tc.expectShouldPruneObjects, shouldPruneObjects)
			require.Equal(t, tc.pruneObjectsCfg, pruneObjectsCfg)

			require.True(t, tc.strategy.ShouldRepackReferences(ctx))
		})
	}
}

// mockOptimizationStrategy is a mock strategy that can be used with OptimizeRepository.
type mockOptimizationStrategy struct {
	shouldRepackObjects    bool
	repackObjectsCfg       RepackObjectsConfig
	shouldPruneObjects     bool
	pruneObjectsCfg        PruneObjectsConfig
	shouldRepackReferences func(ctx context.Context) bool
	shouldWriteCommitGraph bool
	writeCommitGraphCfg    WriteCommitGraphConfig
}

func (m mockOptimizationStrategy) ShouldRepackObjects(context.Context) (bool, RepackObjectsConfig) {
	return m.shouldRepackObjects, m.repackObjectsCfg
}

func (m mockOptimizationStrategy) ShouldPruneObjects(context.Context) (bool, PruneObjectsConfig) {
	return m.shouldPruneObjects, m.pruneObjectsCfg
}

func (m mockOptimizationStrategy) ShouldRepackReferences(ctx context.Context) bool {
	return m.shouldRepackReferences(ctx)
}

func (m mockOptimizationStrategy) ShouldWriteCommitGraph(context.Context) (bool, WriteCommitGraphConfig) {
	return m.shouldWriteCommitGraph, m.writeCommitGraphCfg
}
