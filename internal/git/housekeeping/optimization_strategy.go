package housekeeping

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"math"
	"os"
	"path/filepath"
	"strings"
	"time"

	"gitlab.com/gitlab-org/gitaly/v15/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/stats"
)

// OptimizationStrategy is an interface to determine which parts of a repository should be
// optimized.
type OptimizationStrategy interface {
	// ShouldRepackObjects determines whether the repository needs to be repacked and, if so,
	// how it should be done.
	ShouldRepackObjects() (bool, RepackObjectsConfig)
	// ShouldPruneObjects determines whether the repository has stale objects that should be
	// pruned.
	ShouldPruneObjects() bool
	// ShouldRepackReferences determines whether the repository's references need to be
	// repacked.
	ShouldRepackReferences() bool
	// ShouldWriteCommitGraph determines whether we need to write the commit-graph and how it
	// should be written.
	ShouldWriteCommitGraph() (bool, WriteCommitGraphConfig)
}

// CutOffTime is time delta that is used to indicate cutoff wherein an object would be considered
// old. Currently this is set to being 2 weeks (2 * 7days * 24hours).
const CutOffTime = -14 * 24 * time.Hour

// HeuristicalOptimizationStrategy is an optimization strategy that is based on a set of
// heuristics.
type HeuristicalOptimizationStrategy struct {
	packfileSize        uint64
	packfileCount       uint64
	looseObjectCount    uint64
	oldLooseObjectCount uint64
	looseRefsCount      int64
	packedRefsSize      int64
	hasAlternate        bool
	hasBitmap           bool
	hasBloomFilters     bool
	isObjectPool        bool
}

// NewHeuristicalOptimizationStrategy constructs a heuristicalOptimizationStrategy for the given
// repository. It derives all data from the repository so that the heuristics used by this
// repository can be decided without further disk reads.
func NewHeuristicalOptimizationStrategy(ctx context.Context, repo *localrepo.Repo) (HeuristicalOptimizationStrategy, error) {
	var strategy HeuristicalOptimizationStrategy

	repoPath, err := repo.Path()
	if err != nil {
		return strategy, fmt.Errorf("getting repository path: %w", err)
	}

	altFile, err := repo.InfoAlternatesPath()
	if err != nil {
		return strategy, fmt.Errorf("getting alternates path: %w", err)
	}

	strategy.hasAlternate = true
	if _, err := os.Stat(altFile); os.IsNotExist(err) {
		strategy.hasAlternate = false
	}

	strategy.isObjectPool = IsPoolRepository(repo)

	strategy.hasBitmap, err = stats.HasBitmap(repoPath)
	if err != nil {
		return strategy, fmt.Errorf("checking for bitmap: %w", err)
	}

	missingBloomFilters, err := stats.IsMissingBloomFilters(repoPath)
	if err != nil {
		return strategy, fmt.Errorf("checking for bloom filters: %w", err)
	}
	strategy.hasBloomFilters = !missingBloomFilters

	strategy.packfileSize, strategy.packfileCount, err = stats.PackfileSizeAndCount(repo)
	if err != nil {
		return strategy, fmt.Errorf("checking largest packfile size: %w", err)
	}

	strategy.looseObjectCount, err = countLooseObjects(repo, time.Now())
	if err != nil {
		return strategy, fmt.Errorf("estimating loose object count: %w", err)
	}

	strategy.oldLooseObjectCount, err = countLooseObjects(repo, time.Now().Add(CutOffTime))
	if err != nil {
		return strategy, fmt.Errorf("estimating old loose object count: %w", err)
	}

	strategy.looseRefsCount, strategy.packedRefsSize, err = countLooseAndPackedRefs(ctx, repo)
	if err != nil {
		return strategy, fmt.Errorf("counting refs: %w", err)
	}

	return strategy, nil
}

// ShouldRepackObjects checks whether the repository's objects need to be repacked. This uses a
// set of heuristics that scales with the size of the object database: the larger the repository,
// the less frequent does it get a full repack.
func (s HeuristicalOptimizationStrategy) ShouldRepackObjects() (bool, RepackObjectsConfig) {
	// If there are neither packfiles nor loose objects in this repository then there is no need
	// to repack anything.
	if s.packfileCount == 0 && s.looseObjectCount == 0 {
		return false, RepackObjectsConfig{}
	}

	// Bitmaps are used to efficiently determine transitive reachability of objects from a
	// set of commits. They are an essential part of the puzzle required to serve fetches
	// efficiently, as we'd otherwise need to traverse the object graph every time to find
	// which objects we have to send. We thus repack the repository with bitmaps enabled in
	// case they're missing.
	//
	// There is one exception: repositories which are connected to an object pool must not have
	// a bitmap on their own. We do not yet use multi-pack indices, and in that case Git can
	// only use one bitmap. We already generate this bitmap in the pool, so member of it
	// shouldn't have another bitmap on their own.
	if !s.hasBitmap && !s.hasAlternate {
		return true, RepackObjectsConfig{
			FullRepack:  true,
			WriteBitmap: true,
		}
	}

	// Whenever we do an incremental repack we create a new packfile, and as a result Git may
	// have to look into every one of the packfiles to find objects. This is less efficient the
	// more packfiles we have, but we cannot repack the whole repository every time either given
	// that this may take a lot of time.
	//
	// Instead, we determine whether the repository has "too many" packfiles. "Too many" is
	// relative though: for small repositories it's fine to do full repacks regularly, but for
	// large repositories we need to be more careful. We thus use a heuristic of "repository
	// largeness": we take the total size of all packfiles, and then the maximum allowed number
	// of packfiles is `log(total_packfile_size) / log(1.3)` for normal repositories and
	// `log(total_packfile_size) / log(10.0)` for pools. This gives the following allowed number
	// of packfiles:
	//
	// -----------------------------------------------------------------------------------
	// | total packfile size | allowed packfiles for repos | allowed packfiles for pools |
	// -----------------------------------------------------------------------------------
	// | none or <10MB         | 5                           | 2                         |
	// | 10MB                  | 8                           | 2                         |
	// | 100MB                 | 17                          | 2                         |
	// | 500MB                 | 23                          | 2                         |
	// | 1GB                   | 26                          | 3                         |
	// | 5GB                   | 32                          | 3                         |
	// | 10GB                  | 35                          | 4                         |
	// | 100GB                 | 43                          | 5                         |
	// -----------------------------------------------------------------------------------
	//
	// The goal is to have a comparatively quick ramp-up of allowed packfiles as the repository
	// size grows, but then slow down such that we're effectively capped and don't end up with
	// an excessive amount of packfiles. On the other hand, pool repositories are potentially
	// reused as basis for many forks and should thus be packed much more aggressively.
	//
	// This is a heuristic and thus imperfect by necessity. We may tune it as we gain experience
	// with the way it behaves.
	lowerLimit, log := 5.0, 1.3
	if s.isObjectPool {
		lowerLimit, log = 2.0, 10.0
	}

	if uint64(math.Max(lowerLimit, math.Log(float64(s.packfileSize/1024/1024))/math.Log(log))) <= s.packfileCount {
		return true, RepackObjectsConfig{
			FullRepack:  true,
			WriteBitmap: !s.hasAlternate,
		}
	}

	// Most Git commands do not write packfiles directly, but instead write loose objects into
	// the object database. So while we now know that there ain't too many packfiles, we still
	// need to check whether we have too many objects.
	//
	// In this case it doesn't make a lot of sense to scale incremental repacks with the repo's
	// size: we only pack loose objects, so the time to pack them doesn't scale with repository
	// size but with the number of loose objects we have. git-gc(1) uses a threshold of 6700
	// loose objects to start an incremental repack, but one needs to keep in mind that Git
	// typically has defaults which are better suited for the client-side instead of the
	// server-side in most commands.
	//
	// In our case we typically want to ensure that our repositories are much better packed than
	// it is necessary on the client side. We thus take a much stricter limit of 1024 objects.
	if s.looseObjectCount > looseObjectLimit {
		return true, RepackObjectsConfig{
			FullRepack:  false,
			WriteBitmap: false,
		}
	}

	return false, RepackObjectsConfig{}
}

// countLooseObjects counts the number of loose objects in the repository. If a cutoff date is
// given, then this function will only take into account objects which are older than the given
// point in time.
func countLooseObjects(repo *localrepo.Repo, cutoffDate time.Time) (uint64, error) {
	repoPath, err := repo.Path()
	if err != nil {
		return 0, fmt.Errorf("getting repository path: %w", err)
	}

	var looseObjects uint64
	for i := 0; i <= 0xFF; i++ {
		entries, err := os.ReadDir(filepath.Join(repoPath, "objects", fmt.Sprintf("%02x", i)))
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				continue
			}

			return 0, fmt.Errorf("reading loose object shard: %w", err)
		}

		for _, entry := range entries {
			if !isValidLooseObjectName(entry.Name()) {
				continue
			}

			entryInfo, err := entry.Info()
			if err != nil {
				if errors.Is(err, fs.ErrNotExist) {
					continue
				}

				return 0, fmt.Errorf("reading object info: %w", err)
			}

			if entryInfo.ModTime().After(cutoffDate) {
				continue
			}

			looseObjects++
		}
	}

	return looseObjects, nil
}

func isValidLooseObjectName(s string) bool {
	for _, c := range []byte(s) {
		if strings.IndexByte("0123456789abcdef", c) < 0 {
			return false
		}
	}
	return true
}

// ShouldWriteCommitGraph determines whether we need to write the commit-graph and how it should be
// written.
func (s HeuristicalOptimizationStrategy) ShouldWriteCommitGraph() (bool, WriteCommitGraphConfig) {
	// If the repository doesn't have any references at all then there is no point in writing
	// commit-graphs given that it would only contain reachable objects, of which there are
	// none.
	if s.looseRefsCount == 0 && s.packedRefsSize == 0 {
		return false, WriteCommitGraphConfig{}
	}

	// When we have pruned objects in the repository then it may happen that the commit-graph
	// still refers to commits that have now been deleted. While this wouldn't typically cause
	// any issues during runtime, it may cause errors when explicitly asking for any commit that
	// does exist in the commit-graph, only. Furthermore, it causes git-fsck(1) to report that
	// the commit-graph is inconsistent.
	//
	// To fix this case we will replace the complete commit-chain when we have pruned objects
	// from the repository.
	if s.ShouldPruneObjects() {
		return true, WriteCommitGraphConfig{
			ReplaceChain: true,
		}
	}

	// When we repacked the repository then chances are high that we have accumulated quite some
	// objects since the last time we wrote a commit-graph.
	if needsRepacking, _ := s.ShouldRepackObjects(); needsRepacking {
		return true, WriteCommitGraphConfig{}
	}

	// Bloom filters are part of the commit-graph and allow us to efficiently determine which
	// paths have been modified in a given commit without having to look into the object
	// database. In the past we didn't compute bloom filters at all, so we want to rewrite the
	// whole commit-graph to generate them.
	if !s.hasBloomFilters {
		return true, WriteCommitGraphConfig{
			ReplaceChain: true,
		}
	}

	return false, WriteCommitGraphConfig{}
}

// ShouldPruneObjects determines whether the repository has stale objects that should be pruned.
// Object pools are never pruned to not lose data in them, but otherwise we prune when we've found
// enough stale objects that might in fact get pruned.
func (s HeuristicalOptimizationStrategy) ShouldPruneObjects() bool {
	// Pool repositories must never prune any objects, or otherwise we may corrupt members of
	// that pool if they still refer to that object.
	if s.isObjectPool {
		return false
	}

	// When we have a number of loose objects that is older than two weeks then they have
	// surpassed the grace period and may thus be pruned.
	if s.oldLooseObjectCount <= looseObjectLimit {
		return false
	}

	return true
}

// countLooseAndPackedRefs counts the number of loose references that exist in the repository and
// returns the size of the packed-refs file.
func countLooseAndPackedRefs(ctx context.Context, repo *localrepo.Repo) (int64, int64, error) {
	repoPath, err := repo.Path()
	if err != nil {
		return 0, 0, fmt.Errorf("getting repository path: %w", err)
	}
	refsPath := filepath.Join(repoPath, "refs")

	looseRefs := int64(0)
	if err := filepath.WalkDir(refsPath, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		if !entry.IsDir() {
			looseRefs++
		}

		return nil
	}); err != nil {
		return 0, 0, fmt.Errorf("counting loose refs: %w", err)
	}

	packedRefsSize := int64(0)
	if stat, err := os.Stat(filepath.Join(repoPath, "packed-refs")); err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			return 0, 0, fmt.Errorf("getting packed-refs size: %w", err)
		}
	} else {
		packedRefsSize = stat.Size()
	}

	return looseRefs, packedRefsSize, nil
}

// ShouldRepackReferences determines whether the repository's references need to be repacked based
// on heuristics. The more references there are, the more loose referencos may exist until they are
// packed again.
func (s HeuristicalOptimizationStrategy) ShouldRepackReferences() bool {
	// If there aren't any loose refs then there is nothing we need to do.
	if s.looseRefsCount == 0 {
		return false
	}

	// Packing loose references into the packed-refs file scales with the number of references
	// we're about to write. We thus decide whether we repack refs by weighing the current size
	// of the packed-refs file against the number of loose references. This is done such that we
	// do not repack too often on repositories with a huge number of references, where we can
	// expect a lot of churn in the number of references.
	//
	// As a heuristic, we repack if the number of loose references in the repository exceeds
	// `log(packed_refs_size_in_bytes/100)/log(1.15)`, which scales as following (number of refs
	// is estimated with 100 bytes per reference):
	//
	// - 1kB ~ 10 packed refs: 16 refs
	// - 10kB ~ 100 packed refs: 33 refs
	// - 100kB ~ 1k packed refs: 49 refs
	// - 1MB ~ 10k packed refs: 66 refs
	// - 10MB ~ 100k packed refs: 82 refs
	// - 100MB ~ 1m packed refs: 99 refs
	//
	// We thus allow roughly 16 additional loose refs per factor of ten of packed refs.
	//
	// This heuristic may likely need tweaking in the future, but should serve as a good first
	// iteration.
	if int64(math.Max(16, math.Log(float64(s.packedRefsSize)/100)/math.Log(1.15))) > s.looseRefsCount {
		return false
	}

	return true
}

// EagerOptimizationStrategy is a strategy that will eagerly perform optimizations. All of the data
// structures will be optimized regardless of whether they already are in an optimal state or not.
type EagerOptimizationStrategy struct {
	hasAlternate bool
	isObjectPool bool
}

// NewEagerOptimizationStrategy creates a new EagerOptimizationStrategy.
func NewEagerOptimizationStrategy(ctx context.Context, repo *localrepo.Repo) (EagerOptimizationStrategy, error) {
	altFile, err := repo.InfoAlternatesPath()
	if err != nil {
		return EagerOptimizationStrategy{}, fmt.Errorf("getting alternates path: %w", err)
	}

	hasAlternate := true
	if _, err := os.Stat(altFile); os.IsNotExist(err) {
		hasAlternate = false
	}

	return EagerOptimizationStrategy{
		hasAlternate: hasAlternate,
		isObjectPool: IsPoolRepository(repo),
	}, nil
}

// ShouldRepackObjects always instructs the caller to repack objects. The strategy will always be to
// repack all objects into a single packfile. The bitmap will be written in case the repository does
// not have any alterantes.
func (s EagerOptimizationStrategy) ShouldRepackObjects() (bool, RepackObjectsConfig) {
	return true, RepackObjectsConfig{
		FullRepack:  true,
		WriteBitmap: !s.hasAlternate,
	}
}

// ShouldWriteCommitGraph always instructs the caller to write the commit-graph. The strategy will
// always be to completely rewrite the commit-graph chain.
func (s EagerOptimizationStrategy) ShouldWriteCommitGraph() (bool, WriteCommitGraphConfig) {
	return true, WriteCommitGraphConfig{
		ReplaceChain: true,
	}
}

// ShouldPruneObjects always instructs the caller to prune objects, unless the repository is an
// object pool.
func (s EagerOptimizationStrategy) ShouldPruneObjects() bool {
	return !s.isObjectPool
}

// ShouldRepackReferences always instructs the caller to repack references.
func (s EagerOptimizationStrategy) ShouldRepackReferences() bool {
	return true
}
