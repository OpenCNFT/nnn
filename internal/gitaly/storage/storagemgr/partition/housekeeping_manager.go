package partition

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"runtime/trace"
	"slices"
	"strings"
	"syscall"
	"time"

	"gitlab.com/gitlab-org/gitaly/v16/internal/git"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/gitcmd"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/housekeeping"
	housekeepingcfg "gitlab.com/gitlab-org/gitaly/v16/internal/git/housekeeping/config"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/stats"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/mode"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/wal"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/wal/reftree"
	"gitlab.com/gitlab-org/gitaly/v16/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v16/internal/tracing"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
)

// runHousekeeping models housekeeping tasks. It is supposed to handle housekeeping tasks for repositories
// such as the cleanup of unneeded files and optimizations for the repository's data structures.
type runHousekeeping struct {
	packRefs          *runPackRefs
	repack            *runRepack
	writeCommitGraphs *writeCommitGraphs
}

// runPackRefs models refs packing housekeeping task. It packs heads and tags for efficient repository access.
type runPackRefs struct {
	// PrunedRefs contain a list of references pruned by the `git-pack-refs` command. They are used
	// for comparing to the ref list of the destination repository
	PrunedRefs map[git.ReferenceName]struct{}
	// reftablesBefore contains the data in 'tables.list' before the compaction. This is used to
	// compare with the destination repositories 'tables.list'.
	reftablesBefore []string
	// reftablesAfter contains the data in 'tables.list' after the compaction. This is used for
	// generating the combined 'tables.list' during verification.
	reftablesAfter []string
}

// runRepack models repack housekeeping task. We support multiple repacking strategies. At this stage, the outside
// scheduler determines which strategy to use. The transaction manager is responsible for executing it. In the future,
// we want to make housekeeping smarter by migrating housekeeping scheduling responsibility to this manager. That work
// is tracked in https://gitlab.com/gitlab-org/gitaly/-/issues/5709.
type runRepack struct {
	// config tells which strategy and baggaged options.
	config housekeepingcfg.RepackObjectsConfig
}

// writeCommitGraphs models a commit graph update.
type writeCommitGraphs struct {
	// config includes the configs for writing commit graph.
	config housekeepingcfg.WriteCommitGraphConfig
}

// prepareHousekeeping composes and prepares necessary steps on the staging repository before the changes are staged and
// applied. All commands run in the scope of the staging repository. Thus, we can avoid any impact on other concurrent
// transactions.
func (mgr *TransactionManager) prepareHousekeeping(ctx context.Context, transaction *Transaction) error {
	if transaction.runHousekeeping == nil {
		return nil
	}

	span, ctx := tracing.StartSpanIfHasParent(ctx, "transaction.prepareHousekeeping", nil)
	defer span.Finish()

	finishTimer := mgr.metrics.housekeeping.ReportTaskLatency("total", "prepare")
	defer finishTimer()

	if err := mgr.preparePackRefs(ctx, transaction); err != nil {
		return err
	}
	if err := mgr.prepareRepacking(ctx, transaction); err != nil {
		return err
	}
	if err := mgr.prepareCommitGraphs(ctx, transaction); err != nil {
		return err
	}
	return nil
}

// preparePackRefs runs pack refs on the repository after detecting
// its reference backend type.
func (mgr *TransactionManager) preparePackRefs(ctx context.Context, transaction *Transaction) error {
	defer trace.StartRegion(ctx, "preparePackRefs").End()

	if transaction.runHousekeeping.packRefs == nil {
		return nil
	}

	span, ctx := tracing.StartSpanIfHasParent(ctx, "transaction.preparePackRefs", nil)
	defer span.Finish()

	finishTimer := mgr.metrics.housekeeping.ReportTaskLatency("pack-refs", "prepare")
	defer finishTimer()

	refBackend, err := transaction.snapshotRepository.ReferenceBackend(ctx)
	if err != nil {
		return fmt.Errorf("reference backend: %w", err)
	}

	if refBackend == git.ReferenceBackendReftables {
		if err = mgr.preparePackRefsReftable(ctx, transaction); err != nil {
			return fmt.Errorf("reftable backend: %w", err)
		}
		return nil
	}

	if err = mgr.preparePackRefsFiles(ctx, transaction); err != nil {
		return fmt.Errorf("files backend: %w", err)
	}
	return nil
}

// preparePackRefsReftable is used to prepare compaction for reftables.
//
// The flow here is to find the delta of tables modified post compactions. We note the
// list of tables which were deleted and which were added. In the verification stage,
// we use this information to finally create the modified tables.list. Which is also
// why we don't track 'tables.list' operation here.
func (mgr *TransactionManager) preparePackRefsReftable(ctx context.Context, transaction *Transaction) error {
	runPackRefs := transaction.runHousekeeping.packRefs
	repoPath := mgr.getAbsolutePath(transaction.snapshotRepository.GetRelativePath())

	tablesListPre, err := git.ReadReftablesList(repoPath)
	if err != nil {
		return fmt.Errorf("reading tables.list pre-compaction: %w", err)
	}

	// Execute git-pack-refs command. The command runs in the scope of the snapshot repository. Thus, we can
	// let it prune the ref references without causing any impact to other concurrent transactions.
	var stderr bytes.Buffer
	if err := transaction.snapshotRepository.ExecAndWait(ctx, gitcmd.Command{
		Name: "pack-refs",
		// By using the '--auto' flag, we ensure that git uses the best heuristic
		// for compaction. For reftables, it currently uses a geometric progression.
		// This ensures we don't keep compacting unecessarily to a single file.
		Flags: []gitcmd.Option{gitcmd.Flag{Name: "--auto"}},
	}, gitcmd.WithStderr(&stderr)); err != nil {
		return structerr.New("exec pack-refs: %w", err).WithMetadata("stderr", stderr.String())
	}

	tablesListPost, err := git.ReadReftablesList(repoPath)
	if err != nil {
		return fmt.Errorf("reading tables.list post-compaction: %w", err)
	}

	// If there are no changes after compaction, we don't need to log anything.
	if slices.Equal(tablesListPre, tablesListPost) {
		return nil
	}

	tablesPostMap := make(map[string]struct{})
	for _, table := range tablesListPost {
		tablesPostMap[table] = struct{}{}
	}

	for _, table := range tablesListPre {
		if _, ok := tablesPostMap[table]; !ok {
			// If the table no longer exists, we remove it.
			transaction.walEntry.RecordDirectoryEntryRemoval(
				filepath.Join(transaction.relativePath, "reftable", table),
			)
		} else {
			// If the table exists post compaction too, remove it from the
			// map, since we don't want to record an existing table.
			delete(tablesPostMap, table)
		}
	}

	for file := range tablesPostMap {
		// The remaining tables in tableListPost are new tables
		// which need to be recorded.
		if err := transaction.walEntry.RecordFileCreation(
			filepath.Join(repoPath, "reftable", file),
			filepath.Join(transaction.relativePath, "reftable", file),
		); err != nil {
			return fmt.Errorf("creating new table: %w", err)
		}
	}

	runPackRefs.reftablesAfter = tablesListPost
	runPackRefs.reftablesBefore = tablesListPre

	return nil
}

// preparePackRefsFiles runs git-pack-refs command against the snapshot repository. It collects the resulting packed-refs
// file and the list of pruned references. Unfortunately, git-pack-refs doesn't output which refs are pruned. So, we
// performed two ref walkings before and after running the command. The difference between the two walks is the list of
// pruned refs. This workaround works but is not performant on large repositories with huge amount of loose references.
// Smaller repositories or ones that run housekeeping frequent won't have this issue.
// The work of adding pruned refs dump to `git-pack-refs` is tracked here:
// https://gitlab.com/gitlab-org/git/-/issues/222
func (mgr *TransactionManager) preparePackRefsFiles(ctx context.Context, transaction *Transaction) error {
	runPackRefs := transaction.runHousekeeping.packRefs
	for _, lock := range []string{".new", ".lock"} {
		lockRelativePath := filepath.Join(transaction.relativePath, "packed-refs"+lock)
		lockAbsolutePath := filepath.Join(transaction.snapshot.Root(), lockRelativePath)

		if err := os.Remove(lockAbsolutePath); err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				continue
			}

			return fmt.Errorf("remove %v: %w", lockAbsolutePath, err)
		}

		// The lock file existed. Log its deletion.
		transaction.walEntry.RecordDirectoryEntryRemoval(lockRelativePath)
	}

	// First walk to collect the list of loose refs.
	looseReferences := make(map[git.ReferenceName]struct{})
	repoPath := mgr.getAbsolutePath(transaction.snapshotRepository.GetRelativePath())
	if err := filepath.WalkDir(filepath.Join(repoPath, "refs"), func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !entry.IsDir() {
			// Get fully qualified refs.
			ref, err := filepath.Rel(repoPath, path)
			if err != nil {
				return fmt.Errorf("extracting ref name: %w", err)
			}
			looseReferences[git.ReferenceName(ref)] = struct{}{}
		}
		return nil
	}); err != nil {
		return fmt.Errorf("initial walking refs directory: %w", err)
	}

	packedRefsRelativePath := filepath.Join(transaction.relativePath, "packed-refs")
	packedRefsAbsolutePath := filepath.Join(transaction.snapshot.Root(), packedRefsRelativePath)
	packedRefsPreImage, err := wal.GetInode(packedRefsAbsolutePath)
	if err != nil {
		return fmt.Errorf("get packed-refs pre-image: %w", err)
	}

	// Execute git-pack-refs command. The command runs in the scope of the snapshot repository. Thus, we can
	// let it prune the ref references without causing any impact to other concurrent transactions.
	var stderr bytes.Buffer
	if err := transaction.snapshotRepository.ExecAndWait(ctx, gitcmd.Command{
		Name:  "pack-refs",
		Flags: []gitcmd.Option{gitcmd.Flag{Name: "--all"}},
	}, gitcmd.WithStderr(&stderr)); err != nil {
		return structerr.New("exec pack-refs: %w", err).WithMetadata("stderr", stderr.String())
	}

	packedRefsPostImage, err := wal.GetInode(packedRefsAbsolutePath)
	if err != nil {
		return fmt.Errorf("get packed-refs post-image: %w", err)
	}

	if packedRefsPreImage != packedRefsPostImage {
		if packedRefsPreImage > 0 {
			transaction.walEntry.RecordDirectoryEntryRemoval(packedRefsRelativePath)
		}

		if packedRefsPostImage > 0 {
			if err := transaction.walEntry.RecordFileCreation(packedRefsAbsolutePath, packedRefsRelativePath); err != nil {
				return fmt.Errorf("stage packed-refs: %w", err)
			}
		}
	}

	// Second walk and compare with the initial list of loose references. Any disappeared refs are pruned.
	for ref := range looseReferences {
		_, err := os.Stat(filepath.Join(repoPath, ref.String()))
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				runPackRefs.PrunedRefs[ref] = struct{}{}
			} else {
				return fmt.Errorf("second walk refs directory: %w", err)
			}
		}
	}

	return nil
}

// Git stores loose objects in the object directory under subdirectories with two hex digits in their name.
var regexpLooseObjectDir = regexp.MustCompile("^[[:xdigit:]]{2}$")

// prepareRepacking runs git-repack(1) command against the snapshot repository using desired repacking strategy. Each
// strategy has a different cost and effect corresponding to scheduling frequency.
// - IncrementalWithUnreachable: pack all loose objects into one packfile. This strategy is a no-op because all new
// objects regardless of their reachablity status are packed by default by the manager.
// - Geometric: merge all packs together with geometric repacking. This is expensive or cheap depending on which packs
// get merged. No need for a connectivity check.
// - FullWithUnreachable: merge all packs into one but keep unreachable objects. This is more expensive but we don't
// take connectivity into account. This strategy is essential for object pool. As we cannot prune objects in a pool,
// packing them into one single packfile boosts its performance.
// - FullWithCruft: Merge all packs into one and prune unreachable objects. It is the most effective, but yet costly
// strategy. We cannot run this type of task frequently on a large repository. This strategy is handled as a full
// repacking without cruft because we don't need object expiry.
// Before the command runs, we capture a snapshot of existing packfiles. After the command finishes, we re-capture the
// list and extract the list of to-be-updated packfiles. This practice is to prevent repacking task from deleting
// packfiles of other concurrent updates at the applying phase.
func (mgr *TransactionManager) prepareRepacking(ctx context.Context, transaction *Transaction) error {
	defer trace.StartRegion(ctx, "prepareRepacking").End()

	if transaction.runHousekeeping.repack == nil {
		return nil
	}
	if !transaction.repositoryTarget() {
		return errRelativePathNotSet
	}

	span, ctx := tracing.StartSpanIfHasParent(ctx, "transaction.prepareRepacking", nil)
	defer span.Finish()

	finishTimer := mgr.metrics.housekeeping.ReportTaskLatency("repack", "prepare")
	defer finishTimer()

	var err error
	repack := transaction.runHousekeeping.repack

	// Build a working repository pointing to snapshot repository. Housekeeping task can access the repository
	// without the needs for quarantine.
	workingRepository := mgr.repositoryFactory.Build(transaction.snapshot.RelativePath(transaction.relativePath))
	repoPath := mgr.getAbsolutePath(workingRepository.GetRelativePath())

	isFullRepack, err := housekeeping.ValidateRepacking(repack.config)
	if err != nil {
		return fmt.Errorf("validating repacking: %w", err)
	}

	if repack.config.Strategy == housekeepingcfg.RepackObjectsStrategyIncrementalWithUnreachable {
		// Once the transaction manager has been applied and at least one complete repack has occurred, there
		// should be no loose unreachable objects remaining in the repository. When the transaction manager
		// processes a change, it consolidates all unreachable objects and objects about to become reachable
		// into a new packfile, which is then placed in the repository. As a result, unreachable objects may
		// still exist but are confined to packfiles. These will eventually be cleaned up during a full repack.
		// In the interim, geometric repacking is utilized to optimize the structure of packfiles for faster
		// access. Therefore, this operation is effectively a no-op. However, we maintain it for the sake of
		// backward compatibility with the existing housekeeping scheduler.
		return errRepackNotSupportedStrategy
	}

	// Capture the list of packfiles and their baggages before repacking.
	beforeFiles, err := mgr.collectPackfiles(ctx, repoPath)
	if err != nil {
		return fmt.Errorf("collecting existing packfiles: %w", err)
	}

	// All of the repacking operations pack/remove all loose objects. New ones are not written anymore with transactions.
	// As we're packing them away not, log their removal.
	objectsDirRelativePath := filepath.Join(transaction.relativePath, "objects")
	objectsDirEntries, err := os.ReadDir(filepath.Join(transaction.snapshot.Root(), objectsDirRelativePath))
	if err != nil {
		return fmt.Errorf("read objects dir: %w", err)
	}

	for _, entry := range objectsDirEntries {
		if entry.IsDir() && regexpLooseObjectDir.MatchString(entry.Name()) {
			if err := transaction.walEntry.RecordDirectoryRemoval(transaction.snapshot.Root(), filepath.Join(objectsDirRelativePath, entry.Name())); err != nil {
				return fmt.Errorf("record loose object dir removal: %w", err)
			}
		}
	}

	switch repack.config.Strategy {
	case housekeepingcfg.RepackObjectsStrategyGeometric:
		// Geometric repacking rearranges the list of packfiles according to a geometric progression. This process
		// does not consider object reachability. Since all unreachable objects remain within small packfiles,
		// they become included in the newly created packfiles. Geometric repacking does not prune any objects.
		if err := housekeeping.PerformGeometricRepacking(ctx, workingRepository, repack.config); err != nil {
			return fmt.Errorf("perform geometric repacking: %w", err)
		}
	case housekeepingcfg.RepackObjectsStrategyFullWithUnreachable:
		// Git does not pack loose unreachable objects if there are no existing packs in the repository.
		// Perform an incremental repack first. This ensures all loose object are part of a pack and will be
		// included in the full pack we're about to build. This allows us to remove the loose objects from the
		// repository when applying the pack without losing any objects.
		//
		// Issue: https://gitlab.com/gitlab-org/git/-/issues/336
		if err := housekeeping.PerformIncrementalRepackingWithUnreachable(ctx, workingRepository); err != nil {
			return fmt.Errorf("perform geometric repacking: %w", err)
		}

		// This strategy merges all packfiles into a single packfile, simultaneously removing any loose objects
		// if present. Unreachable objects are then appended to the end of this unified packfile. Although the
		// `git-repack(1)` command does not offer an option to specifically pack loose unreachable objects, this
		// is not an issue because the transaction manager already ensures that unreachable objects are
		// contained within packfiles. Therefore, this strategy effectively consolidates all packfiles into a
		// single one. Adopting this strategy is crucial for alternates, as it ensures that we can manage
		// objects within an object pool without the capability to prune them.
		if err := housekeeping.PerformFullRepackingWithUnreachable(ctx, workingRepository, repack.config); err != nil {
			return err
		}
	case housekeepingcfg.RepackObjectsStrategyFullWithCruft:
		// Both of above strategies don't prune unreachable objects. They re-organize the objects between
		// packfiles. In the traditional housekeeping, the manager gets rid of unreachable objects via full
		// repacking with cruft. It pushes all unreachable objects to a cruft packfile and keeps track of each
		// object mtimes. All unreachable objects exceeding a grace period are cleaned up. The grace period is
		// to ensure the housekeeping doesn't delete a to-be-reachable object accidentally, for example when GC
		// runs while a concurrent push is being processed.
		// The transaction manager handles concurrent requests very differently from the original git way. Each
		// request runs on a snapshot repository and the results are collected in the form of packfiles. Those
		// packfiles contain resulting reachable and unreachable objects. As a result, we don't need to take
		// object expiry nor curft pack into account. This operation triggers a normal full repack without
		// cruft packing.
		// Afterward, packed unreachable objects are removed. During migration to transaction system, there
		// might be some loose unreachable objects. They will eventually be packed via either of the above tasks.
		if err := housekeeping.PerformRepack(ctx, workingRepository, repack.config,
			// Do a full repack. By using `-a` instead of `-A` we will immediately discard unreachable
			// objects instead of exploding them into loose objects.
			gitcmd.Flag{Name: "-a"},
			// Don't include objects part of alternate.
			gitcmd.Flag{Name: "-l"},
			// Delete loose objects made redundant by this repack and redundant packfiles.
			gitcmd.Flag{Name: "-d"},
		); err != nil {
			return err
		}
	}

	// Re-capture the list of packfiles and their baggages after repacking.
	afterFiles, err := mgr.collectPackfiles(ctx, repoPath)
	if err != nil {
		return fmt.Errorf("collecting new packfiles: %w", err)
	}

	for file := range beforeFiles {
		// We delete the files only if it's missing from the before set.
		if _, exist := afterFiles[file]; !exist {
			transaction.walEntry.RecordDirectoryEntryRemoval(filepath.Join(
				objectsDirRelativePath, "pack", file,
			))
		}
	}

	for file := range afterFiles {
		// Similarly, we don't need to link existing packfiles.
		if _, exist := beforeFiles[file]; !exist {
			fileRelativePath := filepath.Join(objectsDirRelativePath, "pack", file)

			if err := transaction.walEntry.RecordFileCreation(
				filepath.Join(transaction.snapshot.Root(), fileRelativePath),
				fileRelativePath,
			); err != nil {
				return fmt.Errorf("record pack file creations: %q: %w", file, err)
			}
		}
	}

	if isFullRepack {
		timestampRelativePath := filepath.Join(transaction.relativePath, stats.FullRepackTimestampFilename)
		timestampAbsolutePath := filepath.Join(transaction.snapshot.Root(), timestampRelativePath)

		info, err := os.Stat(timestampAbsolutePath)
		if err != nil && !errors.Is(err, fs.ErrNotExist) {
			return fmt.Errorf("stat repack timestamp file: %w", err)
		}

		if err := stats.UpdateFullRepackTimestamp(filepath.Join(transaction.snapshot.Root(), transaction.relativePath), time.Now()); err != nil {
			return fmt.Errorf("updating repack timestamp: %w", err)
		}

		if info != nil {
			// The file existed and needs to be removed first.
			transaction.walEntry.RecordDirectoryEntryRemoval(timestampRelativePath)
		}

		if err := transaction.walEntry.RecordFileCreation(timestampAbsolutePath, timestampRelativePath); err != nil {
			return fmt.Errorf("stage repacking timestamp: %w", err)
		}
	}

	return nil
}

// prepareCommitGraphs updates the commit-graph in the snapshot repository. It then hard-links the
// graphs to the staging repository so it can be applied by the transaction manager.
func (mgr *TransactionManager) prepareCommitGraphs(ctx context.Context, transaction *Transaction) error {
	defer trace.StartRegion(ctx, "prepareCommitGraphs").End()

	if transaction.runHousekeeping.writeCommitGraphs == nil {
		return nil
	}
	if !transaction.repositoryTarget() {
		return errRelativePathNotSet
	}

	span, ctx := tracing.StartSpanIfHasParent(ctx, "transaction.prepareCommitGraphs", nil)
	defer span.Finish()

	finishTimer := mgr.metrics.housekeeping.ReportTaskLatency("commit-graph", "prepare")
	defer finishTimer()

	// Check if the legacy commit-graph file exists. If so, remove it as we'd replace it with a
	// commit-graph chain.
	commitGraphRelativePath := filepath.Join(transaction.relativePath, "objects", "info", "commit-graph")
	if info, err := os.Stat(filepath.Join(
		transaction.snapshot.Root(),
		commitGraphRelativePath,
	)); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("stat commit-graph: %w", err)
	} else if info != nil {
		transaction.walEntry.RecordDirectoryEntryRemoval(commitGraphRelativePath)
	}

	// Check for an existing commit-graphs directory. If so, delete it as
	// we log all commit graphs created.
	commitGraphsRelativePath := filepath.Join(transaction.relativePath, "objects", "info", "commit-graphs")
	commitGraphsAbsolutePath := filepath.Join(transaction.snapshot.Root(), commitGraphsRelativePath)
	if info, err := os.Stat(commitGraphsAbsolutePath); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("stat commit-graphs pre-image: %w", err)
	} else if info != nil {
		if err := transaction.walEntry.RecordDirectoryRemoval(transaction.snapshot.Root(), commitGraphsRelativePath); err != nil {
			return fmt.Errorf("record commit-graphs removal: %w", err)
		}
	}

	if err := housekeeping.WriteCommitGraph(ctx,
		mgr.repositoryFactory.Build(transaction.snapshot.RelativePath(transaction.relativePath)),
		transaction.runHousekeeping.writeCommitGraphs.config,
	); err != nil {
		return fmt.Errorf("re-writing commit graph: %w", err)
	}

	// If the directory exists after the operation, log all of the new state.
	if info, err := os.Stat(commitGraphsAbsolutePath); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("stat commit-graphs post-image: %w", err)
	} else if info != nil {
		if err := transaction.walEntry.RecordDirectoryCreation(transaction.snapshot.Root(), commitGraphsRelativePath); err != nil {
			return fmt.Errorf("record commit-graphs creation: %w", err)
		}
	}

	return nil
}

// packfileExtensions contains the packfile extension and its dependencies. They will be collected after running
// repacking command.
var packfileExtensions = map[string]struct{}{
	"multi-pack-index": {},
	".pack":            {},
	".idx":             {},
	".rev":             {},
	".mtimes":          {},
	".bitmap":          {},
}

// collectPackfiles collects the list of packfiles and their luggage files.
func (mgr *TransactionManager) collectPackfiles(ctx context.Context, repoPath string) (map[string]struct{}, error) {
	files, err := os.ReadDir(filepath.Join(repoPath, "objects", "pack"))
	if err != nil {
		return nil, fmt.Errorf("reading objects/pack dir: %w", err)
	}

	// Filter packfiles and relevant files.
	collectedFiles := make(map[string]struct{})
	for _, file := range files {
		// objects/pack directory should not include any sub-directory. We can simply ignore them.
		if file.IsDir() {
			continue
		}
		for extension := range packfileExtensions {
			if strings.HasSuffix(file.Name(), extension) {
				collectedFiles[file.Name()] = struct{}{}
			}
		}
	}

	return collectedFiles, nil
}

// verifyPackRefsReftable verifies if the compaction performed can be safely
// applied to the repository.

// We merge the tables.list generated by our compaction with the existing
// repositories tables.list. Because there could have been new tables after
// we performed compaction.
func (mgr *TransactionManager) verifyPackRefsReftable(transaction *Transaction) (*gitalypb.LogEntry_Housekeeping_PackRefs, error) {
	tables := transaction.runHousekeeping.packRefs.reftablesAfter
	if len(tables) < 1 {
		return nil, nil
	}

	// The tables.list from the snapshot repository should be identical to that of the staging
	// repository. However, concurrent writes might have occurred which wrote new tables to the
	// staging repository. We shouldn't loose that data. So we merge the compacted tables.list
	// with the newer tables from the staging repositories tables.list.
	repoPath := mgr.getAbsolutePath(transaction.stagingRepository.GetRelativePath())
	newTableList, err := git.ReadReftablesList(repoPath)
	if err != nil {
		return nil, fmt.Errorf("reading tables.list: %w", err)
	}

	snapshotRepoPath := mgr.getAbsolutePath(transaction.snapshotRepository.GetRelativePath())

	// tables.list is hard-linked from the repository to the snapshot, we shouldn't
	// directly write to it as we'd modify the original. So let's remove the
	// hard-linked file.
	if err = os.Remove(filepath.Join(snapshotRepoPath, "reftable", "tables.list")); err != nil {
		return nil, fmt.Errorf("removing tables.list: %w", err)
	}

	// We need to merge the tables.list of snapshotRepo with the latest from stagingRepo.
	tablesBefore := transaction.runHousekeeping.packRefs.reftablesBefore
	finalTableList := append(tables, newTableList[len(tablesBefore):]...)

	// Write the updated tables.list so we can add the required operations.
	if err := os.WriteFile(
		filepath.Join(snapshotRepoPath, "reftable", "tables.list"),
		[]byte(strings.Join(finalTableList, "\n")),
		mode.File,
	); err != nil {
		return nil, fmt.Errorf("writing tables.list: %w", err)
	}

	// Add operation to update the tables.list.
	if err := transaction.walEntry.RecordFileUpdate(
		transaction.snapshot.Root(),
		filepath.Join(transaction.relativePath, "reftable", "tables.list"),
	); err != nil {
		return nil, fmt.Errorf("updating tables.list: %w", err)
	}

	return nil, nil
}

// verifyPackRefsFiles verifies if the pack-refs housekeeping task can be logged. Ideally, we can just apply the packed-refs
// file and prune the loose references. Unfortunately, there could be a ref modification between the time the pack-refs
// command runs and the time this transaction is logged. Thus, we need to verify if the transaction conflicts with the
// current state of the repository.
//
// There are three cases when a reference is modified:
// - Reference creation: this is the easiest case. The new reference exists as a loose reference on disk and shadows the
// one in the packed-ref.
// - Reference update: similarly, the loose reference shadows the one in packed-refs with the new OID. However, we need
// to remove it from the list of pruned references. Otherwise, the repository continues to use the old OID.
// - Reference deletion. When a reference is deleted, both loose reference and the entry in the packed-refs file are
// removed. The reflogs are also removed. In addition, we don't use reflogs in Gitaly as core.logAllRefUpdates defaults
// to false in bare repositories. It could of course be that an admin manually enabled it by modifying the config
// on-disk directly. There is no way to extract reference deletion between two states.
//
// In theory, if there is any reference deletion, it can be removed from the packed-refs file. However, it requires
// parsing and regenerating the packed-refs file. So, let's settle down with a conflict error at this point.
func (mgr *TransactionManager) verifyPackRefsFiles(ctx context.Context, transaction *Transaction) (*gitalypb.LogEntry_Housekeeping_PackRefs, error) {
	objectHash, err := transaction.stagingRepository.ObjectHash(ctx)
	if err != nil {
		return nil, fmt.Errorf("object hash: %w", err)
	}
	packRefs := transaction.runHousekeeping.packRefs

	// Check for any concurrent ref deletion between this transaction's snapshot LSN to the end.
	if err := mgr.walkCommittedEntries(transaction, func(entry *gitalypb.LogEntry, objectDependencies map[git.ObjectID]struct{}) error {
		for _, refTransaction := range entry.GetReferenceTransactions() {
			for _, change := range refTransaction.GetChanges() {
				// We handle HEAD updates through the git-update-ref, but since
				// it is not part of the packed-refs file, we don't need to worry about it.
				if bytes.Equal(change.GetReferenceName(), []byte("HEAD")) {
					continue
				}

				if objectHash.IsZeroOID(git.ObjectID(change.GetNewOid())) {
					// Oops, there is a reference deletion. Bail out.
					return errPackRefsConflictRefDeletion
				}
				// Ref update. Remove the updated ref from the list of pruned refs so that the
				// new OID in loose reference shadows the outdated OID in packed-refs.
				delete(packRefs.PrunedRefs, git.ReferenceName(change.GetReferenceName()))
			}
		}
		return nil
	}); err != nil {
		return nil, fmt.Errorf("walking committed entries: %w", err)
	}

	// Build a tree of the loose references we need to prune.
	prunedRefs := reftree.New()
	for reference := range packRefs.PrunedRefs {
		if err := prunedRefs.InsertReference(reference.String()); err != nil {
			return nil, fmt.Errorf("insert reference: %w", err)
		}
	}

	directoriesToKeep := map[string]struct{}{
		// Valid git directory needs to have a 'refs' directory, so we can't remove it.
		"refs": {},
		// Git keeps these top-level directories. We keep them as well to reduce
		// conflicting operations on them.
		"refs/heads": {}, "refs/tags": {},
	}

	// Walk down the deleted references from the leaves towards the root. We'll log the deletion
	// of loose references as we walk towards the root, and remove any directories along the path
	// that became empty as a result of removing the references.
	if err := prunedRefs.WalkPostOrder(func(path string, isDir bool) error {
		if _, ok := directoriesToKeep[path]; ok {
			return nil
		}

		relativePath := filepath.Join(transaction.relativePath, path)

		if err := os.Remove(filepath.Join(
			transaction.stagingSnapshot.Root(),
			relativePath,
		)); err != nil {
			if errors.Is(err, syscall.ENOTEMPTY) {
				// This directory was not empty because someone concurrently wrote
				// a reference into it. Keep it in place.
				return nil
			}

			return fmt.Errorf("remove loose reference: %w", err)
		}

		transaction.walEntry.RecordDirectoryEntryRemoval(relativePath)

		return nil
	}); err != nil {
		return nil, fmt.Errorf("walk post order: %w", err)
	}

	return &gitalypb.LogEntry_Housekeeping_PackRefs{}, nil
}

// verifyPackRefs verifies if the git-pack-refs(1) can be applied without any conflicts.
// It calls the reference backend specific function to handle the core logic.
func (mgr *TransactionManager) verifyPackRefs(ctx context.Context, transaction *Transaction) (*gitalypb.LogEntry_Housekeeping_PackRefs, error) {
	if transaction.runHousekeeping.packRefs == nil {
		return nil, nil
	}

	span, ctx := tracing.StartSpanIfHasParent(ctx, "transaction.verifyPackRefs", nil)
	defer span.Finish()

	finishTimer := mgr.metrics.housekeeping.ReportTaskLatency("pack-refs", "verify")
	defer finishTimer()

	refBackend, err := transaction.stagingRepository.ReferenceBackend(ctx)
	if err != nil {
		return nil, fmt.Errorf("reference backend: %w", err)
	}

	if refBackend == git.ReferenceBackendReftables {
		packRefs, err := mgr.verifyPackRefsReftable(transaction)
		if err != nil {
			return nil, fmt.Errorf("reftable backend: %w", err)
		}
		return packRefs, nil
	}

	packRefs, err := mgr.verifyPackRefsFiles(ctx, transaction)
	if err != nil {
		return nil, fmt.Errorf("files backend: %w", err)
	}
	return packRefs, nil
}

// verifyRepacking checks the object repacking operations for conflicts.
//
// Object repacking without pruning is conflict-free operation. It only rearranges the objects on the disk into
// a more optimal physical format. All objects that other transactions could need are still present in pure repacking
// operations.
//
// Repacking operations that prune unreachable objects from the repository may lead to conflicts. Conflicts may occur
// if concurrent transactions depend on the unreachable objects.
//
// 1. Transactions may point references to the previously unreachable objects and make them reachable.
// 2. Transactions may write new objects that depend on the unreachable objects.
//
// In both cases a pruning operation that removes the objects must be aborted. In the first case, the pruning
// operation would remove reachable objects from the repository and the repository becomes corrupted. In the second case,
// the new objects written into the repository may not be necessarily reachable. Transactions depend on an invariant
// that all objects in the repository are valid. Therefore, we must also reject transactions that attempt to remove
// dependencies of unreachable objects even if such state isn't considered corrupted by Git.
//
// As we don't have a list of pruned objects at hand, the conflicts are identified by checking whether the recorded
// dependencies of a transaction would still exist in the repository after applying the pruning operation.
func (mgr *TransactionManager) verifyRepacking(ctx context.Context, transaction *Transaction) (returnedErr error) {
	repack := transaction.runHousekeeping.repack
	if repack == nil {
		return nil
	}

	span, ctx := tracing.StartSpanIfHasParent(ctx, "transaction.verifyRepacking", nil)
	defer span.Finish()

	finishTimer := mgr.metrics.housekeeping.ReportTaskLatency("repack", "verify")
	defer finishTimer()

	// Other strategies re-organize packfiles without pruning unreachable objects. No need to run following
	// expensive verification.
	if repack.config.Strategy != housekeepingcfg.RepackObjectsStrategyFullWithCruft {
		return nil
	}

	// Setup a working repository of the destination repository and all changes of current transactions. All
	// concurrent changes must land in that repository already.
	snapshot, err := mgr.snapshotManager.GetSnapshot(ctx, []string{transaction.relativePath}, true)
	if err != nil {
		return fmt.Errorf("setting up new snapshot for verifying repacking: %w", err)
	}
	defer func() {
		if err := snapshot.Close(); err != nil {
			returnedErr = errors.Join(returnedErr, fmt.Errorf("close snapshot: %w", err))
		}
	}()

	// To verify the housekeeping transaction, we apply the operations it staged to a snapshot of the target
	// repository's current state. We then check whether the resulting state is valid.
	if err := func() error {
		dbTX := mgr.db.NewTransaction(true)
		defer dbTX.Discard()

		return applyOperations(
			ctx,
			// We're not committing the changes in to the snapshot, so no need to fsync anything.
			func(context.Context, string) error { return nil },
			snapshot.Root(),
			transaction.walEntry.Directory(),
			transaction.walEntry.Operations(),
			dbTX,
		)
	}(); err != nil {
		return fmt.Errorf("apply operations: %w", err)
	}

	// Collect object dependencies. All of them should exist in the resulting packfile or new concurrent
	// packfiles while repacking is running.
	objectDependencies := map[git.ObjectID]struct{}{}
	if err := mgr.walkCommittedEntries(transaction, func(entry *gitalypb.LogEntry, txnObjectDependencies map[git.ObjectID]struct{}) error {
		for oid := range txnObjectDependencies {
			objectDependencies[oid] = struct{}{}
		}

		return nil
	}); err != nil {
		return fmt.Errorf("walking committed entries: %w", err)
	}

	if err := mgr.verifyObjectsExist(ctx, mgr.repositoryFactory.Build(snapshot.RelativePath(transaction.relativePath)), objectDependencies); err != nil {
		var errInvalidObject localrepo.InvalidObjectError
		if errors.As(err, &errInvalidObject) {
			return errRepackConflictPrunedObject
		}

		return fmt.Errorf("verify objects exist: %w", err)
	}

	return nil
}
