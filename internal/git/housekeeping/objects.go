package housekeeping

import (
	"context"
	"errors"
	"fmt"
	"os/exec"
	"strconv"
	"strings"
	"time"

	"gitlab.com/gitlab-org/gitaly/v15/internal/git"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/repository"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/stats"
	"gitlab.com/gitlab-org/gitaly/v15/internal/structerr"
)

const (
	// looseObjectLimit is the limit of loose objects we accept both when doing incremental
	// repacks and when pruning objects.
	looseObjectLimit = 1024
	// rfc2822DateFormat is the date format that Git typically uses for dates.
	rfc2822DateFormat = "Mon Jan 02 2006 15:04:05 -0700"
)

// RepackObjectsStrategy defines how objects shall be repacked.
type RepackObjectsStrategy string

const (
	// RepackObjectsStrategyIncremental performs an incremental repack by writing all loose
	// objects that are currently reachable into a new packfile.
	RepackObjectsStrategyIncremental = RepackObjectsStrategy("incremental")
	// RepackObjectsStrategyFullWithLooseUnreachable performs a full repack by writing all
	// reachable objects into a new packfile. Unreachable objects will be exploded into loose
	// objects.
	RepackObjectsStrategyFullWithLooseUnreachable = RepackObjectsStrategy("full_with_loose_unreachable")
	// RepackObjectsStrategyFullWithCruft performs a full repack by writing all reachable
	// objects into a new packfile. Unreachable objects will be written into a separate cruft
	// packfile.
	RepackObjectsStrategyFullWithCruft = RepackObjectsStrategy("full_with_cruft")
	// RepackObjectsStrategyFullWithUnreachable performs a full repack by writing all reachable
	// objects into a new packfile. Packed unreachable objects will be appended to the packfile
	// and redundant loose object files will be deleted.
	//
	// Note that this will not include unreachable loose objects, but only packed loose objects.
	// git-repack(1) does not currently expose an option to make it include all objects.
	// Combined with geometric repacks though this is acceptable as the geometric strategy will
	// include all loose objects.
	RepackObjectsStrategyFullWithUnreachable = RepackObjectsStrategy("full_with_unreachable")
	// RepackObjectsStrategyGeometric performs an geometric repack. This strategy will repack
	// packfiles so that the resulting pack structure forms a geometric sequence in the number
	// of objects. Loose objects will get soaked up as part of the repack regardless of their
	// reachability.
	RepackObjectsStrategyGeometric = RepackObjectsStrategy("geometric")
)

// RepackObjectsConfig is configuration for RepackObjects.
type RepackObjectsConfig struct {
	// Strategy determines the strategy with which to repack objects.
	Strategy RepackObjectsStrategy
	// WriteBitmap determines whether reachability bitmaps should be written or not. There is no
	// reason to set this to `false`, except for legacy compatibility reasons with existing RPC
	// behaviour
	WriteBitmap bool
	// WriteMultiPackIndex determines whether a multi-pack index should be written or not.
	WriteMultiPackIndex bool
	// CruftExpireBefore determines the cutoff date before which unreachable cruft objects shall
	// be expired and thus deleted.
	CruftExpireBefore time.Time
}

// RepackObjects repacks objects in the given repository and updates the commit-graph. The way
// objects are repacked is determined via the RepackObjectsConfig.
func RepackObjects(ctx context.Context, repo *localrepo.Repo, cfg RepackObjectsConfig) error {
	repoPath, err := repo.Path()
	if err != nil {
		return err
	}

	var isFullRepack bool
	switch cfg.Strategy {
	case RepackObjectsStrategyIncremental, RepackObjectsStrategyGeometric:
		isFullRepack = false
	case RepackObjectsStrategyFullWithLooseUnreachable, RepackObjectsStrategyFullWithCruft, RepackObjectsStrategyFullWithUnreachable:
		isFullRepack = true
	default:
		return structerr.NewInvalidArgument("invalid strategy: %q", cfg.Strategy)
	}

	if !isFullRepack && !cfg.WriteMultiPackIndex && cfg.WriteBitmap {
		return structerr.NewInvalidArgument("cannot write packfile bitmap for an incremental repack")
	}
	if cfg.Strategy != RepackObjectsStrategyFullWithCruft && !cfg.CruftExpireBefore.IsZero() {
		return structerr.NewInvalidArgument("cannot expire cruft objects when not writing cruft packs")
	}

	if isFullRepack {
		// When we have performed a full repack we're updating the "full-repack-timestamp"
		// file. This is done so that we can tell when we have last performed a full repack
		// in a repository. This information can be used by our heuristics to effectively
		// rate-limit the frequency of full repacks.
		//
		// Note that we write the file _before_ actually writing the new pack, which means
		// that even if the full repack fails, we would still pretend to have done it. This
		// is done intentionally, as the likelihood for huge repositories to fail during a
		// full repack is comparatively high. So if we didn't update the timestamp in case
		// of a failure we'd potentially busy-spin trying to do a full repack.
		if err := stats.UpdateFullRepackTimestamp(repoPath, time.Now()); err != nil {
			return fmt.Errorf("updating full-repack timestamp: %w", err)
		}
	}

	var options []git.Option
	switch cfg.Strategy {
	case RepackObjectsStrategyIncremental:
		options = []git.Option{
			git.Flag{Name: "-d"},
		}
	case RepackObjectsStrategyFullWithLooseUnreachable:
		options = []git.Option{
			git.Flag{Name: "-A"},
			git.Flag{Name: "--pack-kept-objects"},
			git.Flag{Name: "-l"},
			git.Flag{Name: "-d"},
		}
	case RepackObjectsStrategyFullWithCruft:
		options = []git.Option{
			git.Flag{Name: "--cruft"},
			git.Flag{Name: "--pack-kept-objects"},
			git.Flag{Name: "-l"},
			git.Flag{Name: "-d"},
		}

		if !cfg.CruftExpireBefore.IsZero() {
			options = append(options, git.ValueFlag{
				Name:  "--cruft-expiration",
				Value: cfg.CruftExpireBefore.Format(rfc2822DateFormat),
			})
		}
	case RepackObjectsStrategyFullWithUnreachable:
		options = []git.Option{
			// Do a full repack.
			git.Flag{Name: "-a"},
			// Don't include objects part of alternate.
			git.Flag{Name: "-l"},
			// Delete loose objects made redundant by this repack.
			git.Flag{Name: "-d"},
			// Keep unreachable objects part of the old packs in the new pack.
			git.Flag{Name: "--keep-unreachable"},
		}
	case RepackObjectsStrategyGeometric:
		options = []git.Option{
			// We use a geometric factor `r`, which means that every successively larger
			// packfile must have at least `r` times the number of objects.
			//
			// This factor ultimately determines how many packfiles there can be at a
			// maximum in a repository for a given number of objects. The maximum number
			// of objects with `n` packfiles and a factor `r` is `(1 - r^n) / (1 - r)`.
			// E.g. with a factor of 4 and 10 packfiles, we can have at most 349,525
			// objects, with 16 packfiles we can have 1,431,655,765 objects. Contrary to
			// that, having a factor of 2 will translate to 1023 objects at 10 packfiles
			// and 65535 objects at 16 packfiles at a maximum.
			//
			// So what we're effectively choosing here is how often we need to repack
			// larger parts of the repository. The higher the factor the more we'll have
			// to repack as the packfiles will be larger. On the other hand, having a
			// smaller factor means we'll have to repack less objects as the slices we
			// need to repack will have less objects.
			//
			// The end result is a hybrid approach between incremental repacks and full
			// repacks: we won't typically repack the full repository, but only a subset
			// of packfiles.
			//
			// For now, we choose a geometric factor of two. Large repositories nowadays
			// typically have a few million objects, which would boil down to having at
			// most 32 packfiles in the repository. This number is not scientifically
			// chosen though any may be changed at a later point in time.
			git.ValueFlag{Name: "--geometric", Value: "2"},
			// Make sure to delete loose objects and packfiles that are made obsolete
			// by the new packfile.
			git.Flag{Name: "-d"},
			// Don't include objects part of an alternate.
			git.Flag{Name: "-l"},
		}
	default:
		return structerr.NewInvalidArgument("invalid strategy: %q", cfg.Strategy)
	}
	if cfg.WriteMultiPackIndex {
		options = append(options, git.Flag{Name: "--write-midx"})
	}

	var stderr strings.Builder
	if err := repo.ExecAndWait(ctx,
		git.Command{
			Name:  "repack",
			Flags: options,
		},
		git.WithConfig(GetRepackGitConfig(ctx, repo, cfg.WriteBitmap)...),
		git.WithStderr(&stderr),
	); err != nil {
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			// We do not embed the `exec.ExitError` directly as it wouldn't typically
			// contain any useful information anyway except for its error code. So we
			// instead only expose what matters and attach stderr to the error metadata.
			return structerr.New("repack failed with error code %d", exitErr.ExitCode()).WithMetadata("stderr", stderr.String())
		}

		return fmt.Errorf("repack failed: %w", err)
	}

	return nil
}

// GetRepackGitConfig returns configuration suitable for Git commands which write new packfiles.
func GetRepackGitConfig(ctx context.Context, repo repository.GitRepo, bitmap bool) []git.ConfigPair {
	config := []git.ConfigPair{
		{Key: "repack.useDeltaIslands", Value: "true"},
		{Key: "repack.writeBitmaps", Value: strconv.FormatBool(bitmap)},
		{Key: "pack.writeBitmapLookupTable", Value: "true"},
	}

	if stats.IsPoolRepository(repo) {
		config = append(config,
			git.ConfigPair{Key: "pack.island", Value: git.ObjectPoolRefNamespace + "/he(a)ds"},
			git.ConfigPair{Key: "pack.island", Value: git.ObjectPoolRefNamespace + "/t(a)gs"},
			git.ConfigPair{Key: "pack.islandCore", Value: "a"},
		)
	} else {
		config = append(config,
			git.ConfigPair{Key: "pack.island", Value: "r(e)fs/heads"},
			git.ConfigPair{Key: "pack.island", Value: "r(e)fs/tags"},
			git.ConfigPair{Key: "pack.islandCore", Value: "e"},
		)
	}

	return config
}

// PruneObjectsConfig determines which objects should be pruned in PruneObjects.
type PruneObjectsConfig struct {
	// ExpireBefore controls the grace period after which unreachable objects shall be pruned.
	// An unreachable object must be older than the given date in order to be considered for
	// deletion.
	ExpireBefore time.Time
}

// PruneObjects prunes loose objects from the repository that are already packed or which are
// unreachable and older than the configured expiry date.
func PruneObjects(ctx context.Context, repo *localrepo.Repo, cfg PruneObjectsConfig) error {
	if err := repo.ExecAndWait(ctx, git.Command{
		Name: "prune",
		Flags: []git.Option{
			// By default, this prunes all unreachable objects regardless of when they
			// have last been accessed. This opens us up for races when there are
			// concurrent commands which are just at the point of writing objects into
			// the repository, but which haven't yet updated any references to make them
			// reachable.
			//
			// To avoid this race, we use a grace window that can be specified by the
			// caller so that we only delete objects that are older than this grace
			// window.
			git.ValueFlag{Name: "--expire", Value: cfg.ExpireBefore.Format(rfc2822DateFormat)},
		},
	}); err != nil {
		return fmt.Errorf("executing prune: %w", err)
	}

	return nil
}
