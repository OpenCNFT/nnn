package housekeeping

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"time"

	"gitlab.com/gitlab-org/gitaly/v16/internal/command"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/gitcmd"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v16/internal/structerr"
)

const (
	// WorktreesPrefix is a subdirectory in the repository where Gitaly creates worktrees.
	WorktreesPrefix = "worktrees"
	// GitlabWorktreePrefix is a subdirectory in the repository where Gitaly creates worktrees.
	GitlabWorktreePrefix = "gitlab-worktree"
)

// CleanupWorktrees cleans up stale and disconnected worktrees for the given repository.
func CleanupWorktrees(ctx context.Context, repo *localrepo.Repo) error {
	if _, err := repo.Path(ctx); err != nil {
		return err
	}

	worktreeThreshold := time.Now().Add(-6 * time.Hour)
	if err := cleanStaleWorktrees(ctx, repo, worktreeThreshold); err != nil {
		return structerr.NewInternal("cleanStaleWorktrees: %w", err)
	}

	if err := cleanDisconnectedWorktrees(ctx, repo); err != nil {
		return structerr.NewInternal("cleanDisconnectedWorktrees: %w", err)
	}

	return nil
}

func cleanStaleWorktrees(ctx context.Context, repo *localrepo.Repo, threshold time.Time) error {
	repoPath, err := repo.Path(ctx)
	if err != nil {
		return err
	}

	worktreePath := filepath.Join(repoPath, GitlabWorktreePrefix)

	dirInfo, err := os.Stat(worktreePath)
	if err != nil {
		if os.IsNotExist(err) || !dirInfo.IsDir() {
			return nil
		}
		return err
	}

	worktreeEntries, err := os.ReadDir(worktreePath)
	if err != nil {
		return err
	}

	for _, entry := range worktreeEntries {
		if !entry.IsDir() || (entry.Type()&fs.ModeSymlink != 0) {
			continue
		}

		info, err := entry.Info()
		if err != nil {
			// It's fine if the entry has disappeared meanwhile, we wanted to remove it
			// anyway.
			if errors.Is(err, fs.ErrNotExist) {
				continue
			}

			return fmt.Errorf("statting worktree: %w", err)
		}

		if info.ModTime().Before(threshold) {
			err := removeWorktree(ctx, repo, info.Name())
			switch {
			case errors.Is(err, errUnknownWorktree):
				// if git doesn't recognise the worktree then we can safely remove it
				if err := os.RemoveAll(filepath.Join(worktreePath, info.Name())); err != nil {
					return fmt.Errorf("worktree remove dir: %w", err)
				}
			case err != nil:
				return err
			}
		}
	}

	return nil
}

// errUnknownWorktree indicates that git does not recognise the worktree
var errUnknownWorktree = errors.New("unknown worktree")

func removeWorktree(ctx context.Context, repo *localrepo.Repo, name string) error {
	objectHash, err := repo.ObjectHash(ctx)
	if err != nil {
		return fmt.Errorf("detecting object hash: %w", err)
	}

	var stderr bytes.Buffer
	err = repo.ExecAndWait(ctx, gitcmd.Command{
		Name:   "worktree",
		Action: "remove",
		Flags:  []gitcmd.Option{gitcmd.Flag{Name: "--force"}},
		Args:   []string{name},
	},
		gitcmd.WithRefTxHook(objectHash, repo),
		gitcmd.WithStderr(&stderr),
	)
	if isExitWithCode(err, 128) && strings.HasPrefix(stderr.String(), "fatal: '"+name+"' is not a working tree") {
		return errUnknownWorktree
	} else if err != nil {
		return fmt.Errorf("remove worktree: %w, stderr: %q", err, stderr.String())
	}

	return nil
}

func isExitWithCode(err error, code int) bool {
	actual, ok := command.ExitStatus(err)
	if !ok {
		return false
	}

	return code == actual
}

func cleanDisconnectedWorktrees(ctx context.Context, repo *localrepo.Repo) error {
	repoPath, err := repo.Path(ctx)
	if err != nil {
		return err
	}

	// Spawning a command is expensive. We thus try to avoid the overhead by first
	// determining if there could possibly be any work to be done by git-worktree(1). We do so
	// by reading the directory in which worktrees are stored, and if it's empty then we know
	// that there aren't any worktrees in the first place.
	worktreeEntries, err := os.ReadDir(filepath.Join(repoPath, WorktreesPrefix))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
	}

	hasWorktrees := false
	for _, worktreeEntry := range worktreeEntries {
		if !worktreeEntry.IsDir() {
			continue
		}

		if worktreeEntry.Name() == "." || worktreeEntry.Name() == ".." {
			continue
		}

		hasWorktrees = true
		break
	}

	// There are no worktrees, so let's avoid spawning the Git command.
	if !hasWorktrees {
		return nil
	}

	objectHash, err := repo.ObjectHash(ctx)
	if err != nil {
		return fmt.Errorf("detecting object hash: %w", err)
	}

	return repo.ExecAndWait(ctx, gitcmd.Command{
		Name:   "worktree",
		Action: "prune",
	}, gitcmd.WithRefTxHook(objectHash, repo))
}
