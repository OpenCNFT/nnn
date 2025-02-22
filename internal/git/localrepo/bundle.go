package localrepo

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"gitlab.com/gitlab-org/gitaly/v16/internal/featureflag"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/gitcmd"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/transaction"
	"gitlab.com/gitlab-org/gitaly/v16/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v16/internal/tempdir"
)

// ErrEmptyBundle is returned when the bundle to be created would have been empty.
var ErrEmptyBundle = errors.New("refusing to create empty bundle")

// CreateBundleOpts are optional configurations used when creating a bundle
type CreateBundleOpts struct {
	// Patterns contains all patterns which shall be bundled. Patterns should
	// be in the format accepted by git-rev-list(1) over stdin. Patterns which
	// don't match any reference will be silently ignored.
	Patterns io.Reader
}

// CreateBundle creates a bundle that contains all refs.
// When the bundle would be empty ErrEmptyBundle is returned.
func (repo *Repo) CreateBundle(ctx context.Context, out io.Writer, opts *CreateBundleOpts) error {
	if opts == nil {
		opts = &CreateBundleOpts{}
	}

	var stderr strings.Builder

	gitOpts := []gitcmd.Option{
		gitcmd.OutputToStdout,
	}
	cmdOpts := []gitcmd.CmdOpt{
		gitcmd.WithStdout(out),
		gitcmd.WithStderr(&stderr),
	}

	if opts.Patterns != nil {
		gitOpts = append(gitOpts,
			gitcmd.Flag{Name: "--ignore-missing"},
			gitcmd.Flag{Name: "--stdin"},
		)
		cmdOpts = append(cmdOpts, gitcmd.WithStdin(opts.Patterns))
	} else {
		gitOpts = append(gitOpts, gitcmd.Flag{Name: "--all"})
	}

	err := repo.ExecAndWait(ctx,
		gitcmd.Command{
			Name:   "bundle",
			Action: "create",
			Flags:  gitOpts,
		},
		cmdOpts...,
	)
	if isExitWithCode(err, 128) && strings.HasPrefix(stderr.String(), "fatal: Refusing to create empty bundle.") {
		return fmt.Errorf("create bundle: %w", ErrEmptyBundle)
	} else if err != nil {
		return fmt.Errorf("create bundle: %w: stderr: %q", err, stderr.String())
	}

	return nil
}

// CloneBundle clones a repository from a Git bundle with the mirror refspec.
func (repo *Repo) CloneBundle(ctx context.Context, reader io.Reader) error {
	// When cloning from a file, `git-clone(1)` requires the path to the file. Create a temporary
	// file with the Git bundle contents that is used for cloning.
	bundlePath, err := repo.createTempBundle(ctx, reader)
	if err != nil {
		return err
	}

	repoPath, err := repo.locator.GetRepoPath(ctx, repo, storage.WithRepositoryVerificationSkipped())
	if err != nil {
		return fmt.Errorf("getting repo path: %w", err)
	}

	refFormat := git.ReferenceBackendFiles.Name
	if featureflag.NewRepoReftableBackend.IsEnabled(ctx) {
		refFormat = git.ReferenceBackendReftables.Name
	}

	var cloneErr bytes.Buffer
	cloneCmd, err := repo.gitCmdFactory.NewWithoutRepo(ctx,
		gitcmd.Command{
			Name: "clone",
			Flags: []gitcmd.Option{
				gitcmd.Flag{Name: "--quiet"},
				gitcmd.Flag{Name: "--mirror"},
				gitcmd.Flag{Name: fmt.Sprintf("--ref-format=%s", refFormat)},
			},
			Args: []string{bundlePath, repoPath},
		},
		gitcmd.WithStderr(&cloneErr),
		gitcmd.WithDisabledHooks(),
	)
	if err != nil {
		return fmt.Errorf("spawning git-clone: %w", err)
	}

	if err := cloneCmd.Wait(); err != nil {
		return structerr.New("waiting for git-clone: %w", err).WithMetadata("stderr", cloneErr.String())
	}

	objectHash, err := repo.ObjectHash(ctx)
	if err != nil {
		return fmt.Errorf("detecting object hash: %w", err)
	}

	// When cloning a repository, the remote repository is automatically set up as "origin". As this
	// is unnecessary, remove the configured "origin" remote.
	var remoteErr bytes.Buffer
	remoteCmd, err := repo.Exec(ctx,
		gitcmd.Command{
			Name: "remote",
			Args: []string{"remove", "origin"},
		},
		gitcmd.WithStderr(&remoteErr),
		gitcmd.WithRefTxHook(objectHash, repo),
	)
	if err != nil {
		return fmt.Errorf("spawning git-remote: %w", err)
	}

	if err := remoteCmd.Wait(); err != nil {
		return structerr.New("waiting for git-remote: %w", err).WithMetadata("stderr", remoteErr.String())
	}

	return nil
}

// FetchBundleOpts are optional configurations used when fetching from a bundle.
type FetchBundleOpts struct {
	// UpdateHead updates HEAD based on the HEAD object ID in the bundle file,
	// if available.
	UpdateHead bool
}

// FetchBundle fetches references from a bundle. Refs will be mirrored to the
// repository with the refspec "+refs/*:refs/*".
func (repo *Repo) FetchBundle(ctx context.Context, txManager transaction.Manager, reader io.Reader, opts *FetchBundleOpts) error {
	if opts == nil {
		opts = &FetchBundleOpts{}
	}

	bundlePath, err := repo.createTempBundle(ctx, reader)
	if err != nil {
		return fmt.Errorf("fetch bundle: %w", err)
	}

	fetchConfig := []gitcmd.ConfigPair{
		{Key: "remote.inmemory.url", Value: bundlePath},
		{Key: "remote.inmemory.fetch", Value: git.MirrorRefSpec},
	}
	fetchOpts := FetchOpts{
		CommandOptions: []gitcmd.CmdOpt{
			gitcmd.WithConfigEnv(fetchConfig...),
			// Starting in Git version 2.46.0, executing git-fetch(1) on a bundle performs fsck
			// checks when `transfer.fsckObjects` is enabled. Prior to this, this configuration was
			// always ignored and fsck checks were not run. Unfortunately, fsck message severity
			// configuration is ignored by Git only for bundle fetches. Until this is supported by
			// Git, disable `transfer.fsckObjects` so bundles containing fsck errors can continue to
			// be fetched. This matches behavior prior to Git version 2.46.0.
			gitcmd.WithConfig(gitcmd.ConfigPair{Key: "transfer.fsckObjects", Value: "false"}),
		},
	}
	if err := repo.FetchRemote(ctx, "inmemory", fetchOpts); err != nil {
		return fmt.Errorf("fetch bundle: %w", err)
	}

	if opts.UpdateHead {
		if err := repo.updateHeadFromBundle(ctx, txManager, bundlePath); err != nil {
			return fmt.Errorf("fetch bundle: %w", err)
		}
	}

	return nil
}

// createTempBundle copies reader onto the filesystem so that a path can be
// passed to git. git-fetch does not support streaming a bundle over a pipe.
func (repo *Repo) createTempBundle(ctx context.Context, reader io.Reader) (bundlPath string, returnErr error) {
	tmpDir, err := tempdir.New(ctx, repo.GetStorageName(), repo.logger, repo.locator)
	if err != nil {
		return "", fmt.Errorf("create temp bundle: %w", err)
	}

	bundlePath := filepath.Join(tmpDir.Path(), "repo.bundle")

	file, err := os.Create(bundlePath)
	if err != nil {
		return "", fmt.Errorf("create temp bundle: %w", err)
	}
	defer func() {
		if err := file.Close(); err != nil && returnErr == nil {
			returnErr = fmt.Errorf("create temp bundle: %w", err)
		}
	}()

	if _, err = io.Copy(file, reader); err != nil {
		return "", fmt.Errorf("create temp bundle: %w", err)
	}

	return bundlePath, nil
}

// updateHeadFromBundle updates HEAD from a bundle file
func (repo *Repo) updateHeadFromBundle(ctx context.Context, txManager transaction.Manager, bundlePath string) error {
	head, err := repo.findBundleHead(ctx, bundlePath)
	if err != nil {
		return fmt.Errorf("update head from bundle: %w", err)
	}
	if head == nil {
		return nil
	}

	branch, err := repo.GuessHead(ctx, *head)
	if err != nil {
		return fmt.Errorf("update head from bundle: %w", err)
	}

	if err := repo.SetDefaultBranch(ctx, txManager, branch); err != nil {
		return fmt.Errorf("update head from bundle: %w", err)
	}
	return nil
}

// findBundleHead tries to extract HEAD and its target from a bundle. Returns
// nil when HEAD is not found.
func (repo *Repo) findBundleHead(ctx context.Context, bundlePath string) (*git.Reference, error) {
	cmd, err := repo.Exec(ctx, gitcmd.Command{
		Name:   "bundle",
		Action: "list-heads",
		Args:   []string{bundlePath, "HEAD"},
	}, gitcmd.WithSetupStdout())
	if err != nil {
		return nil, fmt.Errorf("find bundle HEAD: %w", err)
	}
	decoder := gitcmd.NewShowRefDecoder(cmd)
	for {
		var ref git.Reference
		err := decoder.Decode(&ref)
		if errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			return nil, fmt.Errorf("find bundle HEAD: %w", err)
		}
		if ref.Name != "HEAD" {
			continue
		}
		return &ref, nil
	}
	return nil, nil
}
