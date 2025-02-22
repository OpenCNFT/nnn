package backup

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"strings"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/sirupsen/logrus"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/catfile"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/gitcmd"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/counter"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/transaction"
	"gitlab.com/gitlab-org/gitaly/v16/internal/grpc/client"
	"gitlab.com/gitlab-org/gitaly/v16/internal/log"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	// ErrSkipped means the repository was skipped because there was nothing to backup
	ErrSkipped = errors.New("repository skipped")
	// ErrDoesntExist means that the data was not found.
	ErrDoesntExist = errors.New("doesn't exist")

	backupLatency = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name: "gitaly_backup_latency_seconds",
			Help: "Latency of a repository backup by phase",
		},
		[]string{"phase"})
	backupBundleSize = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "gitaly_backup_bundle_bytes",
			Help:    "Size of a Git bundle uploaded in a backup",
			Buckets: prometheus.ExponentialBucketsRange(1, 10*math.Pow(1024, 3), 20), // up to 10GB
		})
)

// Backup represents all the information needed to restore a backup for a repository
type Backup struct {
	// ID is the identifier that uniquely identifies the backup for this repository.
	ID string `toml:"-"`
	// Repository is the repository being backed up.
	Repository storage.Repository `toml:"-"`
	// Empty is true if the repository is empty
	Empty bool `toml:"empty"`
	// NonExistent is true if the repository does not exist.
	// Any Project/Non-Project that doesn't have any repository initialised can be considered as non-existent.
	NonExistent bool `toml:"non_existent"`
	// Steps are the ordered list of steps required to restore this backup
	Steps []Step `toml:"steps"`
	// ObjectFormat is the name of the object hash used by the repository.
	ObjectFormat string `toml:"object_format"`
	// HeadReference is the reference that HEAD points to.
	HeadReference string `toml:"head_reference,omitempty"`
}

// Step represents an incremental step that makes up a complete backup for a repository
type Step struct {
	// BundlePath is the path of the bundle
	BundlePath string `toml:"bundle_path,omitempty"`
	// RefPath is the path of the ref file
	RefPath string `toml:"ref_path,omitempty"`
	// PreviousRefPath is the path of the previous ref file
	PreviousRefPath string `toml:"previous_ref_path,omitempty"`
	// CustomHooksPath is the path of the custom hooks archive
	CustomHooksPath string `toml:"custom_hooks_path,omitempty"`
}

// Locator finds sink backup paths for repositories
type Locator interface {
	// BeginFull returns the tentative backup paths needed to create a full backup.
	BeginFull(ctx context.Context, repo storage.Repository, backupID string) *Backup

	// BeginIncremental returns the backup with the last element of Steps being
	// the tentative step needed to create an incremental backup.
	BeginIncremental(ctx context.Context, repo storage.Repository, backupID string) (*Backup, error)

	// Commit persists the backup so that it can be looked up by FindLatest. It
	// is expected that the last element of Steps will be the newly created
	// backup.
	Commit(ctx context.Context, backup *Backup) error

	// FindLatest returns the latest backup that was written by Commit
	FindLatest(ctx context.Context, repo storage.Repository) (*Backup, error)

	// Find returns the repository backup at the given backupID. If the backup does
	// not exist then the error ErrDoesntExist is returned.
	Find(ctx context.Context, repo storage.Repository, backupID string) (*Backup, error)
}

// Repository abstracts git access required to make a repository backup
type Repository interface {
	// ListRefs returns an iterator to fetch the full set of refs and targets for the repository.
	ListRefs(ctx context.Context) (RefIterator, error)
	// GetCustomHooks fetches the custom hooks archive.
	GetCustomHooks(ctx context.Context, out io.Writer) error
	// CreateBundle fetches a bundle that contains refs matching patterns. When
	// patterns is nil all refs are bundled.
	CreateBundle(ctx context.Context, out io.Writer, patterns io.Reader) error
	// Remove removes the repository. Does not return an error if the
	// repository cannot be found.
	Remove(ctx context.Context) error
	// Create creates the repository.
	Create(ctx context.Context, hash git.ObjectHash, defaultBranch string) error
	// FetchBundle fetches references from a bundle. Refs will be mirrored to
	// the repository.
	FetchBundle(ctx context.Context, reader io.Reader, updateHead bool) error
	// SetCustomHooks updates the custom hooks for the repository.
	SetCustomHooks(ctx context.Context, reader io.Reader) error
	// ObjectHash detects the object hash used by the repository.
	ObjectHash(ctx context.Context) (git.ObjectHash, error)
	// HeadReference fetches the reference pointed to by HEAD.
	HeadReference(ctx context.Context) (git.ReferenceName, error)
	// ResetRefs attempts to reset the list of refs in the repository to match the
	// specified refs slice. This can fail if objects pointed to by a ref no longer
	// exists in the repository. The list of refs should not include the symbolic
	// HEAD reference.
	ResetRefs(ctx context.Context, refs []git.Reference) error
	// SetHeadReference sets the symbolic HEAD reference of the repository to the
	// given target, for example a branch name.
	SetHeadReference(ctx context.Context, target git.ReferenceName) error
}

// ResolveLocator returns a locator implementation based on a locator identifier.
func ResolveLocator(layout string, sink *Sink) (Locator, error) {
	var locator Locator = LegacyLocator{}

	switch layout {
	case "legacy":
	case "pointer":
		locator = PointerLocator{
			Sink:     sink,
			Fallback: locator,
		}
	case "manifest":
		locator = nil
	default:
		return nil, fmt.Errorf("unknown layout: %q", layout)
	}

	locator = NewManifestLocator(sink, locator)

	return locator, nil
}

// Manager manages process of the creating/restoring backups.
type Manager struct {
	sink    *Sink
	locator Locator
	logger  log.Logger

	// repositoryFactory returns an abstraction over git repositories in order
	// to create and restore backups.
	repositoryFactory func(ctx context.Context, repo *gitalypb.Repository, server storage.ServerInfo) (Repository, error)
}

// NewManager creates and returns initialized *Manager instance.
func NewManager(sink *Sink, logger log.Logger, locator Locator, pool *client.Pool) *Manager {
	return &Manager{
		sink:    sink,
		locator: locator,
		repositoryFactory: func(ctx context.Context, repo *gitalypb.Repository, server storage.ServerInfo) (Repository, error) {
			if err := setContextServerInfo(ctx, &server, repo.GetStorageName()); err != nil {
				return nil, err
			}

			conn, err := pool.Dial(ctx, server.Address, server.Token)
			if err != nil {
				return nil, err
			}

			return NewRemoteRepository(repo, conn), nil
		},
		logger: logger,
	}
}

// NewManagerLocal creates and returns a *Manager instance for operating on local repositories.
func NewManagerLocal(
	sink *Sink,
	logger log.Logger,
	locator Locator,
	storageLocator storage.Locator,
	gitCmdFactory gitcmd.CommandFactory,
	catfileCache catfile.Cache,
	txManager transaction.Manager,
	repoCounter *counter.RepositoryCounter,
) *Manager {
	return &Manager{
		sink:    sink,
		locator: locator,
		repositoryFactory: func(ctx context.Context, repo *gitalypb.Repository, server storage.ServerInfo) (Repository, error) {
			localRepo := localrepo.New(logger, storageLocator, gitCmdFactory, catfileCache, repo)

			return NewLocalRepository(logger, storageLocator, gitCmdFactory, txManager, repoCounter, catfileCache, localRepo), nil
		},
		logger: logger,
	}
}

// streamRefs streams the full set of refs into the backup sink.
func (mgr *Manager) streamRefs(ctx context.Context, repo Repository, path string) (refCount int, returnErr error) {
	timer := prometheus.NewTimer(backupLatency.WithLabelValues("refs"))
	defer timer.ObserveDuration()

	w, err := mgr.sink.GetWriter(ctx, path)
	if err != nil {
		return refCount, fmt.Errorf("stream refs: get writer: %w", err)
	}
	defer func() {
		if err := w.Close(); err != nil {
			returnErr = errors.Join(returnErr, fmt.Errorf("stream refs: close writer: %w", err))
		}
	}()

	iterator, err := repo.ListRefs(ctx)
	if err != nil {
		return refCount, fmt.Errorf("stream refs: %w", err)
	}
	defer func() {
		if err := iterator.Close(); err != nil {
			returnErr = errors.Join(returnErr, fmt.Errorf("stream refs: close iterator: %w", err))
		}
	}()

	for iterator.Next() {
		ref := iterator.Ref()
		_, err = fmt.Fprintf(w, "%s %s\n", ref.Target, ref.Name)
		if err != nil {
			return refCount, fmt.Errorf("stream refs: write ref: %w", err)
		}

		refCount++
	}
	if err := iterator.Err(); err != nil && !errors.Is(err, io.EOF) {
		return refCount, fmt.Errorf("stream refs: ref iterator: %w", err)
	}

	return refCount, nil
}

// Create creates a repository backup.
func (mgr *Manager) Create(ctx context.Context, req *CreateRequest) error {
	if req.VanityRepository == nil {
		req.VanityRepository = req.Repository
	}
	repo, err := mgr.repositoryFactory(ctx, req.Repository, req.Server)
	if err != nil {
		return fmt.Errorf("manager: %w", err)
	}

	var backup *Backup
	if req.Incremental {
		var err error
		backup, err = mgr.locator.BeginIncremental(ctx, req.VanityRepository, req.BackupID)
		if err != nil {
			return fmt.Errorf("manager: %w", err)
		}
	} else {
		backup = mgr.locator.BeginFull(ctx, req.VanityRepository, req.BackupID)
	}

	hash, err := repo.ObjectHash(ctx)
	switch {
	case status.Code(err) == codes.NotFound:
		backup.NonExistent = true
		backup.Empty = true
		return mgr.locator.Commit(ctx, backup)

	case err != nil:
		return fmt.Errorf("manager: %w", err)
	}

	backup.ObjectFormat = hash.Format

	headRef, err := repo.HeadReference(ctx)
	if err != nil {
		return fmt.Errorf("manager: %w", err)
	}
	backup.HeadReference = headRef.String()

	step := &backup.Steps[len(backup.Steps)-1]
	refCount, err := mgr.streamRefs(ctx, repo, step.RefPath)
	if err != nil {
		return fmt.Errorf("manager: %w", err)
	}

	if refCount > 0 {
		if err := mgr.writeBundle(ctx, repo, step); err != nil {
			return fmt.Errorf("manager: %w", err)
		}
	} else {
		backup.Empty = true
	}

	if err := mgr.writeCustomHooks(ctx, repo, step.CustomHooksPath); err != nil {
		return fmt.Errorf("manager: %w", err)
	}

	if err := mgr.locator.Commit(ctx, backup); err != nil {
		return fmt.Errorf("manager: %w", err)
	}

	return nil
}

// Restore restores a repository from a backup. If req.BackupID is empty, the
// latest backup will be used.
func (mgr *Manager) Restore(ctx context.Context, req *RestoreRequest) error {
	if req.VanityRepository == nil {
		req.VanityRepository = req.Repository
	}

	repo, err := mgr.repositoryFactory(ctx, req.Repository, req.Server)
	if err != nil {
		return fmt.Errorf("manager: %w", err)
	}

	var backup *Backup
	if req.BackupID == "" {
		backup, err = mgr.locator.FindLatest(ctx, req.VanityRepository)
		switch {
		case errors.Is(err, ErrDoesntExist):
			return removeRepository(ctx, repo, err)
		case err != nil:
			return fmt.Errorf("manager: %w", err)
		}
	} else {
		backup, err = mgr.locator.Find(ctx, req.VanityRepository, req.BackupID)
		switch {
		case errors.Is(err, ErrDoesntExist):
			return fmt.Errorf("manager: %w: %w", ErrDoesntExist, err)
		case err != nil:
			return fmt.Errorf("manager: %w", err)
		}
	}

	if len(backup.Steps) == 0 {
		return fmt.Errorf("manager: no backup steps")
	}

	switch {
	case backup.NonExistent:
		return nil // Nothing to restore for non-existent repositories
	case backup.Empty:
		if _, err := recreateRepo(ctx, repo, backup); err != nil {
			return fmt.Errorf("manager: recreate empty repo: %w", err)
		}

	}

	// Git bundles can not be created for empty repositories.
	if !backup.Empty {
		// Restore Git objects, potentially from increments.
		if err := mgr.restoreFromRefs(ctx, repo, backup); err != nil {
			mgr.logger.WithFields(log.Fields{
				"storage":       req.Repository.GetStorageName(),
				"relative_path": req.Repository.GetRelativePath(),
				"backup_id":     backup.ID,
				logrus.ErrorKey: err,
			}).Warn("unable to reset refs. Proceeding with a normal restore")

			// If we can't reset the refs, perform a full restore by recreating the repo and cloning from the bundle.
			if err := mgr.restoreFromBundle(ctx, repo, backup); err != nil {
				return fmt.Errorf("manager: restore from bundle: %w", err)
			}
		}
	}

	// Restore custom hooks. Each custom hooks archive contains the entirety of the hooks, so
	// we can just restore the most recent archive.
	latestStep := backup.Steps[len(backup.Steps)-1]
	return mgr.restoreCustomHooks(ctx, repo, latestStep.CustomHooksPath)
}

func (mgr *Manager) restoreFromRefs(ctx context.Context, repo Repository, backup *Backup) error {
	latestStep := backup.Steps[len(backup.Steps)-1]
	refs, err := mgr.readRefs(ctx, latestStep.RefPath)
	if err != nil {
		return fmt.Errorf("read refs from backup: %w", err)
	}
	if len(refs) == 0 {
		return errors.New("no refs in backup")
	}

	// Reset all refs except for HEAD.
	if err := repo.ResetRefs(ctx, refs); err != nil {
		return fmt.Errorf("reset refs: %w", err)
	}

	// Explicitly reset HEAD to the default branch tracked by the manifest if available. In a
	// bundle restore, this would've been done during repository creation.
	headRef := git.ReferenceName(backup.HeadReference)
	if headRef == "" {
		return errors.New("expected HEAD to be a symbolic reference")
	}

	return repo.SetHeadReference(ctx, headRef)
}

func (mgr *Manager) restoreFromBundle(ctx context.Context, repo Repository, backup *Backup) error {
	defaultBranchKnown, err := recreateRepo(ctx, repo, backup)
	if err != nil {
		return fmt.Errorf("recreate repo: %w", err)
	}

	for _, step := range backup.Steps {
		refs, err := mgr.readRefs(ctx, step.RefPath)
		if err != nil {
			return fmt.Errorf("read refs: %w", err)
		}

		// In case old manifest gets loaded and since empty repository
		// backups do not contain a bundle, skip bundle restoration.
		// TODO: Remove refs check after couple of releases.
		if len(refs) > 0 {
			if err := mgr.restoreBundle(ctx, repo, step.BundlePath, !defaultBranchKnown); err != nil {
				return fmt.Errorf("restore bundle: %w", err)
			}
		}
	}

	return nil
}

func removeRepository(ctx context.Context, repo Repository, cause error) error {
	if err := repo.Remove(ctx); err != nil {
		return fmt.Errorf("manager: repository removed: %w", err)
	}

	return cause
}

// setContextServerInfo overwrites server with gitaly connection info from ctx metadata when server is zero.
func setContextServerInfo(ctx context.Context, server *storage.ServerInfo, storageName string) error {
	if !server.Zero() {
		return nil
	}

	var err error
	*server, err = storage.ExtractGitalyServer(ctx, storageName)
	if err != nil {
		return fmt.Errorf("set context server info: %w", err)
	}

	return nil
}

func (mgr *Manager) writeBundle(ctx context.Context, repo Repository, step *Step) (returnErr error) {
	timer := prometheus.NewTimer(backupLatency.WithLabelValues("bundle"))
	defer timer.ObserveDuration()

	var patterns io.Reader
	if len(step.PreviousRefPath) > 0 {
		// If there is a previous ref path, then we are creating an increment
		negatedRefs, err := mgr.negatedKnownRefs(ctx, step)
		if err != nil {
			return fmt.Errorf("write bundle: %w", err)
		}
		defer func() {
			if err := negatedRefs.Close(); err != nil && returnErr == nil {
				returnErr = fmt.Errorf("write bundle: %w", err)
			}
		}()

		patterns = io.MultiReader(strings.NewReader("--all\n"), negatedRefs)
	}

	w := NewLazyWriter(func() (io.WriteCloser, error) {
		return mgr.sink.GetWriter(ctx, step.BundlePath)
	})
	defer func() {
		backupBundleSize.Observe(float64(w.BytesWritten()))
		if err := w.Close(); err != nil && returnErr == nil {
			returnErr = fmt.Errorf("write bundle: %w", err)
		}
	}()

	if err := repo.CreateBundle(ctx, w, patterns); err != nil {
		if errors.Is(err, localrepo.ErrEmptyBundle) {
			return nil
		}
		return fmt.Errorf("write bundle: %w", err)
	}

	return nil
}

func (mgr *Manager) negatedKnownRefs(ctx context.Context, step *Step) (io.ReadCloser, error) {
	if len(step.PreviousRefPath) == 0 {
		return io.NopCloser(new(bytes.Reader)), nil
	}

	r, w := io.Pipe()

	go func() {
		defer w.Close()

		reader, err := mgr.sink.GetReader(ctx, step.PreviousRefPath)
		if err != nil {
			_ = w.CloseWithError(err)
			return
		}
		defer reader.Close()

		d := gitcmd.NewShowRefDecoder(reader)
		for {
			var ref git.Reference

			if err := d.Decode(&ref); errors.Is(err, io.EOF) {
				break
			} else if err != nil {
				_ = w.CloseWithError(err)
				return
			}

			if _, err := fmt.Fprintf(w, "^%s\n", ref.Target); err != nil {
				_ = w.CloseWithError(err)
				return
			}
		}
	}()

	return r, nil
}

func (mgr *Manager) readRefs(ctx context.Context, path string) ([]git.Reference, error) {
	reader, err := mgr.sink.GetReader(ctx, path)
	if err != nil {
		return nil, fmt.Errorf("read refs: %w", err)
	}
	defer reader.Close()

	var refs []git.Reference

	d := gitcmd.NewShowRefDecoder(reader)
	for {
		var ref git.Reference

		if err := d.Decode(&ref); errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			return refs, fmt.Errorf("read refs: %w", err)
		}

		// HEAD is tracked as a symbolic reference in the backup manifest and will be restored separately.
		if ref.Name == "HEAD" {
			continue
		}

		refs = append(refs, ref)
	}

	return refs, nil
}

func (mgr *Manager) restoreBundle(ctx context.Context, repo Repository, path string, updateHead bool) error {
	reader, err := mgr.sink.GetReader(ctx, path)
	if err != nil {
		return fmt.Errorf("restore bundle: %q: %w", path, err)
	}
	defer reader.Close()

	if err := repo.FetchBundle(ctx, reader, updateHead); err != nil {
		return fmt.Errorf("restore bundle: %q: %w", path, err)
	}
	return nil
}

func (mgr *Manager) writeCustomHooks(ctx context.Context, repo Repository, path string) (returnErr error) {
	timer := prometheus.NewTimer(backupLatency.WithLabelValues("custom_hooks"))
	defer timer.ObserveDuration()

	w := NewLazyWriter(func() (io.WriteCloser, error) {
		return mgr.sink.GetWriter(ctx, path)
	})
	defer func() {
		if err := w.Close(); err != nil && returnErr == nil {
			returnErr = fmt.Errorf("write custom hooks: %w", err)
		}
	}()
	if err := repo.GetCustomHooks(ctx, w); err != nil {
		return fmt.Errorf("write custom hooks: %w", err)
	}
	return nil
}

func (mgr *Manager) restoreCustomHooks(ctx context.Context, repo Repository, path string) error {
	reader, err := mgr.sink.GetReader(ctx, path)
	if err != nil {
		if errors.Is(err, ErrDoesntExist) {
			return nil
		}
		return fmt.Errorf("restore custom hooks: %w", err)
	}
	defer reader.Close()

	if err := repo.SetCustomHooks(ctx, reader); err != nil {
		return fmt.Errorf("restore custom hooks, %q: %w", path, err)
	}
	return nil
}

func recreateRepo(ctx context.Context, repo Repository, backup *Backup) (bool, error) {
	hash, err := git.ObjectHashByFormat(backup.ObjectFormat)
	if err != nil {
		return false, err
	}

	defaultBranch, defaultBranchKnown := git.ReferenceName(backup.HeadReference).Branch()

	if err := repo.Remove(ctx); err != nil {
		return defaultBranchKnown, err
	}
	if err := repo.Create(ctx, hash, defaultBranch); err != nil {
		return defaultBranchKnown, err
	}

	return defaultBranchKnown, nil
}
