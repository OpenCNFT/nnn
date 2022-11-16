package updateref

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"regexp"

	"gitlab.com/gitlab-org/gitaly/v15/internal/command"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git"
)

var errTransactionNotStarted = errors.New("transaction hasn't been started")

// ErrAlreadyLocked indicates a reference cannot be locked because another
// process has already locked it.
type ErrAlreadyLocked struct {
	Ref string
}

func (e *ErrAlreadyLocked) Error() string {
	return fmt.Sprintf("reference is already locked: %q", e.Ref)
}

// ErrInvalidReferenceFormat indicates a reference name was invalid.
type ErrInvalidReferenceFormat struct {
	// ReferenceName is the invalid reference name.
	ReferenceName string
}

func (e ErrInvalidReferenceFormat) Error() string {
	return fmt.Sprintf("invalid reference format: %q", e.ReferenceName)
}

// Updater wraps a `git update-ref --stdin` process, presenting an interface
// that allows references to be easily updated in bulk. It is not suitable for
// concurrent use.
//
// The caller needs to start a reference transaction with Start prior to staging
// any changes. The changes are performed only if Commit is called at the end.
// The caller is responsible for closing the Updater once it is finished with it
// by calling Cancel.
type Updater struct {
	repo       git.RepositoryExecutor
	cmd        *command.Command
	stdout     *bufio.Reader
	stderr     *bytes.Buffer
	objectHash git.ObjectHash

	// activeTransaction tracks whether the there is an open
	// transaction going. This is used for erroring on incorrect
	// usage of the updater.
	activeTransaction bool
}

// UpdaterOpt is a type representing options for the Updater.
type UpdaterOpt func(*updaterConfig)

type updaterConfig struct {
	disableTransactions bool
}

// WithDisabledTransactions disables hooks such that no reference-transactions
// are used for the updater.
func WithDisabledTransactions() UpdaterOpt {
	return func(cfg *updaterConfig) {
		cfg.disableTransactions = true
	}
}

// New returns a new bulk updater, wrapping a `git update-ref` process. Call the
// various methods to enqueue updates, then call Commit() to attempt to apply all
// the updates at once.
//
// It is important that ctx gets canceled somewhere. If it doesn't, the process
// spawned by New() may never terminate.
func New(ctx context.Context, repo git.RepositoryExecutor, opts ...UpdaterOpt) (*Updater, error) {
	var cfg updaterConfig
	for _, opt := range opts {
		opt(&cfg)
	}

	txOption := git.WithRefTxHook(repo)
	if cfg.disableTransactions {
		txOption = git.WithDisabledHooks()
	}

	var stderr bytes.Buffer
	cmd, err := repo.Exec(ctx,
		git.SubCmd{
			Name:  "update-ref",
			Flags: []git.Option{git.Flag{Name: "-z"}, git.Flag{Name: "--stdin"}},
		},
		txOption,
		git.WithSetupStdin(),
		git.WithStderr(&stderr),
	)
	if err != nil {
		return nil, err
	}

	objectHash, err := repo.ObjectHash(ctx)
	if err != nil {
		return nil, fmt.Errorf("detecting object hash: %w", err)
	}

	return &Updater{
		repo:       repo,
		cmd:        cmd,
		stderr:     &stderr,
		stdout:     bufio.NewReader(cmd),
		objectHash: objectHash,
	}, nil
}

// Start begins a new reference transaction. The reference changes are not perfromed until Commit
// is explicitly called.
func (u *Updater) Start() error {
	if u.activeTransaction {
		return errors.New("transaction already open")
	}

	u.activeTransaction = true

	return u.setState("start")
}

// Update commands the reference to be updated to point at the object ID specified in newOID. If
// newOID is the zero OID, then the branch will be deleted. If oldOID is a non-empty string, then
// the reference will only be updated if its current value matches the old value. If the old value
// is the zero OID, then the branch must not exist.
//
// A reference transaction must be started before calling Update.
func (u *Updater) Update(reference git.ReferenceName, newOID, oldOID git.ObjectID) error {
	if !u.activeTransaction {
		return errTransactionNotStarted
	}

	_, err := fmt.Fprintf(u.cmd, "update %s\x00%s\x00%s\x00", reference.String(), newOID, oldOID)
	return err
}

// Create commands the reference to be created with the given object ID. The ref must not exist.
//
// A reference transaction must be started before calling Create.
func (u *Updater) Create(reference git.ReferenceName, oid git.ObjectID) error {
	return u.Update(reference, oid, u.objectHash.ZeroOID)
}

// Delete commands the reference to be removed from the repository. This command will ignore any old
// state of the reference and just force-remove it.
//
// A reference transaction must be started before calling Delete.
func (u *Updater) Delete(reference git.ReferenceName) error {
	return u.Update(reference, u.objectHash.ZeroOID, "")
}

var (
	refLockedRegex        = regexp.MustCompile("cannot lock ref '(.+?)'")
	refInvalidFormatRegex = regexp.MustCompile(`invalid ref format: (.*)\\n"`)
)

// Prepare prepares the reference transaction by locking all references and determining their
// current values. The updates are not yet committed and will be rolled back in case there is no
// call to `Commit()`. This call is optional.
func (u *Updater) Prepare() error {
	if !u.activeTransaction {
		return errTransactionNotStarted
	}

	if err := u.setState("prepare"); err != nil {
		matches := refLockedRegex.FindSubmatch([]byte(err.Error()))
		if len(matches) > 1 {
			return &ErrAlreadyLocked{Ref: string(matches[1])}
		}

		matches = refInvalidFormatRegex.FindSubmatch([]byte(err.Error()))
		if len(matches) > 1 {
			return ErrInvalidReferenceFormat{ReferenceName: string(matches[1])}
		}

		return err
	}

	return nil
}

// Commit applies the commands specified in other calls to the Updater. Commit finishes the
// reference transaction and another one must be started before further changes can be staged.
func (u *Updater) Commit() error {
	if !u.activeTransaction {
		return errTransactionNotStarted
	}

	if err := u.setState("commit"); err != nil {
		return err
	}

	u.activeTransaction = false

	return nil
}

// Cancel closes the updater and aborts a possible open transaction. No changes will be written
// to disk, all lockfiles will be cleaned up and the process will exit.
func (u *Updater) Cancel() error {
	if err := u.cmd.Wait(); err != nil {
		return fmt.Errorf("canceling update: %w", err)
	}
	return nil
}

func (u *Updater) setState(state string) error {
	_, err := fmt.Fprintf(u.cmd, "%s\x00", state)
	if err != nil {
		// We need to explicitly cancel the command here and wait for it to terminate such
		// that we can retrieve the command's stderr in a race-free manner.
		_ = u.Cancel()
		return fmt.Errorf("updating state to %q: %w, stderr: %q", state, err, u.stderr)
	}

	// For each state-changing command, git-update-ref(1) will report successful execution via
	// "<command>: ok" lines printed to its stdout. Ideally, we should thus verify here whether
	// the command was successfully executed by checking for exactly this line, otherwise we
	// cannot be sure whether the command has correctly been processed by Git or if an error was
	// raised.
	line, err := u.stdout.ReadString('\n')
	if err != nil {
		// We need to explicitly cancel the command here and wait for it to
		// terminate such that we can retrieve the command's stderr in a race-free
		// manner.
		_ = u.Cancel()

		return fmt.Errorf("state update to %q failed: %w, stderr: %q", state, err, u.stderr)
	}

	if line != fmt.Sprintf("%s: ok\n", state) {
		return fmt.Errorf("state update to %q not successful: expected ok, got %q", state, line)
	}

	return nil
}
