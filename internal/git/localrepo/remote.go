package localrepo

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"

	"gitlab.com/gitlab-org/gitaly/v16/internal/git/gitcmd"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
)

// FetchOptsTags controls what tags needs to be imported on fetch.
type FetchOptsTags string

func (t FetchOptsTags) String() string {
	return string(t)
}

var (
	// FetchOptsTagsDefault enables importing of tags only on fetched branches.
	FetchOptsTagsDefault = FetchOptsTags("")
	// FetchOptsTagsAll enables importing of every tag from the remote repository.
	FetchOptsTagsAll = FetchOptsTags("--tags")
	// FetchOptsTagsNone disables importing of tags from the remote repository.
	FetchOptsTagsNone = FetchOptsTags("--no-tags")
)

// FetchOpts is used to configure invocation of the 'FetchRemote' command.
type FetchOpts struct {
	// Env is a list of env vars to pass to the cmd.
	Env []string
	// CommandOptions is a list of options to use with 'git' command.
	CommandOptions []gitcmd.CmdOpt
	// Prune if set fetch removes any remote-tracking references that no longer exist on the remote.
	// https://git-scm.com/docs/git-fetch#Documentation/git-fetch.txt---prune
	Prune bool
	// Force if set fetch overrides local references with values from remote that's
	// doesn't have the previous commit as an ancestor.
	// https://git-scm.com/docs/git-fetch#Documentation/git-fetch.txt---force
	Force bool
	// Verbose controls how much information is written to stderr. The list of
	// refs updated by the fetch will only be listed if verbose is true.
	// https://git-scm.com/docs/git-fetch#Documentation/git-fetch.txt---quiet
	// https://git-scm.com/docs/git-fetch#Documentation/git-fetch.txt---verbose
	Verbose bool
	// Tags controls whether tags will be fetched as part of the remote or not.
	// https://git-scm.com/docs/git-fetch#Documentation/git-fetch.txt---tags
	// https://git-scm.com/docs/git-fetch#Documentation/git-fetch.txt---no-tags
	Tags FetchOptsTags
	// DryRun, if enabled, performs the `git-fetch(1)` command without updating any references.
	DryRun bool
	// Porcelain controls `git-fetch(1)` command output and when enabled prints output in an
	// easy-to-parse format. By default, `git-fetch(1)` output is suppressed by the `--quiet` flag.
	// Therefore, the Verbose option must also be enabled to receive output.
	Porcelain bool
	// Stdout if set it would be used to redirect stdout stream into it.
	Stdout io.Writer
	// Stderr if set it would be used to redirect stderr stream into it.
	Stderr io.Writer
	// DisableTransactions will disable the reference-transaction hook and atomic transactions.
	DisableTransactions bool
}

// FetchFailedError indicates that the fetch has failed.
type FetchFailedError struct {
	err error
}

// Error returns the error message.
func (e FetchFailedError) Error() string {
	return e.err.Error()
}

// FetchRemote fetches changes from the specified remote. Returns an FetchFailedError error in case
// the fetch itself failed.
func (repo *Repo) FetchRemote(ctx context.Context, remoteName string, opts FetchOpts) error {
	if err := validateNotBlank(remoteName, "remoteName"); err != nil {
		return err
	}

	objectHash, err := repo.ObjectHash(ctx)
	if err != nil {
		return fmt.Errorf("detecting object hash: %w", err)
	}

	var stderr bytes.Buffer
	if opts.Stderr == nil {
		opts.Stderr = &stderr
	}

	commandOptions := []gitcmd.CmdOpt{
		gitcmd.WithEnv(opts.Env...),
		gitcmd.WithStdout(opts.Stdout),
		gitcmd.WithStderr(opts.Stderr),
		gitcmd.WithConfig(gitcmd.ConfigPair{
			// Git is so kind to point out that we asked it to not show forced updates
			// by default, so we need to ask it not to do that.
			Key: "advice.fetchShowForcedUpdates", Value: "false",
		}),
	}
	if opts.DisableTransactions {
		commandOptions = append(commandOptions, gitcmd.WithDisabledHooks())
	} else {
		commandOptions = append(commandOptions, gitcmd.WithRefTxHook(repo, objectHash))
	}
	commandOptions = append(commandOptions, opts.CommandOptions...)

	cmd, err := repo.gitCmdFactory.New(ctx, repo,
		gitcmd.Command{
			Name:  "fetch",
			Flags: opts.buildFlags(),
			Args:  []string{remoteName},
		},
		commandOptions...,
	)
	if err != nil {
		return err
	}

	if err := cmd.Wait(); err != nil {
		return FetchFailedError{errorWithStderr(err, stderr.Bytes())}
	}

	return nil
}

// FetchInternal performs a fetch from an internal Gitaly-hosted repository. Returns an
// FetchFailedError error in case git-fetch(1) failed.
func (repo *Repo) FetchInternal(
	ctx context.Context,
	remoteRepo *gitalypb.Repository,
	refspecs []string,
	opts FetchOpts,
) error {
	if len(refspecs) == 0 {
		return fmt.Errorf("fetch internal called without refspecs")
	}

	var stderr bytes.Buffer
	if opts.Stderr == nil {
		opts.Stderr = &stderr
	}

	objectHash, err := repo.ObjectHash(ctx)
	if err != nil {
		return fmt.Errorf("detecting object hash: %w", err)
	}

	commandOptions := []gitcmd.CmdOpt{
		gitcmd.WithEnv(opts.Env...),
		gitcmd.WithStderr(opts.Stderr),
		gitcmd.WithInternalFetchWithSidechannel(
			&gitalypb.SSHUploadPackWithSidechannelRequest{
				Repository:       remoteRepo,
				GitConfigOptions: []string{"uploadpack.allowAnySHA1InWant=true"},
				GitProtocol:      gitcmd.ProtocolV2,
			},
		),
		gitcmd.WithConfig(gitcmd.ConfigPair{
			// Git is so kind to point out that we asked it to not show forced updates
			// by default, so we need to ask it not to do that.
			Key: "advice.fetchShowForcedUpdates", Value: "false",
		}),
	}

	if opts.DisableTransactions {
		commandOptions = append(commandOptions, gitcmd.WithDisabledHooks())
	} else {
		commandOptions = append(commandOptions, gitcmd.WithRefTxHook(repo, objectHash))
	}
	commandOptions = append(commandOptions, opts.CommandOptions...)

	if err := repo.ExecAndWait(ctx,
		gitcmd.Command{
			Name:  "fetch",
			Flags: opts.buildFlags(),
			Args: append(
				[]string{gitcmd.InternalGitalyURL},
				refspecs...,
			),
		},
		commandOptions...,
	); err != nil {
		return FetchFailedError{errorWithStderr(err, stderr.Bytes())}
	}

	return nil
}

func (opts FetchOpts) buildFlags() []gitcmd.Option {
	flags := []gitcmd.Option{
		// We don't need FETCH_HEAD, and it can potentially be hundreds of megabytes when
		// doing a mirror-sync of repos with huge numbers of references.
		gitcmd.Flag{Name: "--no-write-fetch-head"},
	}

	if !opts.Verbose {
		flags = append(flags, gitcmd.Flag{Name: "--quiet"})
	}

	if opts.Prune {
		flags = append(flags, gitcmd.Flag{Name: "--prune"})
	}

	if opts.Force {
		flags = append(flags, gitcmd.Flag{Name: "--force"})
	}

	if opts.Tags != FetchOptsTagsDefault {
		flags = append(flags, gitcmd.Flag{Name: opts.Tags.String()})
	}

	if !opts.DisableTransactions {
		flags = append(flags, gitcmd.Flag{Name: "--atomic"})
	}

	if opts.DryRun {
		flags = append(flags, gitcmd.Flag{Name: "--dry-run"})
	}

	if opts.Porcelain {
		flags = append(flags, gitcmd.Flag{Name: "--porcelain"})
	}

	// Even if we ask Git to not print any output and to force-update branches it will still
	// compute whether branches have been force-updated only to discard that information again.
	// Let's ask it not to given that this check can be quite expensive.
	if !opts.Verbose && opts.Force {
		flags = append(flags, gitcmd.Flag{Name: "--no-show-forced-updates"})
	}

	return flags
}

func validateNotBlank(val, name string) error {
	if strings.TrimSpace(val) == "" {
		return fmt.Errorf("%w: %q is blank or empty", gitcmd.ErrInvalidArg, name)
	}
	return nil
}

func envGitSSHCommand(cmd string) string {
	return "GIT_SSH_COMMAND=" + cmd
}

// PushOptions are options that can be configured for a push.
type PushOptions struct {
	// SSHCommand is the command line to use for git's SSH invocation. The command line is used
	// as is and must be verified by the caller to be safe.
	SSHCommand string
	// Force decides whether to force push all of the refspecs.
	Force bool
	// Config is the Git configuration which gets passed to the git-push(1) invocation.
	// Configuration is set up via `WithConfigEnv()`, so potential credentials won't be leaked
	// via the command line.
	Config []gitcmd.ConfigPair
}

// Push force pushes the refspecs to the remote.
func (repo *Repo) Push(ctx context.Context, remote string, refspecs []string, options PushOptions) error {
	if len(refspecs) == 0 {
		return errors.New("refspecs to push must be explicitly specified")
	}

	var env []string
	if options.SSHCommand != "" {
		env = append(env, envGitSSHCommand(options.SSHCommand))
	}

	var flags []gitcmd.Option
	if options.Force {
		flags = append(flags, gitcmd.Flag{Name: "--force"})
	}

	stderr := &bytes.Buffer{}
	if err := repo.ExecAndWait(ctx,
		gitcmd.Command{
			Name:  "push",
			Flags: flags,
			Args:  append([]string{remote}, refspecs...),
		},
		gitcmd.WithStderr(stderr),
		gitcmd.WithEnv(env...),
		gitcmd.WithConfigEnv(options.Config...),
	); err != nil {
		return fmt.Errorf("git push: %w, stderr: %q", err, stderr)
	}

	return nil
}
