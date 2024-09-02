package log

import (
	"context"
	"fmt"
	"strings"

	"gitlab.com/gitlab-org/gitaly/v16/internal/command"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/catfile"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/gitcmd"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
)

// LastCommitForPath returns the last commit which modified path.
func LastCommitForPath(
	ctx context.Context,
	gitCmdFactory gitcmd.CommandFactory,
	objectReader catfile.ObjectContentReader,
	repo storage.Repository,
	revision git.Revision,
	path string,
	options *gitalypb.GlobalOptions,
) (*catfile.Commit, error) {
	var stdout strings.Builder
	cmd, err := gitCmdFactory.New(ctx, repo, gitcmd.Command{
		Name:        "log",
		Flags:       []gitcmd.Option{gitcmd.Flag{Name: "--format=%H"}, gitcmd.Flag{Name: "--max-count=1"}},
		Args:        []string{revision.String()},
		PostSepArgs: []string{path},
	}, append(gitcmd.ConvertGlobalOptions(options), gitcmd.WithStdout(&stdout))...)
	if err != nil {
		return nil, err
	}

	if err := cmd.Wait(); err != nil {
		return nil, fmt.Errorf("logging last commit for path: %w", err)
	}

	if stdout.Len() == 0 {
		return nil, catfile.NotFoundError{Revision: fmt.Sprintf("%s:%s", revision, path)}
	}

	commitID, trailer, ok := strings.Cut(stdout.String(), "\n")
	if !ok {
		return nil, fmt.Errorf("expected object ID terminated by newline")
	} else if len(trailer) > 0 {
		return nil, fmt.Errorf("object ID has trailing data")
	}

	return catfile.GetCommit(ctx, objectReader, git.Revision(commitID))
}

// GitLogCommand returns a Command that executes git log with the given the arguments
func GitLogCommand(ctx context.Context, gitCmdFactory gitcmd.CommandFactory, repo storage.Repository, revisions []git.Revision, paths []string, options *gitalypb.GlobalOptions, extraArgs ...gitcmd.Option) (*command.Command, error) {
	args := make([]string, len(revisions))
	for i, revision := range revisions {
		args[i] = revision.String()
	}

	return gitCmdFactory.New(ctx, repo, gitcmd.Command{
		Name:        "log",
		Flags:       append([]gitcmd.Option{gitcmd.Flag{Name: "--pretty=%H"}}, extraArgs...),
		Args:        args,
		PostSepArgs: paths,
	}, append(gitcmd.ConvertGlobalOptions(options), gitcmd.WithSetupStdout())...)
}
