package log

import (
	"context"
	"io"

	"gitlab.com/gitlab-org/gitaly/v16/internal/command"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/catfile"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v16/internal/helper/text"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
)

// LastCommitForPath returns the last commit which modified path.
func LastCommitForPath(
	ctx context.Context,
	gitCmdFactory git.CommandFactory,
	objectReader catfile.ObjectContentReader,
	repo storage.Repository,
	revision git.Revision,
	path string,
	options *gitalypb.GlobalOptions,
) (*gitalypb.GitCommit, error) {
	cmd, err := gitCmdFactory.New(ctx, repo, git.Command{
		Name:        "log",
		Flags:       []git.Option{git.Flag{Name: "--format=%H"}, git.Flag{Name: "--max-count=1"}},
		Args:        []string{revision.String()},
		PostSepArgs: []string{path},
	}, append(git.ConvertGlobalOptions(options), git.WithSetupStdout())...)
	if err != nil {
		return nil, err
	}

	commitID, err := io.ReadAll(cmd)
	if err != nil {
		return nil, err
	}

	return catfile.GetCommit(ctx, objectReader, git.Revision(text.ChompBytes(commitID)))
}

// GitLogCommand returns a Command that executes git log with the given the arguments
func GitLogCommand(ctx context.Context, gitCmdFactory git.CommandFactory, repo storage.Repository, revisions []git.Revision, paths []string, options *gitalypb.GlobalOptions, extraArgs ...git.Option) (*command.Command, error) {
	args := make([]string, len(revisions))
	for i, revision := range revisions {
		args[i] = revision.String()
	}

	return gitCmdFactory.New(ctx, repo, git.Command{
		Name:        "log",
		Flags:       append([]git.Option{git.Flag{Name: "--pretty=%H"}}, extraArgs...),
		Args:        args,
		PostSepArgs: paths,
	}, append(git.ConvertGlobalOptions(options), git.WithSetupStdout())...)
}
