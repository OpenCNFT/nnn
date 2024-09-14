package diff

import (
	"context"
	"io"

	"gitlab.com/gitlab-org/gitaly/v16/internal/git/gitcmd"
	"gitlab.com/gitlab-org/gitaly/v16/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/v16/streamio"
)

func (s *server) RawDiff(in *gitalypb.RawDiffRequest, stream gitalypb.DiffService_RawDiffServer) error {
	if err := validateRequest(stream.Context(), s.locator, in); err != nil {
		return structerr.NewInvalidArgument("%w", err)
	}

	subCmd := gitcmd.Command{
		Name:  "diff",
		Flags: []gitcmd.Option{gitcmd.Flag{Name: "--full-index"}},
		Args:  []string{in.LeftCommitId, in.RightCommitId},
	}

	sw := streamio.NewWriter(func(p []byte) error {
		return stream.Send(&gitalypb.RawDiffResponse{Data: p})
	})

	repo := s.localrepo(in.GetRepository())

	return sendRawOutput(stream.Context(), repo, sw, subCmd)
}

func (s *server) RawPatch(in *gitalypb.RawPatchRequest, stream gitalypb.DiffService_RawPatchServer) error {
	if err := validateRequest(stream.Context(), s.locator, in); err != nil {
		return structerr.NewInvalidArgument("%w", err)
	}

	subCmd := gitcmd.Command{
		Name:  "format-patch",
		Flags: []gitcmd.Option{gitcmd.Flag{Name: "--stdout"}, gitcmd.ValueFlag{Name: "--signature", Value: "GitLab"}},
		Args:  []string{in.LeftCommitId + ".." + in.RightCommitId},
	}

	sw := streamio.NewWriter(func(p []byte) error {
		return stream.Send(&gitalypb.RawPatchResponse{Data: p})
	})

	repo := s.localrepo(in.GetRepository())

	return sendRawOutput(stream.Context(), repo, sw, subCmd)
}

func sendRawOutput(ctx context.Context, repo gitcmd.RepositoryExecutor, sender io.Writer, subCmd gitcmd.Command) error {
	cmd, err := repo.Exec(ctx, subCmd, gitcmd.WithSetupStdout())
	if err != nil {
		return structerr.NewInternal("cmd: %w", err)
	}

	if _, err := io.Copy(sender, cmd); err != nil {
		return structerr.NewAborted("send: %w", err)
	}

	return cmd.Wait()
}
