package repository

import (
	"context"
	"fmt"

	"gitlab.com/gitlab-org/gitaly/v16/internal/git/gitcmd"
)

func (s *server) removeOriginInRepo(ctx context.Context, repository gitcmd.RepositoryExecutor) error {
	cmd, err := repository.Exec(ctx, gitcmd.Command{Name: "remote", Args: []string{"remove", "origin"}}, gitcmd.WithRefTxHook(repository))
	if err != nil {
		return fmt.Errorf("remote cmd start: %w", err)
	}
	if err := cmd.Wait(); err != nil {
		return fmt.Errorf("remote cmd wait: %w", err)
	}

	return nil
}
