package repository

import (
	"context"
	"fmt"

	"gitlab.com/gitlab-org/gitaly/v16/internal/git/gitcmd"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/localrepo"
)

func (s *server) removeOriginInRepo(ctx context.Context, repo *localrepo.Repo) error {
	objectHash, err := repo.ObjectHash(ctx)
	if err != nil {
		return fmt.Errorf("detecting object hash: %w", err)
	}

	cmd, err := s.gitCmdFactory.New(ctx, repo,
		gitcmd.Command{Name: "remote", Args: []string{"remove", "origin"}},
		gitcmd.WithRefTxHook(repo, objectHash))
	if err != nil {
		return fmt.Errorf("remote cmd start: %w", err)
	}
	if err := cmd.Wait(); err != nil {
		return fmt.Errorf("remote cmd wait: %w", err)
	}

	return nil
}
