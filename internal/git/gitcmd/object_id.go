package gitcmd

import (
	"bytes"
	"context"
	"fmt"

	"gitlab.com/gitlab-org/gitaly/v16/internal/git"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v16/internal/helper/text"
	"gitlab.com/gitlab-org/gitaly/v16/internal/structerr"
)

// DetectObjectHash detects the object-hash used by the given repository.
func DetectObjectHash(ctx context.Context, gitCmdFactory CommandFactory, repository storage.Repository) (git.ObjectHash, error) {
	var stdout, stderr bytes.Buffer

	revParseCmd, err := gitCmdFactory.New(ctx, repository, Command{
		Name: "rev-parse",
		Flags: []Option{
			Flag{"--show-object-format"},
		},
	}, WithStdout(&stdout), WithStderr(&stderr))
	if err != nil {
		return git.ObjectHash{}, fmt.Errorf("spawning rev-parse: %w", err)
	}

	if err := revParseCmd.Wait(); err != nil {
		return git.ObjectHash{}, structerr.New("reading object format: %w", err).WithMetadata("stderr", stderr.String())
	}

	return git.ObjectHashByFormat(text.ChompBytes(stdout.Bytes()))
}
