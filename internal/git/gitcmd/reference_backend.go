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

// DetectReferenceBackend detects the reference backend used by the repository.
// If the git version doesn't support `--show-ref-format`, it'll simply echo
// '--show-ref-format'. We fallback to files backend in such situations.
func DetectReferenceBackend(ctx context.Context, gitCmdFactory CommandFactory, repository storage.Repository) (git.ReferenceBackend, error) {
	var stdout, stderr bytes.Buffer

	revParseCmd, err := gitCmdFactory.New(ctx, repository, Command{
		Name: "rev-parse",
		Flags: []Option{
			Flag{"--show-ref-format"},
		},
	}, WithStdout(&stdout), WithStderr(&stderr))
	if err != nil {
		return git.ReferenceBackend{}, fmt.Errorf("spawning rev-parse: %w", err)
	}

	if err := revParseCmd.Wait(); err != nil {
		return git.ReferenceBackend{}, structerr.New("reading reference backend: %w", err).WithMetadata("stderr", stderr.String())
	}

	backend, err := git.ReferenceBackendByName(text.ChompBytes(stdout.Bytes()))
	if err != nil {
		// If we don't know the backend type, let's just fallback to the files backend.
		return git.ReferenceBackendFiles, nil
	}
	return backend, nil
}
