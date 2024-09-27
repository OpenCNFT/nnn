package partition

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"runtime/trace"
	"strings"

	"gitlab.com/gitlab-org/gitaly/v16/internal/git"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/gitcmd"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v16/internal/structerr"
	"golang.org/x/sync/errgroup"
)

func checkObjects(ctx context.Context, repository *localrepo.Repo, revisions []git.Revision, callback func(revision git.Revision, objectID git.ObjectID) error) error {
	defer trace.StartRegion(ctx, "checkObjects").End()

	eg, ctx := errgroup.WithContext(ctx)
	revisionReader, revisionWriter := io.Pipe()
	eg.Go(func() (returnedErr error) {
		defer revisionWriter.CloseWithError(returnedErr)

		for _, revision := range revisions {
			if _, err := fmt.Fprintln(revisionWriter, revision.String()); err != nil {
				return fmt.Errorf("write revision: %w", err)
			}
		}

		return nil
	})

	resultReader, resultWriter := io.Pipe()
	eg.Go(func() (returnedErr error) {
		defer revisionReader.CloseWithError(returnedErr)
		defer resultWriter.CloseWithError(returnedErr)

		var stderr bytes.Buffer
		if err := repository.ExecAndWait(ctx,
			gitcmd.Command{
				Name: "cat-file",
				Flags: []gitcmd.Option{
					gitcmd.Flag{Name: "--batch-check=%(objectname)"},
					gitcmd.Flag{Name: "--buffer"},
				},
			},
			gitcmd.WithStdin(revisionReader),
			gitcmd.WithStdout(resultWriter),
			gitcmd.WithStderr(&stderr),
		); err != nil {
			return structerr.New("cat-file: %w", err).WithMetadata("stderr", stderr.String())
		}

		return nil
	})

	eg.Go(func() (returnedErr error) {
		defer resultReader.CloseWithError(returnedErr)

		objectHash, err := repository.ObjectHash(ctx)
		if err != nil {
			return fmt.Errorf("object hash: %w", err)
		}

		scanner := bufio.NewScanner(resultReader)
		for i := 0; scanner.Scan(); i++ {
			oid := objectHash.ZeroOID
			if rawOID, isMissing := strings.CutSuffix(scanner.Text(), " missing"); !isMissing {
				oid, err = objectHash.FromHex(rawOID)
				if err != nil {
					return fmt.Errorf("parse object id: %w", err)
				}
			}

			if err := callback(revisions[i], oid); err != nil {
				return fmt.Errorf("callback: %w", err)
			}
		}

		if err := scanner.Err(); err != nil {
			return fmt.Errorf("scanning cat-file output: %w", err)
		}

		return nil
	})

	return eg.Wait()
}
