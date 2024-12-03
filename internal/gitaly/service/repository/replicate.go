package repository

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"gitlab.com/gitlab-org/gitaly/v16/internal/command"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/gitcmd"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/remoterepo"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/repoutil"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/mode"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/storagemgr/partition/migration"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/transaction"
	"gitlab.com/gitlab-org/gitaly/v16/internal/grpc/client"
	"gitlab.com/gitlab-org/gitaly/v16/internal/grpc/metadata"
	"gitlab.com/gitlab-org/gitaly/v16/internal/safe"
	"gitlab.com/gitlab-org/gitaly/v16/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v16/internal/tempdir"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/v16/streamio"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ErrInvalidSourceRepository is returned when attempting to replicate from an invalid source repository.
var ErrInvalidSourceRepository = status.Error(codes.NotFound, "invalid source repository")

// ReplicateRepository replicates data from a source repository to target repository. On the target
// repository, this operation ensures synchronization of the following components:
//
// - Git config
// - Git attributes
// - Custom Git hooks,
// - References and objects
func (s *server) ReplicateRepository(ctx context.Context, in *gitalypb.ReplicateRepositoryRequest) (*gitalypb.ReplicateRepositoryResponse, error) {
	if err := validateReplicateRepository(ctx, s.locator, in); err != nil {
		return nil, structerr.NewInvalidArgument("%w", err)
	}

	if err := s.locator.ValidateRepository(ctx, in.GetRepository()); err != nil {
		repoPath, err := s.locator.GetRepoPath(ctx, in.GetRepository(), storage.WithRepositoryVerificationSkipped())
		if err != nil {
			return nil, structerr.NewInternal("%w", err)
		}

		if err = s.create(ctx, in, repoPath); err != nil {
			if errors.Is(err, ErrInvalidSourceRepository) {
				return nil, ErrInvalidSourceRepository
			}

			return nil, structerr.NewInternal("%w", err)
		}
	}

	repoClient, err := s.newRepoClient(ctx, in.GetSource().GetStorageName())
	if err != nil {
		return nil, structerr.NewInternal("new client: %w", err)
	}

	// We're checking for repository existence up front such that we can give a conclusive error
	// in case it doesn't. Otherwise, the error message returned to the client would depend on
	// the order in which the sync functions were executed. Most importantly, given that
	// `syncRepository` uses FetchInternalRemote which in turn uses gitaly-ssh, this code path
	// cannot pass up NotFound errors given that there is no communication channel between
	// Gitaly and gitaly-ssh.
	request, err := repoClient.RepositoryExists(ctx, &gitalypb.RepositoryExistsRequest{
		Repository: in.GetSource(),
	})
	if err != nil {
		return nil, structerr.NewInternal("checking for repo existence: %w", err)
	}
	if !request.GetExists() {
		return nil, ErrInvalidSourceRepository
	}

	// The partitioning hint should not be forwarded to other Gitaly nodes as the path is irrelevant for them.
	outgoingCtx := storage.ContextWithoutPartitioningHint(ctx)
	outgoingCtx = metadata.IncomingToOutgoing(outgoingCtx)

	if err := s.replicateRepository(outgoingCtx, in.GetSource(), in.GetRepository()); err != nil {
		return nil, structerr.NewInternal("replicating repository: %w", err)
	}

	return &gitalypb.ReplicateRepositoryResponse{}, nil
}

func (s *server) replicateRepository(ctx context.Context, source, target *gitalypb.Repository) error {
	if err := s.syncGitconfig(ctx, source, target, func(ctx context.Context, path string, content io.Reader) error {
		if err := s.writeFile(ctx, path, content); err != nil {
			return err
		}

		if tx := storage.ExtractTransaction(ctx); tx != nil {
			originalConfigRelativePath, err := filepath.Rel(tx.FS().Root(), path)
			if err != nil {
				return fmt.Errorf("original config relative path: %w", err)
			}

			if err := tx.FS().RecordRemoval(originalConfigRelativePath); err != nil {
				return fmt.Errorf("record old config removal: %w", err)
			}

			if err := tx.FS().RecordFile(originalConfigRelativePath); err != nil {
				return fmt.Errorf("record new config creation: %w", err)
			}
		}

		return nil
	}); err != nil {
		return fmt.Errorf("synchronizing gitconfig: %w", err)
	}

	if err := s.syncReferences(ctx, source, target); err != nil {
		return fmt.Errorf("synchronizing references: %w", err)
	}

	if err := s.syncCustomHooks(ctx, source, target); err != nil {
		return fmt.Errorf("synchronizing custom hooks: %w", err)
	}

	return nil
}

func validateReplicateRepository(ctx context.Context, locator storage.Locator, in *gitalypb.ReplicateRepositoryRequest) error {
	if err := locator.ValidateRepository(ctx, in.GetRepository(), storage.WithSkipRepositoryExistenceCheck()); err != nil {
		return err
	}

	if in.GetSource() == nil {
		return errors.New("source repository cannot be empty")
	}

	if in.GetRepository().GetStorageName() == in.GetSource().GetStorageName() {
		return errors.New("repository and source have the same storage")
	}

	return nil
}

func (s *server) create(ctx context.Context, in *gitalypb.ReplicateRepositoryRequest, repoPath string) error {
	// if the directory exists, remove it
	if _, err := os.Stat(repoPath); err == nil {
		tempDir, err := tempdir.NewWithoutContext(in.GetRepository().GetStorageName(), s.logger, s.locator)
		if err != nil {
			return err
		}

		if err = os.Rename(repoPath, filepath.Join(tempDir.Path(), filepath.Base(repoPath))); err != nil {
			return fmt.Errorf("error deleting invalid repo: %w", err)
		}

		s.logger.WithField("repo_path", repoPath).WarnContext(ctx, "removed invalid repository")
	}

	if err := s.createFromSnapshot(ctx, in.GetSource(), in.GetRepository()); err != nil {
		return fmt.Errorf("could not create repository from snapshot: %w", err)
	}

	return nil
}

func (s *server) createFromSnapshot(ctx context.Context, source, target *gitalypb.Repository) error {
	if err := repoutil.Create(ctx, s.logger, s.locator, s.gitCmdFactory, s.catfileCache, s.txManager, s.repositoryCounter, target, func(repo *gitalypb.Repository) error {
		if err := s.extractSnapshot(ctx, source, repo); err != nil {
			return fmt.Errorf("extracting snapshot: %w", err)
		}

		// The archive extracted above does not contain the configuration file. If SHA256 is used, this
		// would lead to an invalid repository as the object format configuration is not present. This
		// leads to failures with transactions as we need to pack the object directory after the repository
		// is created. Sync the config here to ensure the correct object format is configured before
		// returning. We write it directly to disk without voting as the voting is handled by the voting
		// round of the repository creation. We also don't want to record the config creating operation
		// separately as it is already recorded by `repoutil.Create` when it records the entire repository.
		//
		// We only run this with transactions as the file update is not atomic without transactions.
		if tx := storage.ExtractTransaction(ctx); tx != nil {
			if err := s.syncGitconfig(ctx, source, repo, func(ctx context.Context, path string, content io.Reader) (returnedErr error) {
				file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, mode.File)
				if err != nil {
					return fmt.Errorf("open: %w", err)
				}

				defer func() {
					if err := file.Close(); err != nil {
						returnedErr = errors.Join(returnedErr, fmt.Errorf("close: %w", err))
					}
				}()

				if _, err := io.Copy(file, content); err != nil {
					return fmt.Errorf("copy: %w", err)
				}

				return nil
			}); err != nil {
				return fmt.Errorf("sync gitconfig: %w", err)
			}
		}

		return nil
	}); err != nil {
		return fmt.Errorf("creating repository: %w", err)
	}

	if tx := storage.ExtractTransaction(ctx); tx != nil {
		if err := migration.RecordKeyCreation(tx, tx.OriginalRepository(target).GetRelativePath()); err != nil {
			return fmt.Errorf("recording migration key: %w", err)
		}
	}

	return nil
}

func (s *server) extractSnapshot(ctx context.Context, source, target *gitalypb.Repository) error {
	repoClient, err := s.newRepoClient(ctx, source.GetStorageName())
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}

	stream, err := repoClient.GetSnapshot(ctx, &gitalypb.GetSnapshotRequest{Repository: source})
	if err != nil {
		return fmt.Errorf("get snapshot: %w", err)
	}

	// We need to catch a possible 'invalid repository' error from GetSnapshot. On an empty read,
	// BSD tar exits with code 0 so we'd receive the error when waiting for the command. GNU tar on
	// Linux exits with a non-zero code, which causes Go to return an os.ExitError hiding the original
	// error reading from stdin. To get access to the error on both Linux and macOS, we read the first
	// message from the stream here to get access to the possible 'invalid repository' first on both
	// platforms.
	firstBytes, err := stream.Recv()
	if err != nil {
		switch {
		case structerr.GRPCCode(err) == codes.NotFound && strings.Contains(err.Error(), "GetRepoPath: not a git repository:"):
			// The error condition exists for backwards compatibility purposes, only,
			// and can be removed in the next release.
			return ErrInvalidSourceRepository
		case structerr.GRPCCode(err) == codes.NotFound && strings.Contains(err.Error(), storage.ErrRepositoryNotFound.Error()):
			return ErrInvalidSourceRepository
		case structerr.GRPCCode(err) == codes.FailedPrecondition && strings.Contains(err.Error(), storage.ErrRepositoryNotValid.Error()):
			return ErrInvalidSourceRepository
		default:
			return fmt.Errorf("first snapshot read: %w", err)
		}
	}

	snapshotReader := io.MultiReader(
		bytes.NewReader(firstBytes.GetData()),
		streamio.NewReader(func() ([]byte, error) {
			resp, err := stream.Recv()
			return resp.GetData(), err
		}),
	)

	targetPath, err := s.locator.GetRepoPath(ctx, target, storage.WithRepositoryVerificationSkipped())
	if err != nil {
		return fmt.Errorf("target path: %w", err)
	}

	stderr := &bytes.Buffer{}
	cmd, err := command.New(ctx, s.logger, []string{"tar", "-C", targetPath, "-xvf", "-"},
		command.WithStdin(snapshotReader),
		command.WithStderr(stderr),
	)
	if err != nil {
		return fmt.Errorf("create tar command: %w", err)
	}

	if err = cmd.Wait(); err != nil {
		return structerr.New("wait for tar: %w", err).WithMetadata("stderr", stderr)
	}

	return nil
}

func (s *server) syncReferences(ctx context.Context, source, target *gitalypb.Repository) error {
	repo := s.localrepo(target)

	if err := fetchInternalRemote(ctx, s.txManager, s.conns, repo, source); err != nil {
		return fmt.Errorf("fetch internal remote: %w", err)
	}

	return nil
}

func fetchInternalRemote(
	ctx context.Context,
	txManager transaction.Manager,
	conns *client.Pool,
	repo *localrepo.Repo,
	remoteRepoProto *gitalypb.Repository,
) error {
	var stderr bytes.Buffer
	if err := repo.FetchInternal(
		ctx,
		remoteRepoProto,
		[]string{git.MirrorRefSpec},
		localrepo.FetchOpts{
			Prune:  true,
			Stderr: &stderr,
			// By default, Git will fetch any tags that point into the fetched references. This check
			// requires time, and is ultimately a waste of compute because we already mirror all refs
			// anyway, including tags. By adding `--no-tags` we can thus ask Git to skip that and thus
			// accelerate the fetch.
			Tags: localrepo.FetchOptsTagsNone,
			CommandOptions: []gitcmd.CmdOpt{
				gitcmd.WithConfig(gitcmd.ConfigPair{Key: "fetch.negotiationAlgorithm", Value: "skipping"}),
				// Disable the consistency checks of objects fetched into the replicated repository.
				// These fetched objects come from preexisting internal sources, thus it would be
				// problematic for the fetch to fail consistency checks due to altered requirements.
				gitcmd.WithConfig(gitcmd.ConfigPair{Key: "fetch.fsckObjects", Value: "false"}),
			},
		},
	); err != nil {
		if errors.As(err, &localrepo.FetchFailedError{}) {
			return structerr.New("%w", err).WithMetadata("stderr", stderr.String())
		}

		return fmt.Errorf("fetch: %w", err)
	}

	remoteRepo, err := remoterepo.New(ctx, remoteRepoProto, conns)
	if err != nil {
		return structerr.NewInternal("%w", err)
	}

	remoteDefaultBranch, err := remoteRepo.HeadReference(ctx)
	if err != nil {
		return structerr.NewInternal("getting remote default branch: %w", err)
	}

	defaultBranch, err := repo.HeadReference(ctx)
	if err != nil {
		return structerr.NewInternal("getting local default branch: %w", err)
	}

	if defaultBranch != remoteDefaultBranch {
		if err := repo.SetDefaultBranch(ctx, txManager, remoteDefaultBranch); err != nil {
			return structerr.NewInternal("setting default branch: %w", err)
		}
	}

	return nil
}

// syncCustomHooks replicates custom hooks from a source to a target.
func (s *server) syncCustomHooks(ctx context.Context, source, target *gitalypb.Repository) error {
	repoClient, err := s.newRepoClient(ctx, source.GetStorageName())
	if err != nil {
		return fmt.Errorf("creating repo client: %w", err)
	}

	stream, err := repoClient.GetCustomHooks(ctx, &gitalypb.GetCustomHooksRequest{
		Repository: source,
	})
	if err != nil {
		return fmt.Errorf("getting custom hooks: %w", err)
	}

	reader := streamio.NewReader(func() ([]byte, error) {
		request, err := stream.Recv()
		return request.GetData(), err
	})

	if err := repoutil.SetCustomHooks(ctx, s.logger, s.locator, s.txManager, reader, target); err != nil {
		return fmt.Errorf("setting custom hooks: %w", err)
	}

	return nil
}

func (s *server) syncGitconfig(ctx context.Context, source, target *gitalypb.Repository, writeConfig func(ctx context.Context, path string, content io.Reader) error) error {
	repoClient, err := s.newRepoClient(ctx, source.GetStorageName())
	if err != nil {
		return err
	}

	repoPath, err := s.locator.GetRepoPath(ctx, target)
	if err != nil {
		return err
	}

	stream, err := repoClient.GetConfig(ctx, &gitalypb.GetConfigRequest{
		Repository: source,
	})
	if err != nil {
		return err
	}

	configPath := filepath.Join(repoPath, "config")
	return writeConfig(ctx, configPath, streamio.NewReader(func() ([]byte, error) {
		resp, err := stream.Recv()
		return resp.GetData(), err
	}))
}

func (s *server) writeFile(ctx context.Context, path string, reader io.Reader) (returnedErr error) {
	parentDir := filepath.Dir(path)
	if err := os.MkdirAll(parentDir, mode.Directory); err != nil {
		return err
	}

	lockedFile, err := safe.NewLockingFileWriter(path, safe.LockingFileWriterConfig{
		FileWriterConfig: safe.FileWriterConfig{
			FileMode: mode.File,
		},
	})
	if err != nil {
		return fmt.Errorf("creating file writer: %w", err)
	}
	defer func() {
		if err := lockedFile.Close(); err != nil && returnedErr == nil {
			returnedErr = err
		}
	}()

	if _, err := io.Copy(lockedFile, reader); err != nil {
		return err
	}

	if err := transaction.CommitLockedFile(ctx, s.txManager, lockedFile); err != nil {
		return err
	}

	return nil
}

// newRepoClient creates a new RepositoryClient that talks to the gitaly of the source repository
func (s *server) newRepoClient(ctx context.Context, storageName string) (gitalypb.RepositoryServiceClient, error) {
	gitalyServerInfo, err := storage.ExtractGitalyServer(ctx, storageName)
	if err != nil {
		return nil, err
	}

	conn, err := s.conns.Dial(ctx, gitalyServerInfo.Address, gitalyServerInfo.Token)
	if err != nil {
		return nil, err
	}

	return gitalypb.NewRepositoryServiceClient(conn), nil
}
