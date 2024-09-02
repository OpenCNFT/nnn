package objectpool

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"

	"gitlab.com/gitlab-org/gitaly/v16/internal/featureflag"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/catfile"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/gitcmd"
	housekeepingmgr "gitlab.com/gitlab-org/gitaly/v16/internal/git/housekeeping/manager"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/transaction"
	"gitlab.com/gitlab-org/gitaly/v16/internal/log"
	"gitlab.com/gitlab-org/gitaly/v16/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
)

// Create creates an object pool for the given source repository. This is done by creating a local
// clone of the source repository where source and target repository will then have the same
// references afterwards.
//
// The source repository will not join the object pool. Thus, its objects won't get deduplicated.
func Create(
	ctx context.Context,
	logger log.Logger,
	locator storage.Locator,
	gitCmdFactory gitcmd.CommandFactory,
	catfileCache catfile.Cache,
	txManager transaction.Manager,
	housekeepingManager housekeepingmgr.Manager,
	proto *gitalypb.ObjectPool,
	sourceRepo *localrepo.Repo,
) (*ObjectPool, error) {
	objectPoolPath, err := locator.GetRepoPath(ctx, proto.GetRepository(), storage.WithRepositoryVerificationSkipped())
	if err != nil {
		return nil, err
	}

	if _, err := os.Stat(objectPoolPath); err == nil {
		return nil, structerr.NewFailedPrecondition("target path exists already").
			WithMetadata("object_pool_path", objectPoolPath)
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, structerr.NewInternal("checking object pool existence: %w", err).
			WithMetadata("object_pool_path", objectPoolPath)
	}

	sourceRepoPath, err := sourceRepo.Path(ctx)
	if err != nil {
		return nil, fmt.Errorf("getting source repository path: %w", err)
	}

	objectHash, err := sourceRepo.ObjectHash(ctx)
	if err != nil {
		return nil, fmt.Errorf("detecting source repo object hash: %w", err)
	}

	refFormat := git.ReferenceBackendFiles.Name
	if featureflag.NewRepoReftableBackend.IsEnabled(ctx) {
		refFormat = git.ReferenceBackendReftables.Name
	}

	var stderr bytes.Buffer
	cmd, err := gitCmdFactory.NewWithoutRepo(ctx,
		gitcmd.Command{
			Name: "clone",
			Flags: []gitcmd.Option{
				gitcmd.Flag{Name: "--quiet"},
				gitcmd.Flag{Name: "--bare"},
				gitcmd.Flag{Name: "--local"},
				gitcmd.Flag{Name: fmt.Sprintf("--ref-format=%s", refFormat)},
			},
			Args: []string{sourceRepoPath, objectPoolPath},
		},
		gitcmd.WithRefTxHook(sourceRepo),
		gitcmd.WithStderr(&stderr),
		// When cloning an empty repository then Git isn't capable to figure out the correct
		// object hash that the new repository needs to use and just uses the default object
		// format. To work around this shortcoming we thus set the default object hash to
		// match the source repository's object hash.
		gitcmd.WithEnv("GIT_DEFAULT_HASH="+objectHash.Format),
	)
	if err != nil {
		return nil, fmt.Errorf("spawning clone: %w", err)
	}

	if err := cmd.Wait(); err != nil {
		return nil, fmt.Errorf("cloning to pool: %w, stderr: %q", err, stderr.String())
	}

	objectPool, err := FromProto(ctx, logger, locator, gitCmdFactory, catfileCache, txManager, housekeepingManager, proto)
	if err != nil {
		return nil, err
	}

	// git-clone(1) writes the remote configuration into the object pool. We nowadays don't have
	// remote configuration in the gitconfig anymore, so let's remove it before returning. Note
	// that we explicitly don't use git-remote(1) to do this, as this command would also remove
	// references part of the remote.
	if err := objectPool.ExecAndWait(ctx, gitcmd.Command{
		Name: "config",
		Flags: []gitcmd.Option{
			gitcmd.Flag{Name: "--remove-section"},
		},
		Args: []string{
			"remote.origin",
		},
	}); err != nil {
		return nil, fmt.Errorf("removing origin remote config: %w", err)
	}

	return objectPool, nil
}
