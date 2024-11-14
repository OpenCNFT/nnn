package housekeeping

import (
	"context"
	"fmt"

	"gitlab.com/gitlab-org/gitaly/v16/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/transaction"
)

// OffloadingPromisorRemote is the name of the Git remote used for offloading.
const OffloadingPromisorRemote = "offload"

// SetOffloadingGitConfig updates the Git configuration file to enable a repository for offloading by configuring
// settings specific to establishing a promisor remote.
//
// The praefectTxManager represents the voting-based Praefect transaction manager.
// When this helper function is used within the storage/partition transaction manager,
// praefectTxManager will naturally be nil. This is due to the differing underlying mechanisms
// between the storage/partition transaction manager and the praefectTxManager.
//
// As a result, the logic in repo.SetConfig will be adjusted to handle this scenario.
// This explanation will become obsolete and can be removed in the future.
func SetOffloadingGitConfig(ctx context.Context, repo *localrepo.Repo, url, filter string, praefectTxManager transaction.Manager) error {
	if url == "" {
		return fmt.Errorf("set offloading config: promisor remote url missing")
	}
	if filter == "" {
		return fmt.Errorf("set offloading config: filter missing")
	}

	configMap := map[string]string{
		fmt.Sprintf("remote.%s.url", OffloadingPromisorRemote):                url,
		fmt.Sprintf("remote.%s.promisor", OffloadingPromisorRemote):           "true",
		fmt.Sprintf("remote.%s.fetch", OffloadingPromisorRemote):              fmt.Sprintf("+refs/heads/*:refs/remotes/%s/*", OffloadingPromisorRemote),
		fmt.Sprintf("remote.%s.partialclonefilter", OffloadingPromisorRemote): filter,
	}

	for k, v := range configMap {
		if err := repo.SetConfig(ctx, k, v, praefectTxManager); err != nil {
			return fmt.Errorf("set Git config: %w", err)
		}
	}
	return nil
}

// ResetOffloadingGitConfig is the reverse operation of SetOffloadingGitConfig.
// It removes all Git configuration entries related to the offloading remote.
//
// The praefectTxManager represents the voting-based Praefect transaction manager.
// When this helper function is used within the storage/partition transaction manager,
// praefectTxManager will naturally be nil. This is due to the differing underlying mechanisms
// between the storage/partition transaction manager and the praefectTxManager.
//
// As a result, the logic in repo.SetConfig will be adjusted to handle this scenario.
// This explanation will become obsolete and can be removed in the future.
func ResetOffloadingGitConfig(ctx context.Context, repo *localrepo.Repo,
	praefectTxManager transaction.Manager,
) error {
	// Using regex here is safe as all offloading promisor remote-related configurations
	// are prefixed with "remote.<OffloadingPromisorRemote>". This follows the logic
	// in `SetOffloadingGitConfig`. If any new logic is added to `SetOffloadingGitConfig`
	// in the future, the reset logic should be updated accordingly.
	regex := fmt.Sprintf("remote.%s", OffloadingPromisorRemote)

	if err := repo.UnsetMatchingConfig(ctx, regex, praefectTxManager); err != nil {
		return fmt.Errorf("unset Git config: %w", err)
	}
	return nil
}
>>>>>>> 17840d671 (fixup! housekeeping: Add SetOffloadingGitConfig helper function)
