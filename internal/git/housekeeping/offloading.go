package housekeeping

import (
	"context"

	"gitlab.com/gitlab-org/gitaly/v16/internal/git/gitcmd"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/housekeeping/config"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/localrepo"
)

// SetOffloadingGitConfig adds a new promisor remote in the git config of the offloaded repository.
func SetOffloadingGitConfig(ctx context.Context, repo *localrepo.Repo) error {
	return nil
}

// ResetOffloadingGitConfig remotes the promisor remote from the offloaded repository.
func ResetOffloadingGitConfig(ctx context.Context, repo *localrepo.Repo) error {
	return nil
}

// AddOffloadTransientAlternate adds an alternate in the offloaded repository. This alternate
// serves as a cache for the object that download back temporarily.
func AddOffloadTransientAlternate(ctx context.Context, repo *localrepo.Repo) error {
	return nil
}

// RemoveOffloadTransientAlternate removes the transient alternate for offloading from the
// git alternate file.
func RemoveOffloadTransientAlternate(ctx context.Context, repo *localrepo.Repo) error {
	return nil
}

// GetOffloadingRepackOptions returns the flags for  `git-repack(1)` command
func GetOffloadingRepackOptions(filter string, filterTo string) (config.RepackObjectsConfig, []gitcmd.Option) {
	return config.RepackObjectsConfig{}, []gitcmd.Option{}
}

// UploadToOffloadingStorage uploads the files to the offloading storage
func UploadToOffloadingStorage(ctx context.Context, bucket string, bucketPrefix string, uploadFrom string) error {
	return nil
}

// DownloadOffloadingStorage downloads the files from offloading storage
func DownloadOffloadingStorage(ctx context.Context, bucket string) error {
	return nil
}

// RemoveFromOffloadingStorage removes files from offloading storage
func RemoveFromOffloadingStorage() error {
	return nil
}
