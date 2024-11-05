package housekeeping

import (
	gs "cloud.google.com/go/storage"
	"context"
	"fmt"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/gitcmd"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/housekeeping/config"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/localrepo"
	"io"
	"io/fs"
	"os"
	"path/filepath"
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
	client, err := gs.NewClient(ctx)

	if err != nil {
		return err
	}

	bucketClient := client.Bucket(bucket)

	fileToUpload := make([]string, 0)
	err = filepath.Walk(uploadFrom, func(path string, info fs.FileInfo, err error) error {
		if err != nil {
			//m.logger.Error(fmt.Sprintf("prevent panic by handling failure accessing a path %q: %v\n", path, err))
			return err
		}
		if !info.IsDir() {
			fileToUpload = append(fileToUpload, path)
		}

		return nil
	})

	for _, path := range fileToUpload {
		fmt.Println("I want to upload " + path)
		if err := uploadObject(ctx, bucketClient, path, bucketPrefix); err != nil {
			return err
		}
	}

	err = client.Close()

	return err

}

// DownloadOffloadingStorage downloads the files from offloading storage
func DownloadOffloadingStorage(ctx context.Context, bucket string) error {
	return nil
}

// RemoveFromOffloadingStorage removes files from offloading storage
func RemoveFromOffloadingStorage() error {
	return nil
}

func uploadObject(ctx context.Context, bucketClient *gs.BucketHandle, path string, bucketPrefix string) error {

	file, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open file: %v", err)
	}
	objectPath := fmt.Sprintf("%s/%s", bucketPrefix, filepath.Base(path))

	wc := bucketClient.Object(objectPath).NewWriter(ctx)

	n, err := io.Copy(wc, file)
	if err != nil {
		return fmt.Errorf("io.Copy: %v", err)
	}

	if err := wc.Close(); err != nil {
		return fmt.Errorf("Writer.Close: %v", err)
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("File.Close: %v", err)
	}

	return err
}
