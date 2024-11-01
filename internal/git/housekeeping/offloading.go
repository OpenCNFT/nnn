package housekeeping

import (
	"bytes"
	gs "cloud.google.com/go/storage"
	"context"
	"fmt"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/gitcmd"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/housekeeping/config"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/stats"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/transaction"
	"gitlab.com/gitlab-org/gitaly/v16/internal/log"
	"gitlab.com/gitlab-org/gitaly/v16/internal/structerr"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
)

func SetOffloadingGitConfig(ctx context.Context, repo *localrepo.Repo, txManager transaction.Manager,
	url string, filter string) error {

	repo.GetGitAlternateObjectDirectories()
	configMap := map[string]string{
		"remote.offload.url":                url,
		"remote.offload.promisor":           "true",
		"remote.offload.fetch":              "+refs/heads/*:refs/remotes/gsremote/*",
		"remote.offload.partialclonefilter": filter,
	}

	for k, v := range configMap {
		if err := setConfig(ctx, k, v, repo); err != nil {
			return err
		}
	}
	return nil
}

func ResetOffloadingGitConfig(ctx context.Context, repo *localrepo.Repo) error {

	var remoteErr bytes.Buffer
	if err := repo.ExecAndWait(ctx, gitcmd.Command{
		Name: "remote",
		Args: []string{"remove", "offload"},
	}); err != nil {
		// Please refer to https://git-scm.com/docs/git-config#_description
		// on return codes.
		return structerr.New("waiting for git-remote: %w", err).WithMetadata("stderr", remoteErr.String())

	}
	return nil
}

func AddOffloadTransientAlternate(ctx context.Context, repo *localrepo.Repo, storageDir string, originalRepoPath string) error {

	//storageDir = "/Users/peijian/Gitlab/gitlab-development-kit/repositories"
	repoPath := repo.GetRelativePath()

	// Create the file (or truncate it if it already exists)
	file, err := os.Create(filepath.Join(storageDir, repoPath, "objects", "info", "alternates"))
	if err != nil {
		return err
	}
	defer file.Close() // Ensure the file is closed when done

	// Write a string to the file
	content := filepath.Join(storageDir, "offload_transient", originalRepoPath, "objects")
	_, err = file.WriteString(content)
	if err != nil {
		return err
	}

	return nil
}

// RemoveOffloadTransientAlternate
// if transient is the only dir, then this alternates file needs to be removed
// otherwise, remove transient from the alternate file
func RemoveOffloadTransientAlternate(ctx context.Context, repo *localrepo.Repo) error {

	stats.ReadAlternatesFile(repo.GetRelativePath())
	// os.Create can truncate make use of it
	return nil
}

// GetOffloadingRepackOptions returns the flags for repacking without large blobs
func GetOffloadingRepackOptions(filter string, filterTo string) (config.RepackObjectsConfig, []gitcmd.Option) {
	opts := []gitcmd.Option{
		gitcmd.Flag{Name: "-a"},
		gitcmd.Flag{Name: "-d"},
		gitcmd.ValueFlag{Name: "--filter", Value: filter},
		gitcmd.ValueFlag{Name: "--filter-to", Value: filterTo + "/pack"},
	}

	repackCfg := config.RepackObjectsConfig{
		WriteBitmap: false,
	}

	return repackCfg, opts
}

// Upload uploads path to bucket
func UploadToBucket(ctx context.Context, bucket string, bucketPrefix string, uploadFrom string) error {
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

func uploadObject(ctx context.Context, bucketClient *gs.BucketHandle, path string, bucketPrefix string) error {

	file, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open file: %v", err)
	}
	fmt.Println("opening file " + path)
	objectPath := fmt.Sprintf("%s/%s", bucketPrefix, filepath.Base(path))

	wc := bucketClient.Object(objectPath).NewWriter(ctx)

	n, err := io.Copy(wc, file)
	fmt.Println("copying file bytes " + strconv.FormatInt(n, 10))
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

func Move(fromPath string, toPath string) error {
	// Move to a fake path first
	err := filepath.Walk(fromPath, func(fromPath string, info fs.FileInfo, err error) error {
		if err != nil {
			//logger.Error(fmt.Sprintf("prevent panic by handling failure accessing a path %q: %v\n", path, err))
			return err
		}
		if !info.IsDir() {
			newPath := fmt.Sprintf("%s/objects/pack/%s", toPath, filepath.Base(fromPath))
			if err := os.Rename(fromPath, newPath); err != nil {
				return err
			}
		}

		return nil
	})

	return err
}

func Cleanup(path string) error {

	err := os.RemoveAll(path)

	return err
}

func RemoveFromBucket(logger log.Logger, ctx context.Context, bucket string, bucketPrefix string, filesToDelete []string) {

	client, err := gs.NewClient(ctx)
	//var j interface{} = client
	//j.(*http.Client).CloseIdleConnections()

	if err != nil {
		logger.Error(fmt.Errorf("create google storage client %w", err).Error())
	}

	bucketClient := client.Bucket(bucket)
	for _, f := range filesToDelete {
		objectPath := fmt.Sprintf("%s/%s", bucketPrefix, f)
		if err := bucketClient.Object(objectPath).Delete(ctx); err != nil {
			// TODO we can do a retry with channel
			logger.Error(fmt.Errorf("create google storage client %w", err).Error())
		}
	}

}

func setConfig(ctx context.Context, key, value string, repo *localrepo.Repo) (returnedErr error) {
	repoPath, err := repo.Path(ctx)
	if err != nil {
		return err
	}
	configPath := filepath.Join(repoPath, "config")

	if err != nil {
		return fmt.Errorf("creating config writer: %w", err)
	}

	if err := repo.ExecAndWait(ctx, gitcmd.Command{
		Name: "config",
		Flags: []gitcmd.Option{
			gitcmd.Flag{Name: "--replace-all"},
			gitcmd.ValueFlag{Name: "--file", Value: configPath},
		},
		Args: []string{key, value},
	}); err != nil {
		// Please refer to https://git-scm.com/docs/git-config#_description
		// on return codes.
		switch {
		case isExitWithCode(err, 1):
			// section or key is invalid
			return fmt.Errorf("%w: bad section or name", gitcmd.ErrInvalidArg)
		case isExitWithCode(err, 2):
			// no section or name was provided
			return fmt.Errorf("%w: missing section or name", gitcmd.ErrInvalidArg)
		}
	}

	return nil
}
