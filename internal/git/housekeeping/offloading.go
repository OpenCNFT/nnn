package housekeeping

import (
	"context"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	gs "cloud.google.com/go/storage"

	"gitlab.com/gitlab-org/gitaly/v16/internal/git/gitcmd"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/housekeeping/config"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/stats"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/mode"
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
func ResetOffloadingGitConfig(ctx context.Context, repo *localrepo.Repo, praefectTxManager transaction.Manager) error {
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

// PerformRepackingForOffloading performs a full repacking task using the git-repack(1) command
// for the purpose of offloading. It also adds a .promisor file to mark the pack file
// as a promisor pack file, which is used specifically for offloading.
func PerformRepackingForOffloading(ctx context.Context, repo *localrepo.Repo, filter, filterToDir string) error {
	count, err := stats.LooseObjects(ctx, repo)
	if err != nil {
		return fmt.Errorf("count loose objects: %w", err)
	}
	if count > 0 {
		return fmt.Errorf("loose objects when performing repack for offloading")
	}

	repackOpts := []gitcmd.Option{
		// Do a full repack.
		gitcmd.Flag{Name: "-a"},
		// Delete loose objects made redundant by this repack.
		gitcmd.Flag{Name: "-d"},
		// Don't include objects part of alternate.
		gitcmd.Flag{Name: "-l"},

		// Apply the specified filter to determine which objects to include in the repack.
		gitcmd.ValueFlag{Name: "--filter", Value: filter},

		// Relocate the pack files containing objects that match the specified filter to the designated directory.
		// The "pack" serves as a file prefix. The relocated pack files will reside in
		// filterToDir, and their filenames will adhere to the format:
		// pack-<hash>.[pack, idx, rev]
		gitcmd.ValueFlag{Name: "--filter-to", Value: filepath.Join(filterToDir, "pack")},
	}

	repackCfg := config.RepackObjectsConfig{
		WriteBitmap:         false,
		WriteMultiPackIndex: true,
	}

	if err := PerformRepack(ctx, repo, repackCfg, repackOpts...); err != nil {
		return fmt.Errorf("perform repack for offloading: %w", err)
	}

	// Add .promisor file. The .promisor file is used to identify pack files
	// as promisor pack files. This file has no content and serves solely as a
	// marker.
	repoPath, err := repo.Path(ctx)
	if err != nil {
		return fmt.Errorf("getting repository path: %w", err)
	}
	packfilesPath := filepath.Join(repoPath, "objects", "pack")
	entries, err := os.ReadDir(packfilesPath)
	if err != nil {
		return fmt.Errorf("reading packfiles directory: %w", err)
	}

	// The .promisor file should have the same name as the pack file.
	var packFile []string
	for _, entry := range entries {
		entryName := entry.Name()
		if strings.HasPrefix(entryName, "pack-") && strings.HasSuffix(entryName, ".pack") {
			packFile = append(packFile, strings.TrimSuffix(entryName, ".pack"))
		}

	}
	if len(packFile) != 1 {
		return fmt.Errorf("expect just one pack file")
	}
	err = os.WriteFile(filepath.Join(repoPath, "objects", "pack", packFile[0]+".promisor"), []byte{}, mode.File)
	if err != nil {
		return fmt.Errorf("create promisor file: %w", err)
	}

	return nil
}

// UploadToOffloadingStorage uploads all files under fromPath to the offloading storage, i.e. bucket with
// toPrefix
// 1. Move to temp folder
// 2. all upload success, mv from tmp to real
// 3. we need a process to cleanup the tmp folder, files are older than X days
// file are put into a channel, workers take files from chanel, and upload to storage
// error channel is used to address error in worker
// Some logic is binded to GCP feature, genearalzation is considered in futeure iteration if
// the feature is proofed to be of value.
func UploadToOffloadingStorage(ctx context.Context, fromPath, bucket, toPrefix string) error {
	client, err := gs.NewClient(ctx)

	// check no objects with the prefix, depend on rehydration cleanup
	// use _UPLOAD_COMPLETE_ as a marker for upload complete, simulate atomicity
	// if error, delete what have been uploaded to keep best effort
	// worst case, not _UPLOAD_COMPLETE_, async housekeep should delete all the file in this folder

	if err != nil {
		return err
	}

	bucketClient := client.Bucket(bucket)

	fileToUpload := make([]string, 0)
	err = filepath.Walk(fromPath, func(path string, info fs.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			fileToUpload = append(fileToUpload, path)
		}

		return nil
	})

	for _, path := range fileToUpload {
		if err := uploadObject(ctx, bucketClient, path, toPrefix); err != nil {
			return err
		}
	}

	err = client.Close()

	return err
}

func DownloadAndRemoveFromOffloadStorage(fromPrefix string, toPath string) error {
	return nil
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

// We shoudl have retry here
// func doDelete()

// check Empty on prefix/ folder
