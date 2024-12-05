package housekeeping

import (
	gs "cloud.google.com/go/storage"
	"context"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

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
func UploadToOffloadingStorage(ctx context.Context, client *gs.Client, fromPath, bucket, toPrefix string) error {
	var err error
	//client, err := gs.NewClient(ctx)
	////filesUploaded := make([]string, 0)
	//
	////defer deleteObjects(bucket, filesUploaded)
	//defer client.Close()
	//if err != nil {
	//	return fmt.Errorf("create gs client: %w", err)
	//}
	// TODO is the prefix empty?
	bucketClient := client.Bucket(bucket)
	if bucketClient == nil {
		return fmt.Errorf("bucket client error")
	}

	// check no objects with the prefix, depend on rehydration cleanup
	// use _UPLOAD_COMPLETE_ as a marker for upload complete, simulate atomicity
	// if error, delete what have been uploaded to keep best effort
	// worst case, not _UPLOAD_COMPLETE_, async housekeep should delete all the file in this folder

	//filesUploadingCh := make(chan string)
	//filesUploadingErrCh := make(chan error)

	var filesToUpload []string
	err = filepath.Walk(fromPath, func(path string, info fs.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			filesToUpload = append(filesToUpload, path)
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("walk path %s: %w", fromPath, err)
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	errCh := make(chan error, 1)      // Use a buffered channel to ensure non-blocking writes
	successCh := make(chan string, 1) // Record success info for rollback
	// 3 pack files, each as has a go routine to upload

	var wg sync.WaitGroup
	for _, path := range filesToUpload {
		wg.Add(1)
		go uploadObject(ctx, bucketClient, path, toPrefix, errCh, successCh, &wg)
	}

	done := make(chan struct{})

	var count int
	var errorMsg string
	var successMsg string

	go func() {
		for {
			select {
			case workerErr := <-errCh:
				//fmt.Printf("Received error: %v. Cancelling other workers...\n", workerErr)
				cancel()
				errorMsg = errorMsg + " " + workerErr.Error()
			case mgs := <-successCh:
				count++
				successMsg = successMsg + " " + mgs
			case <-done:
				fmt.Println("All workers completed")
				return
			}
		}
	}()

	wg.Wait()
	close(done)

	fmt.Println("Main: Exiting program")
	//fmt.Printf(errorMsg)
	//fmt.Printf(successMsg)

	return nil

}

// TODO downloading
//func DownloadAndRemoveFromOffloadStorage(fromPrefix string, toPath string) error {
//	return nil
//}

// TODO cleanup all the fmt.print
func uploadObject(ctx context.Context, bucketClient *gs.BucketHandle, path string, bucketPrefix string,
	errCh chan error, sc chan string, wg *sync.WaitGroup) {
	defer wg.Done()

	// Simulate random work duration
	//workDuration := rand.Intn(2) + 1 // Random duration between 1-5 seconds
	resCh := make(chan error)

	// uploading logic
	go func() {
		defer close(resCh)
		file, err := os.Open(path)
		if err != nil {
			resCh <- fmt.Errorf("open file: %v", err)
			return
		}
		fmt.Println("opening file " + path)
		objectPath := fmt.Sprintf("%s/%s", bucketPrefix, filepath.Base(path))

		ctx, cancel := context.WithTimeout(ctx, time.Second*50)
		defer cancel()

		o := bucketClient.Object(objectPath)
		// expect that there is no object with the same name exist
		// prevent race condition
		o = o.If(gs.Conditions{DoesNotExist: true})
		wc := o.NewWriter(ctx)

		n, err := io.Copy(wc, file)
		fmt.Println("copying file bytes " + strconv.FormatInt(n, 10))
		if err != nil {
			resCh <- fmt.Errorf("io.Copy: %v", err)
			return
		}

		if err := wc.Close(); err != nil {
			resCh <- fmt.Errorf("Writer.Close: %v", err)
			return
		}
		if err := file.Close(); err != nil {
			resCh <- fmt.Errorf("File.Close: %v", err)
			return
		}
		resCh <- nil
	}()

	select {
	case err := <-resCh:
		if err != nil {
			errCh <- err
		} else {
			sc <- path
		}

	//case <-time.After(time.Duration(workDuration) * time.Second):
	//	ext := filepath.Ext(path)
	//	if ext == ".error" {
	//		fmt.Printf("Worker %s encountered an error \n", path)
	//		errCh <- fmt.Errorf("worker %s encountered an error", path)
	//	} else {
	//		fmt.Printf("Worker %s Completed successfully \n", path)
	//		sc <- fmt.Sprintf("Worker %s: Completed successfully\n", path)
	//		//fmt.Printf("Worker %s: Completed successfully\n", path)
	//	}
	case <-ctx.Done():
		fmt.Printf("Worker %s: Aborted due to cancellation\n", path)
	}
}

// TODO best effort delete, We should have retry here. If not able to clean,
// we still have scavanger process
// TODO need race condition
func deleteObjects(bucket string, objects []string) error {
	return nil
}

// check Empty on prefix/ folder
