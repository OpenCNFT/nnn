// Package cache supplies background workers for periodically cleaning the
// cache folder on all storages listed in the config file. Upon configuration
// validation, one worker will be started for each storage. The worker will
// walk the cache directory tree and remove any files older than one hour. The
// worker will walk the cache directory every ten minutes.
package cache

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"time"

	"github.com/sirupsen/logrus"
	"gitlab.com/gitlab-org/gitaly/internal/config"
	"gitlab.com/gitlab-org/gitaly/internal/tempdir"
)

func cleanWalk(storage config.Storage) error {
	walkErr := filepath.Walk(tempdir.CacheDir(storage), func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if info.IsDir() {
			return nil
		}

		countWalkCheck()

		threshold := time.Now().Add(-1 * staleAge)
		if info.ModTime().After(threshold) {
			return nil
		}

		if err := os.Remove(path); err != nil {
			if os.IsNotExist(err) {
				// race condition: another file walker on the same storage may
				// have deleted the file already
				return nil
			}

			return err
		}

		countWalkRemoval()

		return nil
	})

	if os.IsNotExist(walkErr) {
		return nil
	}

	return walkErr
}

const cleanWalkFrequency = 10 * time.Minute

func startCleanWalker(storage config.Storage) {
	if disableWalker {
		return
	}

	logrus.WithField("storage", storage.Name).Info("Starting disk cache object walker")
	walkTick := time.NewTicker(cleanWalkFrequency)
	go func() {
		for {
			if err := cleanWalk(storage); err != nil {
				logrus.WithField("storage", storage.Name).Error(err)
			}

			<-walkTick.C
		}
	}()
}

var (
	disableMoveAndClear bool // only used to disable move and clear in tests
	disableWalker       bool // only used to disable object walker in tests
)

// moveAndClear will move the cache to the storage location's
// temporary folder, and then remove its contents asynchronously
func moveAndClear(storage config.Storage) error {
	if disableMoveAndClear {
		return nil
	}

	logger := logrus.WithField("storage", storage.Name)
	logger.Info("clearing disk cache object folder")

	tempPath := tempdir.TempDir(storage)
	if err := os.MkdirAll(tempPath, 0755); err != nil {
		return err
	}

	tmpDir, err := ioutil.TempDir(tempPath, "diskcache")
	if err != nil {
		return err
	}

	logger.Infof("moving disk cache object folder to %s", tmpDir)
	cachePath := tempdir.CacheDir(storage)
	if err := os.Rename(cachePath, filepath.Join(tmpDir, "moved")); err != nil {
		if os.IsNotExist(err) {
			logger.Info("disk cache object folder doesn't exist, no need to remove")
			return nil
		}

		return err
	}

	go func() {
		start := time.Now()
		if err := os.RemoveAll(tmpDir); err != nil {
			logger.Errorf("unable to remove disk cache objects: %q", err)
		}

		logger.Infof("cleared all cache object files in %s after %s", tmpDir, time.Since(start))
	}()

	return nil
}

func init() {
	config.RegisterHook(func(cfg config.Cfg) error {
		for _, storage := range cfg.Storages {
			if err := moveAndClear(storage); err != nil {
				return err
			}

			startCleanWalker(storage)
		}
		return nil
	})
}
