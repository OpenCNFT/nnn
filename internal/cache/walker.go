package cache

import (
	"os"
	"path/filepath"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
	"gitlab.com/gitlab-org/gitaly/internal/config"
)

var (
	walkerCheckTotal = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "gitaly_diskcache_walker_check_total",
			Help: "Total number of events during diskcache filesystem walks",
		},
	)
	walkerRemovalTotal = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "gitaly_diskcache_walker_removal_total",
			Help: "Total number of events during diskcache filesystem walks",
		},
	)
)

func init() {
	prometheus.MustRegister(walkerCheckTotal)
	prometheus.MustRegister(walkerRemovalTotal)
}

func countWalkRemoval() { walkerRemovalTotal.Inc() }
func countWalkCheck()   { walkerCheckTotal.Inc() }

// TODO: replace constant with constant defined in !1305
const fileStaleness = time.Hour

func cleanWalk(storagePath string) error {
	return filepath.Walk(storagePath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if info.IsDir() {
			return nil
		}

		countWalkCheck()

		threshold := time.Now().Add(-1 * fileStaleness)
		if info.ModTime().After(threshold) {
			return nil
		}

		if err := os.Remove(path); err != nil {
			return err
		}

		countWalkRemoval()

		return nil
	})
}

const cleanWalkFrequency = 10 * time.Minute

func startCleanWalker(storage config.Storage) {
	walkTick := time.NewTicker(cleanWalkFrequency)
	go func() {
		for {
			if err := cleanWalk(storage.Path); err != nil {
				logrus.WithField("storage", storage.Name).Error(err)
			}
			<-walkTick.C
		}
	}()
}

func init() {
	config.RegisterHook(func() error {
		for _, storage := range config.Config.Storages {
			startCleanWalker(storage)
		}
		return nil
	})
}
