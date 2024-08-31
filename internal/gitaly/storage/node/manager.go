package node

import (
	"context"
	"fmt"
	"sync"

	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
)

// LogConsumerFactory returns a LogConsumer that requires a LogManagerAccessor for construction and
// a function to close the LogConsumer.
type LogConsumerFactory func(storage.LogManagerAccessor) (_ storage.LogConsumer, cleanup func())

// StorageFactory is responsible for instantiating a Storage.
type StorageFactory interface {
	// New sets up a new Storage instance.
	New(storageName, storagePath string, consumer storage.LogConsumer) (Storage, error)
}

// Storage extends the general Storage interface.
type Storage interface {
	storage.Storage
	// Close closes the Storage for further access and waits it to
	// shutdown completely.
	Close()
	// CallLogManager executes the provided function against the Partition for the specified partition, starting it if necessary.
	CallLogManager(ctx context.Context, partitionID storage.PartitionID, fn func(lm storage.LogManager)) error
}

// Manager is responsible for setting up the Gitaly node's storages and
// routing accesses to the correct storage based on the storage name.
type Manager struct {
	// storages contains all of the the configured storages. It's keyed
	// by the storage name and the value is the Storage itself.
	storages map[string]Storage
	// consumerCleanup cleans up the log consumer.
	consumerCleanup func()
}

// NewManager returns a new Manager.
func NewManager(
	configuredStorages []config.Storage,
	storageFactory StorageFactory,
	consumerFactory LogConsumerFactory,
) (_ *Manager, returnedErr error) {
	mgr := &Manager{
		storages:        make(map[string]Storage, len(configuredStorages)),
		consumerCleanup: func() {},
	}
	defer func() {
		if returnedErr != nil {
			mgr.Close()
		}
	}()

	var consumer storage.LogConsumer
	if consumerFactory != nil {
		consumer, mgr.consumerCleanup = consumerFactory(mgr)
	}

	for _, cfgStorage := range configuredStorages {
		storage, err := storageFactory.New(cfgStorage.Name, cfgStorage.Path, consumer)
		if err != nil {
			return nil, fmt.Errorf("new storage %q: %w", cfgStorage.Name, err)
		}

		mgr.storages[cfgStorage.Name] = storage
	}

	return mgr, nil
}

// GetStorage retrieves a Storage by its name.
func (mgr *Manager) GetStorage(storageName string) (storage.Storage, error) {
	return mgr.getStorage(storageName)
}

func (mgr *Manager) getStorage(storageName string) (Storage, error) {
	handle, ok := mgr.storages[storageName]
	if !ok {
		return nil, storage.NewStorageNotFoundError(storageName)
	}

	return handle, nil
}

// CallLogManager implements storage.LogManagerAccessor by relaying the call to the correct storage.
func (mgr *Manager) CallLogManager(ctx context.Context, storageName string, partitionID storage.PartitionID, fn func(lm storage.LogManager)) error {
	handle, err := mgr.getStorage(storageName)
	if err != nil {
		return err
	}

	return handle.CallLogManager(ctx, partitionID, fn)
}

// Close closes the storages. It waits for the storages to fully close
// before returning.
func (mgr *Manager) Close() {
	var active sync.WaitGroup
	for _, storage := range mgr.storages {
		storage := storage

		active.Add(1)
		go func() {
			defer active.Done()
			storage.Close()
		}()
	}

	active.Wait()

	mgr.consumerCleanup()
}
