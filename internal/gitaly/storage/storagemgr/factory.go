package storagemgr

import (
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/keyvalue/databasemgr"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/node"
	"gitlab.com/gitlab-org/gitaly/v16/internal/log"
)

// Factory is a factory type that can instantiate new storages.
type Factory struct {
	logger           log.Logger
	dbMgr            *databasemgr.DBManager
	partitionFactory PartitionFactory
	metrics          *Metrics
}

// NewFactory returns a new Factory.
func NewFactory(
	logger log.Logger,
	dbMgr *databasemgr.DBManager,
	partitionFactory PartitionFactory,
	metrics *Metrics,
) Factory {
	return Factory{
		logger:           logger,
		dbMgr:            dbMgr,
		partitionFactory: partitionFactory,
		metrics:          metrics,
	}
}

// New returns a new Storage.
func (f Factory) New(name, path string) (node.Storage, error) {
	return NewStorageManager(
		f.logger, name, path, f.dbMgr, f.partitionFactory, f.metrics,
	)
}
