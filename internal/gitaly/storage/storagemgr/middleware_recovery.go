package storagemgr

import (
	"errors"
	"fmt"
	"io/fs"
	"os"

	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/keyvalue"
)

// MayHavePendingWAL checks whether transactions have been enabled in the past on the
// storages by checking whether the database directory exists. If so, the repositories
// in the storage may be incomplete if they have pending WAL entries.
func MayHavePendingWAL(storagePaths []string) (bool, error) {
	for _, storagePath := range storagePaths {
		if _, err := os.Stat(keyvalue.DatabaseDirectoryPath(storagePath)); err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				// Database didn't exist.
				continue
			}

			return false, fmt.Errorf("stat: %w", err)
		}

		return true, nil
	}

	return false, nil
}
