package fshistory

import (
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v16/internal/structerr"
)

// NewConflictingOperationError returns a new error detailing a conflicting file
// system operation.
func NewConflictingOperationError(path string, readLSN, targetLSN storage.LSN) error {
	return structerr.NewAborted("conflicting file system operation").WithMetadataItems(
		structerr.MetadataItem{Key: "path", Value: path},
		structerr.MetadataItem{Key: "read_lsn", Value: readLSN},
		structerr.MetadataItem{Key: "target_lsn", Value: targetLSN},
	)
}

func newNotFoundError(path string) error {
	return structerr.NewAborted("path not found").WithMetadata("path", path)
}

func newNotDirectoryError(path string) error {
	return structerr.NewAborted("not a directory").WithMetadata("path", path)
}

func newDirectoryNotEmptyError(path string) error {
	return structerr.NewAborted("directory not empty").WithMetadata("path", path)
}

func newAlreadyExistsError(path string) error {
	return structerr.NewAborted("path already exists").WithMetadata("path", path)
}
