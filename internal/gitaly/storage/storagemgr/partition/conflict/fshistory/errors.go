package fshistory

import (
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v16/internal/structerr"
)

// ReadWriteConflictError is returned when transaction performed a read on an
// inode that was modified by a concurrent transaction.
type ReadWriteConflictError struct {
	Path     string
	ReadLSN  storage.LSN
	WriteLSN storage.LSN
}

// ErrorMetadata returns the metadata associated with this error for logging.
func (err ReadWriteConflictError) ErrorMetadata() []structerr.MetadataItem {
	return []structerr.MetadataItem{
		{Key: "path", Value: err.Path},
		{Key: "read_lsn", Value: err.ReadLSN},
		{Key: "write_lsn", Value: err.WriteLSN},
	}
}

// Error returns the error message.
func (ReadWriteConflictError) Error() string {
	return "path was modified after read"
}

// NewReadWriteConflictError returns an error detailing a conflicting read.
func NewReadWriteConflictError(path string, readLSN, writeLSN storage.LSN) error {
	return ReadWriteConflictError{Path: path, ReadLSN: readLSN, WriteLSN: writeLSN}
}

// NotFoundError is returned when a given path is not found.
type NotFoundError struct {
	Path string
}

// ErrorMetadata returns the metadata associated with this error for logging.
func (err NotFoundError) ErrorMetadata() []structerr.MetadataItem {
	return []structerr.MetadataItem{{Key: "path", Value: err.Path}}
}

// Error returns the error message.
func (NotFoundError) Error() string {
	return "path not found"
}

func newNotFoundError(path string) error {
	return NotFoundError{Path: path}
}

// NotDirectoryError is returned when an element in a walked
// path is not a directory.
type NotDirectoryError struct {
	Path string
}

// ErrorMetadata returns the metadata associated with this error for logging.
func (err NotDirectoryError) ErrorMetadata() []structerr.MetadataItem {
	return []structerr.MetadataItem{{Key: "path", Value: err.Path}}
}

// Error returns the error message.
func (NotDirectoryError) Error() string {
	return "not a directory"
}

func newNotDirectoryError(path string) error {
	return NotDirectoryError{Path: path}
}

// DirectoryNotEmptyError is returned when attempting to remove a
// directory that is not empty.
type DirectoryNotEmptyError struct {
	Path string
}

// ErrorMetadata returns the metadata associated with this error for logging.
func (err DirectoryNotEmptyError) ErrorMetadata() []structerr.MetadataItem {
	return []structerr.MetadataItem{{Key: "path", Value: err.Path}}
}

// Error returns the error message.
func (DirectoryNotEmptyError) Error() string {
	return "directory not empty"
}

func newDirectoryNotEmptyError(path string) error {
	return DirectoryNotEmptyError{Path: path}
}

// AlreadyExistsError is returned when attempting when a file
// or directory already exists at the path.
type AlreadyExistsError struct {
	Path string
}

// ErrorMetadata returns the metadata associated with this error for logging.
func (err AlreadyExistsError) ErrorMetadata() []structerr.MetadataItem {
	return []structerr.MetadataItem{{Key: "path", Value: err.Path}}
}

// Error returns the error message.
func (AlreadyExistsError) Error() string {
	return "already exists"
}

func newAlreadyExistsError(path string) error {
	return AlreadyExistsError{Path: path}
}
