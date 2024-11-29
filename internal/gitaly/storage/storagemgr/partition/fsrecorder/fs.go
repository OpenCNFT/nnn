package fsrecorder

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/mode"
	"gitlab.com/gitlab-org/gitaly/v16/internal/structerr"
)

func newTargetIsFileError(path string) error {
	return structerr.NewFailedPrecondition("target is a file").WithMetadata("path", path)
}

func newPathEscapesRootError(path string) error {
	return structerr.NewInvalidArgument("path escapes root").WithMetadata("path", path)
}

// WALBuilder is the interface of a WAL entry builder.
type WALBuilder interface {
	// CreateDirectory records creation of a single directory.
	CreateDirectory(relativePath string)
	// RemoveDirectoryEntry records the removal of the directory entry at the given path.
	RemoveDirectoryEntry(relativePath string)
	// CreateFile stages the file at the source and adds an operation to link it
	// to the given destination relative path in the storage.
	CreateFile(sourceAbsolutePath string, relativePath string) error
	// CreateLink records a creation of a hard link to an exisiting file in the partition.
	CreateLink(sourcePath, destinationPath string)
}

// ReadSet contains all paths recorded as being read.
type ReadSet map[string]struct{}

// FS performs file system operations and records them into
// WAL entry as they are performed. The paths provided to
// the methods should be relative to the transaction's
// file system snapshot's root.
type FS struct {
	root    string
	wal     WALBuilder
	readSet ReadSet
}

// NewFS returns a new FS.
func NewFS(root string, wal WALBuilder) FS {
	return FS{
		root:    root,
		wal:     wal,
		readSet: ReadSet{},
	}
}

func (f FS) validatePath(path string) (string, error) {
	absolutePath := filepath.Join(f.root, path)
	if !strings.HasPrefix(absolutePath, f.root+"/") {
		return "", newPathEscapesRootError(path)
	}

	return filepath.Rel(f.root, absolutePath)
}

// Mkdir creates a directory and records the directory creation.
func (f FS) Mkdir(path string) error {
	path, err := f.validatePath(path)
	if err != nil {
		return err
	}

	return f.mkdir(path)
}

func (f FS) mkdir(path string) error {
	if err := os.Mkdir(filepath.Join(f.root, path), mode.Directory); err != nil {
		return fmt.Errorf("mkdir: %w", err)
	}

	f.wal.CreateDirectory(path)

	return nil
}

// MkdirAll creates all missing directories along the path and logs the creations.
func (f FS) MkdirAll(path string) error {
	path, err := f.validatePath(path)
	if err != nil {
		return err
	}

	return f.mkdirAll(path)
}

func (f FS) mkdirAll(path string) error {
	if info, err := os.Lstat(filepath.Join(f.root, path)); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("stat: %w", err)
	} else if info != nil && !info.IsDir() {
		return newTargetIsFileError(path)
	}

	var (
		currentRelativePath string
		currentSuffix       = path
		hasMore             = true
	)

	for hasMore {
		var prefix string
		prefix, currentSuffix, hasMore = strings.Cut(currentSuffix, "/")
		currentRelativePath = filepath.Join(currentRelativePath, prefix)

		if err := f.mkdir(currentRelativePath); err != nil {
			if errors.Is(err, fs.ErrExist) {
				// The directory already existed. Continue to the child directory.
				continue
			}

			return fmt.Errorf("create parent directory: %w", err)
		}
	}

	return nil
}

// Root is the absolute path to the root of the transaction's file system snapshot.
func (f FS) Root() string {
	return f.root
}

// ReadSet returns a set of file system paths read.
func (f FS) ReadSet() ReadSet {
	return f.readSet
}

// RecordRead records the given path as read by the transaction.
func (f FS) RecordRead(path string) error {
	path, err := f.validatePath(path)
	if err != nil {
		return fmt.Errorf("validate path: %w", err)
	}

	f.readSet[path] = struct{}{}

	return nil
}

// RecordFile records a file creation into the transaction.
func (f FS) RecordFile(path string) error {
	path, err := f.validatePath(path)
	if err != nil {
		return fmt.Errorf("validate path: %w", err)
	}

	if err := f.wal.CreateFile(filepath.Join(f.root, path), path); err != nil {
		return fmt.Errorf("record file creation: %w", err)
	}

	return nil
}

// RecordLink records a hard link creation into the transaction.
func (f FS) RecordLink(sourcePath, destinationPath string) error {
	sourcePath, err := f.validatePath(sourcePath)
	if err != nil {
		return fmt.Errorf("validate source path: %w", err)
	}

	destinationPath, err = f.validatePath(destinationPath)
	if err != nil {
		return fmt.Errorf("validate destination path: %w", err)
	}

	f.wal.CreateLink(sourcePath, destinationPath)

	return nil
}

// RecordDirectory records a directory creation into the transaction.
func (f FS) RecordDirectory(path string) error {
	path, err := f.validatePath(path)
	if err != nil {
		return fmt.Errorf("validate path: %w", err)
	}

	f.wal.CreateDirectory(path)

	return nil
}

// RecordRemoval records a directory entry removal into the transaction.
func (f FS) RecordRemoval(path string) error {
	path, err := f.validatePath(path)
	if err != nil {
		return fmt.Errorf("validate path: %w", err)
	}

	f.wal.RemoveDirectoryEntry(path)

	return nil
}
