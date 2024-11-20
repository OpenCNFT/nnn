package fsrecorder

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/mode"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/wal"
	"gitlab.com/gitlab-org/gitaly/v16/internal/structerr"
)

func newTargetIsFileError(path string) error {
	return structerr.NewFailedPrecondition("target is a file").WithMetadata("path", path)
}

func newPathEscapesRootError(path string) error {
	return structerr.NewInvalidArgument("path escapes root").WithMetadata("path", path)
}

// FS performs file system operations and records them into
// WAL entry as they are performed. The paths provided to
// the methods should be relative to the transaction's
// file system snapshot's root.
type FS struct {
	root  string
	entry *wal.Entry
}

// NewFS returns a new FS.
func NewFS(root string, entry *wal.Entry) FS {
	return FS{
		root:  root,
		entry: entry,
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

	f.entry.RecordMkdir(path)

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
