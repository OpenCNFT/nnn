package storage

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

type noopFS struct {
	root string
}

// NewNoopFS returns an FS implementation that is not tied to a transaction
// and operates directly on the storage root.
func NewNoopFS(root string) FS {
	return noopFS{root: root}
}

func (f noopFS) Root() string { return f.root }

func (f noopFS) RecordRead(path string) error { return nil }

func (f noopFS) RecordFile(path string) error { return nil }

func (f noopFS) RecordLink(sourcePath, destinationPath string) error { return nil }

func (f noopFS) RecordDirectory(path string) error { return nil }

func (f noopFS) RecordRemoval(path string) error { return nil }

func newTargetIsFileError(path string) error {
	return structerr.NewFailedPrecondition("target is a file").WithMetadata("path", path)
}

// Link creats a hard link from source to destination and records it.
func Link(f FS, source, destination string) error {
	if err := os.Link(filepath.Join(f.Root(), source), filepath.Join(f.Root(), destination)); err != nil {
		return fmt.Errorf("link: %w", err)
	}

	if err := f.RecordLink(source, destination); err != nil {
		return fmt.Errorf("record link: %w", err)
	}

	return nil
}

// Mkdir creates a directory and records the directory creation.
func Mkdir(f FS, path string) error {
	if err := os.Mkdir(filepath.Join(f.Root(), path), mode.Directory); err != nil {
		return fmt.Errorf("mkdir: %w", err)
	}

	if err := f.RecordDirectory(path); err != nil {
		return fmt.Errorf("record directory: %w", err)
	}

	return nil
}

// MkdirAll creates all missing directories along the path and logs the creations.
func MkdirAll(f FS, path string) error {
	if info, err := os.Lstat(filepath.Join(f.Root(), path)); err != nil && !errors.Is(err, fs.ErrNotExist) {
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

		if err := Mkdir(f, currentRelativePath); err != nil {
			if errors.Is(err, fs.ErrExist) {
				// The directory already existed. Continue to the child directory.
				continue
			}

			return fmt.Errorf("create parent directory: %w", err)
		}
	}

	return nil
}

// RecordDirectoryCreation records the operations to create a given directory all of its children.
func RecordDirectoryCreation(f FS, relativePath string) error {
	if err := walkDirectory(f.Root(), relativePath,
		func(relativePath string, dirEntry fs.DirEntry) error {
			// Create the directories before descending in them so they exist when
			// we try to create the children.
			if err := f.RecordDirectory(relativePath); err != nil {
				return fmt.Errorf("record directory: %w", err)
			}

			return nil
		},
		func(relativePath string, dirEntry fs.DirEntry) error {
			// The parent directory has already been created so we can immediately create
			// the file.
			if err := f.RecordFile(relativePath); err != nil {
				return fmt.Errorf("record file: %w", err)
			}

			return nil
		},
		func(relativePath string, dirEntry fs.DirEntry) error {
			return nil
		},
	); err != nil {
		return fmt.Errorf("walk directory: %w", err)
	}

	return nil
}

// RecordDirectoryRemoval records a directory to be removed with all of its children.
func RecordDirectoryRemoval(f FS, storageRoot, directoryRelativePath string) error {
	if err := walkDirectory(storageRoot, directoryRelativePath,
		func(string, fs.DirEntry) error { return nil },
		func(relativePath string, dirEntry fs.DirEntry) error {
			if err := f.RecordRemoval(relativePath); err != nil {
				return fmt.Errorf("record file removal: %w", err)
			}

			return nil
		},
		func(relativePath string, dirEntry fs.DirEntry) error {
			if err := f.RecordRemoval(relativePath); err != nil {
				return fmt.Errorf("record directory removal: %w", err)
			}

			return nil
		},
	); err != nil {
		return fmt.Errorf("walk directory: %w", err)
	}

	return nil
}

// RecordAlternateUnlink records the operations to unlink the repository at the relative path
// from its alternate. All loose objects and packs are hard linked from the alternate to the
// repository and the `objects/info/alternate` file is removed.
func RecordAlternateUnlink(f FS, storageRoot, relativePath, alternatePath string) error {
	destinationObjectsDir := filepath.Join(relativePath, "objects")
	sourceObjectsDir := filepath.Join(destinationObjectsDir, alternatePath)

	entries, err := os.ReadDir(filepath.Join(storageRoot, sourceObjectsDir))
	if err != nil {
		return fmt.Errorf("read alternate objects dir: %w", err)
	}

	for _, subDir := range entries {
		if !subDir.IsDir() || !(len(subDir.Name()) == 2 || subDir.Name() == "pack") {
			// Only look in objects/<xx> and objects/pack for files.
			continue
		}

		sourceDir := filepath.Join(sourceObjectsDir, subDir.Name())

		objects, err := os.ReadDir(filepath.Join(storageRoot, sourceDir))
		if err != nil {
			return fmt.Errorf("read subdirectory: %w", err)
		}

		if len(objects) == 0 {
			// Don't create empty directories
			continue
		}

		destinationDir := filepath.Join(destinationObjectsDir, subDir.Name())

		// Create the destination directory if it doesn't yet exist.
		if _, err := os.Stat(filepath.Join(storageRoot, destinationDir)); err != nil {
			if !errors.Is(err, fs.ErrNotExist) {
				return fmt.Errorf("stat: %w", err)
			}

			if err := f.RecordDirectory(destinationDir); err != nil {
				return fmt.Errorf("record directory: %w", err)
			}
		}

		// Create all of the objects in the directory if they don't yet exist.
		for _, objectFile := range objects {
			if !objectFile.Type().IsRegular() {
				continue
			}

			objectDestination := filepath.Join(destinationDir, objectFile.Name())
			if _, err := os.Stat(filepath.Join(storageRoot, objectDestination)); err != nil {
				if !errors.Is(err, fs.ErrNotExist) {
					return fmt.Errorf("stat: %w", err)
				}

				// The object doesn't yet exist, log the linking.
				if err := f.RecordLink(
					filepath.Join(sourceDir, objectFile.Name()),
					objectDestination,
				); err != nil {
					return fmt.Errorf("record object linking: %w", err)
				}
			}
		}
	}

	destinationAlternatesPath := filepath.Join(destinationObjectsDir, "info", "alternates")
	if err := f.RecordRemoval(destinationAlternatesPath); err != nil {
		return fmt.Errorf("record alternates removal: %w", err)
	}

	return nil
}
