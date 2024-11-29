package storage

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
)

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
