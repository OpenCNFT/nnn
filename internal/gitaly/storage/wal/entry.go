package wal

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"

	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/mode"
	"gitlab.com/gitlab-org/gitaly/v16/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
)

// Entry represents a write-ahead log entry.
type Entry struct {
	// fileIDSequence is a sequence used to generate unique IDs for files
	// staged as part of this entry.
	fileIDSequence uint64
	// operations are the operations this entry consists of.
	operations operations
	// stateDirectory is the directory where the entry's state is stored.
	stateDirectory string
}

func newIrregularFileStagedError(mode fs.FileMode) error {
	return structerr.NewInvalidArgument("irregular file staged").WithMetadata("mode", mode.String())
}

// NewEntry returns a new Entry that can be used to construct a write-ahead
// log entry.
func NewEntry(stateDirectory string) *Entry {
	return &Entry{stateDirectory: stateDirectory}
}

// Directory returns the absolute path of the directory where the log entry is staging its state.
func (e *Entry) Directory() string {
	return e.stateDirectory
}

// Operations returns the operations of the log entry.
func (e *Entry) Operations() []*gitalypb.LogEntry_Operation {
	return e.operations
}

// stageFile stages a file into the WAL entry by linking it in the state directory.
// The file's name in the state directory is returned and can be used to link the file
// subsequently into the correct location.
func (e *Entry) stageFile(path string) (string, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return "", fmt.Errorf("lstat: %w", err)
	}

	// Error out if there is an attempt to stage someting other than a regular file, ie.
	// symlink, directory or anything else.
	if !info.Mode().IsRegular() {
		return "", newIrregularFileStagedError(info.Mode().Type())
	}

	// Strip the write permissions of the files. Our snapshot isolation relies on files not
	// being modified. Also strip permissions of other users than Gitaly's user.
	//
	// ModeExecutable is used as the mask since it has the widest permission bits we allow
	// with both read and execute permissions set.
	actualPerms := info.Mode().Perm()
	if expectedPerms := actualPerms & (mode.Executable); actualPerms != expectedPerms {
		if err := os.Chmod(path, expectedPerms); err != nil {
			return "", fmt.Errorf("chmod: %w", err)
		}
	}

	e.fileIDSequence++

	// We use base 36 as it produces shorter names and thus smaller log entries.
	// The file names within the log entry are not important as the manifest records the
	// actual name the file will be linked as.
	fileName := strconv.FormatUint(e.fileIDSequence, 36)
	if err := os.Link(path, filepath.Join(e.stateDirectory, fileName)); err != nil {
		return "", fmt.Errorf("link: %w", err)
	}

	return fileName, nil
}

// SetKey adds an operation to set a key with a value in the partition's key-value store.
func (e *Entry) SetKey(key, value []byte) {
	e.operations.setKey(key, value)
}

// DeleteKey adds an operation to delete a key from the partition's key-value store.
func (e *Entry) DeleteKey(key []byte) {
	e.operations.deleteKey(key)
}

// RecordMkdir records creation of a single directory.
func (e *Entry) RecordMkdir(relativePath string) {
	e.operations.createDirectory(relativePath)
}

// RecordFileCreation stages the file at the source and adds an operation to link it
// to the given destination relative path in the storage.
func (e *Entry) RecordFileCreation(sourceAbsolutePath string, relativePath string) error {
	stagedFile, err := e.stageFile(sourceAbsolutePath)
	if err != nil {
		return fmt.Errorf("stage file: %w", err)
	}

	e.operations.createHardLink(stagedFile, relativePath, false)
	return nil
}

// CreateLink records a creation of a hard link to an exisiting file in the partition.
func (e *Entry) CreateLink(sourceRelativePath, destinationRelativePath string) {
	e.operations.createHardLink(sourceRelativePath, destinationRelativePath, true)
}

// RecordFileUpdate records a file being updated. It stages operations to remove the old file,
// to place the new file in its place.
func (e *Entry) RecordFileUpdate(storageRoot, relativePath string) error {
	e.RecordDirectoryEntryRemoval(relativePath)

	if err := e.RecordFileCreation(filepath.Join(storageRoot, relativePath), relativePath); err != nil {
		return fmt.Errorf("create file: %w", err)
	}

	return nil
}

// RecordDirectoryEntryRemoval records the removal of the file system object at the given path.
func (e *Entry) RecordDirectoryEntryRemoval(relativePath string) {
	e.operations.removeDirectoryEntry(relativePath)
}

// RecordDirectoryCreation records the operations to create a given directory in the storage and
// all of its children in to the storage.
func (e *Entry) RecordDirectoryCreation(storageRoot, directoryRelativePath string) error {
	if err := e.recordDirectoryCreation(storageRoot, directoryRelativePath); err != nil {
		return err
	}

	return nil
}

func (e *Entry) recordDirectoryCreation(storageRoot, directoryRelativePath string) error {
	if err := walkDirectory(storageRoot, directoryRelativePath,
		func(relativePath string, dirEntry fs.DirEntry) error {
			// Create the directories before descending in them so they exist when
			// we try to create the children.
			e.operations.createDirectory(relativePath)
			return nil
		},
		func(relativePath string, dirEntry fs.DirEntry) error {
			// The parent directory has already been created so we can immediately create
			// the file.
			if err := e.RecordFileCreation(filepath.Join(storageRoot, relativePath), relativePath); err != nil {
				return fmt.Errorf("create file: %w", err)
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
func (e *Entry) RecordDirectoryRemoval(storageRoot, directoryRelativePath string) error {
	if err := walkDirectory(storageRoot, directoryRelativePath,
		func(string, fs.DirEntry) error { return nil },
		func(relativePath string, dirEntry fs.DirEntry) error {
			e.operations.removeDirectoryEntry(relativePath)
			return nil
		},
		func(relativePath string, dirEntry fs.DirEntry) error {
			e.operations.removeDirectoryEntry(relativePath)
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
func (e *Entry) RecordAlternateUnlink(storageRoot, relativePath, alternatePath string) error {
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

			e.operations.createDirectory(destinationDir)
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
				e.operations.createHardLink(
					filepath.Join(sourceDir, objectFile.Name()),
					objectDestination,
					true,
				)
			}
		}
	}

	destinationAlternatesPath := filepath.Join(destinationObjectsDir, "info", "alternates")
	e.RecordDirectoryEntryRemoval(destinationAlternatesPath)

	return nil
}
