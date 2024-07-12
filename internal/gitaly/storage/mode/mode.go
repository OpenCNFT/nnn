// Package mode contains the file modes that are supported by the storage. All files and
// directories written to the storage must use one of these modes.
package mode

import "io/fs"

const (
	// Directory is the mode directories are stored with in the storage.
	// It gives the owner read, write, and execute permissions on directories.
	Directory fs.FileMode = fs.ModeDir | 0o700
	// ReadOnlyDirectory is the mode given to directories in read-only snapshots.
	// It gives the owner read and execute permissions on directories.
	ReadOnlyDirectory fs.FileMode = fs.ModeDir | 0o500
	// Executable is the mode executable files are stored with in the storage.
	// It gives the owner read and execute permissions on the executable files.
	Executable fs.FileMode = 0o500
	// File is the mode files are stored with in the storage.
	// It gives the owner read permissions on the files.
	File fs.FileMode = 0o400
)
