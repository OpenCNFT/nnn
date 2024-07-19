// Package perm provides constants for file and directory permissions.
//
// Note that these permissions are further restricted by the system configured
// umask.
package perm

import (
	"io/fs"
)

const (
	// SharedFile is the permission given for a file that may be read outside
	// of gitaly.
	SharedFile fs.FileMode = 0o644
)

// Umask represents a umask that is used to mask mode bits.
type Umask int

// Mask applies the mask on the mode.
func (mask Umask) Mask(mode fs.FileMode) fs.FileMode {
	return mode & ^fs.FileMode(mask)
}
