package gitaly

import (
	"embed"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/mode"
	"golang.org/x/sync/errgroup"
)

// buildDir is the directory path where our build target places the built binaries.
const buildDir = "_build/bin"

// packedBinariesFS contains embedded binaries. If you modify the below embeddings, you must also update
// GITALY_PACKED_EXECUTABLES in Makefile and packedBinaries in internal/gitaly/config/config.go. Embedded
// Git binaries should not be added to GITALY_PACKED_EXECUTABLES.
//
//go:embed _build/bin/gitaly-hooks _build/bin/gitaly-ssh _build/bin/gitaly-lfs-smudge _build/bin/gitaly-gpg
//go:embed _build/bin/gitaly-git-*
var packedBinariesFS embed.FS

// UnpackAuxiliaryBinaries unpacks the packed auxiliary binaries of Gitaly into destination directory.
//
// Gitaly invoking auxiliary binaries across different releases is a source of backwards compatibility issues.
// The calling protocol may change and cause issues if we don't carefully maintain the compatibility. Major version
// changing the module path also causes problems for gob encoding as it effectively changes the name of every type.
// To avoid having to maintain backwards compatibility between the different Gitaly binaries, we want to pin a given
// gitaly binary to only ever call the auxiliary binaries of the same build. We achieve this by packing the auxiliary
// binaries in the main gitaly binary and unpacking them on start to a temporary directory we can call them from. This
// way updating the gitaly binaries on the disk is atomic and a running gitaly can't call auxiliary binaries from a
// different version.
func UnpackAuxiliaryBinaries(destinationDir string, shouldInclude func(binaryName string) bool) error {
	entries, err := packedBinariesFS.ReadDir(buildDir)
	if err != nil {
		return fmt.Errorf("list packed binaries: %w", err)
	}

	g := &errgroup.Group{}
	for _, entry := range entries {
		if !shouldInclude(entry.Name()) {
			continue
		}
		g.Go(func() error {
			packedPath := filepath.Join(buildDir, entry.Name())
			packedFile, err := packedBinariesFS.Open(packedPath)
			if err != nil {
				return fmt.Errorf("open packed binary %q: %w", packedPath, err)
			}
			defer func() {
				// We already check the error below.
				_ = packedFile.Close()
			}()

			unpackedPath := filepath.Join(destinationDir, entry.Name())
			unpackedFile, err := os.OpenFile(unpackedPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, mode.Executable)
			if err != nil {
				return err
			}
			defer func() {
				// We already check the error below.
				_ = unpackedFile.Close()
			}()

			if _, err := io.Copy(unpackedFile, packedFile); err != nil {
				return fmt.Errorf("unpack %q: %w", unpackedPath, err)
			}

			if err := unpackedFile.Close(); err != nil {
				return fmt.Errorf("close %q: %w", unpackedPath, err)
			}

			if err := packedFile.Close(); err != nil {
				return fmt.Errorf("close packed file %q: %w", packedPath, err)
			}

			return nil
		})
	}
	return g.Wait()
}
