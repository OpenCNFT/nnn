package storagemgr

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io/fs"
	"path/filepath"
	"strconv"
	"sync"

	"github.com/dgraph-io/badger/v4"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/stats"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
)

var (
	// errPartitionAssignmentNotFound is returned when attempting to access a
	// partition assignment in the database that doesn't yet exist.
	errPartitionAssignmentNotFound = errors.New("partition assignment not found")
	// errNoAlternate is used internally by partitionAssigner to signal a repository
	// has no alternates.
	errNoAlternate = errors.New("repository has no alternate")
	// errMultipleAlternates is returned when a repository has multiple alternates
	// configured.
	errMultipleAlternates = errors.New("repository has multiple alternates")
	// errAlternatePointsToSelf is returned when a repository's alternate points to the
	// repository itself.
	errAlternatePointsToSelf = errors.New("repository's alternate points to self")
	// errAlternateHasAlternate is returned when a repository's alternate itself has an
	// alternate listed.
	errAlternateHasAlternate = errors.New("repository's alternate has an alternate itself")
)

const prefixPartitionAssignment = "partition_assignment/"

// partitionID uniquely identifies a partition.
type partitionID uint64

func (id partitionID) MarshalBinary() []byte {
	marshaled := make([]byte, binary.Size(id))
	binary.BigEndian.PutUint64(marshaled, uint64(id))
	return marshaled
}

func (id *partitionID) UnmarshalBinary(data []byte) {
	*id = partitionID(binary.BigEndian.Uint64(data))
}

func (id partitionID) String() string {
	return strconv.FormatUint(uint64(id), 10)
}

// partitionAssignmentTable records which partitions repositories are assigned into.
type partitionAssignmentTable struct{ db Database }

func newPartitionAssignmentTable(db Database) *partitionAssignmentTable {
	return &partitionAssignmentTable{db: db}
}

func (pt *partitionAssignmentTable) key(relativePath string) []byte {
	return []byte(fmt.Sprintf("%s%s", prefixPartitionAssignment, relativePath))
}

func (pt *partitionAssignmentTable) getPartitionID(relativePath string) (partitionID, error) {
	var id partitionID
	if err := pt.db.View(func(txn DatabaseTransaction) error {
		item, err := txn.Get(pt.key(relativePath))
		if err != nil {
			if errors.Is(err, badger.ErrKeyNotFound) {
				return errPartitionAssignmentNotFound
			}

			return fmt.Errorf("get: %w", err)
		}

		return item.Value(func(value []byte) error {
			id.UnmarshalBinary(value)
			return nil
		})
	}); err != nil {
		return 0, fmt.Errorf("view: %w", err)
	}

	return id, nil
}

func (pt *partitionAssignmentTable) setPartitionID(relativePath string, id partitionID) error {
	wb := pt.db.NewWriteBatch()
	if err := wb.Set(pt.key(relativePath), id.MarshalBinary()); err != nil {
		return fmt.Errorf("set: %w", err)
	}

	if err := wb.Flush(); err != nil {
		return fmt.Errorf("flush: %w", err)
	}

	return nil
}

// partitionAssigner manages assignment of repositories in to partitions.
type partitionAssigner struct {
	// mutex synchronizes access to repositoryLocks.
	mutex sync.Mutex
	// repositoryLocks holds per-repository locks. The key is a relative path and the
	// channel closing signals the lock being released.
	repositoryLocks map[string]chan struct{}
	// idSequence is the sequence used to mint partition IDs.
	idSequence Sequence
	// partitionAssignmentTable contains the partition assignment records.
	partitionAssignmentTable *partitionAssignmentTable
	// storagePath is the path to the root directory of the storage the relative
	// paths are computed against.
	storagePath string
}

// newPartitionAssigner returns a new partitionAssigner. Close must be called on the
// returned instance to release acquired resources.
func newPartitionAssigner(db Database, storagePath string) (*partitionAssigner, error) {
	seq, err := db.GetSequence([]byte("partition_id_seq"), 100)
	if err != nil {
		return nil, fmt.Errorf("get sequence: %w", err)
	}

	return &partitionAssigner{
		repositoryLocks:          make(map[string]chan struct{}),
		idSequence:               seq,
		partitionAssignmentTable: newPartitionAssignmentTable(db),
		storagePath:              storagePath,
	}, nil
}

func (pa *partitionAssigner) Close() error {
	return pa.idSequence.Release()
}

func (pa *partitionAssigner) allocatePartitionID() (partitionID, error) {
	// Start partition IDs from 1 so the default value refers to an invalid
	// partition.
	var id uint64
	for id == 0 {
		var err error
		id, err = pa.idSequence.Next()
		if err != nil {
			return 0, fmt.Errorf("next: %w", err)
		}
	}

	return partitionID(id), nil
}

// getPartitionID returns the partition ID of the repository. If the repository wasn't yet assigned into
// a partition, it will be assigned into one and the assignment stored. Further accesses return the stored
// partition ID. Repositories without an alternate go into their own partitions. Repositories with an alternate
// are assigned into the same partition as the alternate repository. The alternate is assigned into a partition
// if it hasn't yet been. The method is safe to call concurrently.
func (pa *partitionAssigner) getPartitionID(ctx context.Context, relativePath string) (partitionID, error) {
	return pa.getPartitionIDRecursive(ctx, relativePath, false)
}

func (pa *partitionAssigner) getPartitionIDRecursive(ctx context.Context, relativePath string, recursiveCall bool) (partitionID, error) {
	ptnID, err := pa.partitionAssignmentTable.getPartitionID(relativePath)
	if err != nil {
		if !errors.Is(err, errPartitionAssignmentNotFound) {
			return 0, fmt.Errorf("get partition: %w", err)
		}

		// Repository wasn't yet assigned into a partition.

		pa.mutex.Lock()
		// See if some other goroutine already locked the repository. If so, wait for it to complete
		// and get the partition ID it set.
		if lock, ok := pa.repositoryLocks[relativePath]; ok {
			pa.mutex.Unlock()
			// Some other goroutine is already assigning a partition for the
			// repository. Wait for it to complete and then get the partition.
			select {
			case <-lock:
				ptnID, err := pa.partitionAssignmentTable.getPartitionID(relativePath)
				if err != nil {
					return 0, fmt.Errorf("get partition ID after waiting: %w", err)
				}
				return ptnID, nil
			case <-ctx.Done():
				return 0, ctx.Err()
			}
		}

		// No other goroutine had locked the repository yet. Lock the repository so other goroutines
		// wait while we assign the repository a partition.
		lock := make(chan struct{})
		pa.repositoryLocks[relativePath] = lock
		pa.mutex.Unlock()
		defer func() {
			close(lock)
			pa.mutex.Lock()
			delete(pa.repositoryLocks, relativePath)
			pa.mutex.Unlock()
		}()

		// With the repository locked, check first whether someone else assigned it into a partition
		// while we weren't holding the lock between the first failed attempt getting the assignment
		// and locking the repository.
		ptnID, err = pa.partitionAssignmentTable.getPartitionID(relativePath)
		if !errors.Is(err, errPartitionAssignmentNotFound) {
			if err != nil {
				return 0, fmt.Errorf("recheck partition: %w", err)
			}

			// Some other goroutine assigned a partition between the failed attempt and locking the
			// repository.
			return ptnID, nil
		}

		ptnID, err = pa.assignPartitionID(ctx, relativePath, recursiveCall)
		if err != nil {
			return 0, fmt.Errorf("assign partition ID: %w", err)
		}
	}

	return ptnID, nil
}

func (pa *partitionAssigner) assignPartitionID(ctx context.Context, relativePath string, recursiveCall bool) (partitionID, error) {
	// Check if the repository has an alternate. If so, it needs to go into the same
	// partition with it.
	ptnID, err := pa.getAlternatePartitionID(ctx, relativePath, recursiveCall)
	if err != nil {
		if !errors.Is(err, errNoAlternate) {
			return 0, fmt.Errorf("get alternate partition ID: %w", err)
		}

		// The repository has no alternate. Unpooled repositories go into their own partitions.
		// Allocate a new partition ID for this repository.
		ptnID, err = pa.allocatePartitionID()
		if err != nil {
			return 0, fmt.Errorf("acquire partition id: %w", err)
		}
	}

	if err := pa.partitionAssignmentTable.setPartitionID(relativePath, ptnID); err != nil {
		return 0, fmt.Errorf("set partition: %w", err)
	}

	return ptnID, nil
}

func (pa *partitionAssigner) getAlternatePartitionID(ctx context.Context, relativePath string, recursiveCall bool) (partitionID, error) {
	alternate, err := readAlternatesFile(filepath.Join(pa.storagePath, relativePath))
	if err != nil {
		return 0, fmt.Errorf("read alternates file: %w", err)
	}

	if recursiveCall {
		// recursive being true indicates we've arrived here through another repository's alternate.
		// Repositories in Gitaly should only have a single alternate that points to the repository's
		// pool. Chains of alternates are unexpected and could go arbitrarily long, so fail the operation.
		return 0, errAlternateHasAlternate
	}

	// The relative path should point somewhere within the same storage.
	alternateRelativePath, err := storage.ValidateRelativePath(
		pa.storagePath,
		// Take the relative path to the repository, not 'repository/objects'.
		filepath.Dir(
			// The path in alternates file points to the object directory of the alternate
			// repository. The path is relative to the repository's own object directory.
			filepath.Join(relativePath, "objects", alternate),
		),
	)
	if err != nil {
		return 0, fmt.Errorf("validate relative path: %w", err)
	}

	if alternateRelativePath == relativePath {
		// The alternate must not point to the repository itself. Not only is it non-sensical
		// but it would also cause a dead lock as the repository is locked during this call
		// already.
		return 0, errAlternatePointsToSelf
	}

	// The relative path should point to a Git directory.
	if err := storage.ValidateGitDirectory(filepath.Join(pa.storagePath, alternateRelativePath)); err != nil {
		return 0, fmt.Errorf("validate git directory: %w", err)
	}

	// Recursively get the alternate's partition ID or assign it one. This time
	// we set recursive to true to fail the operation if the alternate itself has an
	// alternate configured.
	ptnID, err := pa.getPartitionIDRecursive(ctx, alternateRelativePath, true)
	if err != nil {
		return 0, fmt.Errorf("get partition ID: %w", err)
	}

	return ptnID, nil
}

func readAlternatesFile(repositoryPath string) (string, error) {
	alternates, err := stats.ReadAlternatesFile(repositoryPath)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return "", errNoAlternate
		}

		return "", fmt.Errorf("read alternates file: %w", err)
	}

	if len(alternates) == 0 {
		return "", errNoAlternate
	} else if len(alternates) > 1 {
		// Repositories shouldn't have more than one alternate given they should only be
		// linked to a single pool at most.
		return "", errMultipleAlternates
	}

	return alternates[0], nil
}
