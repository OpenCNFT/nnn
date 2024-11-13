package testentry

import (
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/mode"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
	"google.golang.org/protobuf/proto"
)

// ManifestDirectoryEntry is a test utility to model no-disk manifest file of a WAL log entry. When a log entry is
// materialized, its protobuf representation is marshaled and written as a "MANIFEST" file inside WAL directory.
func ManifestDirectoryEntry(expected *gitalypb.LogEntry) testhelper.DirectoryEntry {
	return testhelper.DirectoryEntry{
		Mode:    mode.File,
		Content: expected,
		ParseContent: func(tb testing.TB, path string, content []byte) any {
			var logEntry gitalypb.LogEntry
			require.NoError(tb, proto.Unmarshal(content, &logEntry))
			return &logEntry
		},
	}
}

// BuildReftableDirectory builds the testhelper.DirectoryState for reftables.
// In the file system backend, we know for certain the files which will be included
// and also their content. But in the reftable backend, we have a binary format
// which we cannot verify and this is not deterministic. So let's do a best effort
// basis here. We verify that the number of tables are correct and that the tables.list
// constituents of reftables. We do not verify the content of the reftable themselves
// here.
func BuildReftableDirectory(data map[int][]git.ReferenceUpdates) testhelper.DirectoryState {
	state := testhelper.DirectoryState{
		"/":    {Mode: mode.Directory},
		"/wal": {Mode: mode.Directory},
	}

	for id, updates := range data {
		prefix := fmt.Sprintf("/wal/000000000000%d", id)
		numTables := uint(len(updates))

		state[prefix] = testhelper.DirectoryEntry{Mode: mode.Directory}
		state[prefix+"/MANIFEST"] = testhelper.DirectoryEntry{
			Mode:    mode.File,
			Content: true,
			ParseContent: func(tb testing.TB, path string, content []byte) any {
				var logEntry gitalypb.LogEntry
				require.NoError(tb, proto.Unmarshal(content, &logEntry))

				// If there are no reftables being created, we exit early.
				if numTables == 0 {
					return true
				}

				// If there are reftables being created, we need to account for
				// N tables and +2 for the tables.list being updated.
				require.Equal(tb, numTables+2, uint(len(logEntry.GetOperations())))

				// The reftables should only be created.
				for i := uint(0); i < numTables; i++ {
					create := logEntry.GetOperations()[i].GetCreateHardLink()
					require.NotNil(tb, create)
					require.True(tb, git.ReftableTableNameRegex.Match(create.GetDestinationPath()))
				}

				// The tables.list should be deleted and create (updated).
				delete := logEntry.GetOperations()[numTables].GetRemoveDirectoryEntry()
				require.NotNil(tb, delete)
				require.True(tb, strings.Contains(string(delete.GetPath()), "tables.list"))

				create := logEntry.GetOperations()[numTables].GetRemoveDirectoryEntry()
				require.NotNil(tb, create)
				require.True(tb, strings.Contains(string(delete.GetPath()), "tables.list"))

				return true
			},
		}

		// Parse the reftable and check its references.
		for i := uint(1); i <= numTables; i++ {
			state[fmt.Sprintf("%s/%d", prefix, i)] = testhelper.DirectoryEntry{
				Mode:    mode.File,
				Content: updates[i-1],
				ParseContent: func(tb testing.TB, path string, content []byte) any {
					table, err := git.NewReftable(content)
					require.NoError(tb, err)

					references, err := table.IterateRefs()
					require.NoError(tb, err)

					refUpdates := make(git.ReferenceUpdates)

					for _, reference := range references {
						update := git.ReferenceUpdate{}

						if reference.IsSymbolic {
							update.NewTarget = git.ReferenceName(reference.Target)
						} else {
							update.NewOID = git.ObjectID(reference.Target)
						}

						refUpdates[reference.Name] = update
					}

					return refUpdates
				},
			}
		}

		// For tables.list, we can verify that all the lines within that file are
		// reftables indeed.
		if numTables > 0 {
			state[fmt.Sprintf("%s/%d", prefix, numTables+1)] = testhelper.DirectoryEntry{
				Mode:    mode.File,
				Content: true,
				ParseContent: func(tb testing.TB, path string, content []byte) any {
					for _, file := range strings.Split(string(content), "\n") {
						if len(file) == 0 {
							break
						}

						require.True(tb, git.ReftableTableNameRegex.Match([]byte(file)))
					}
					return true
				},
			}
		}
	}

	return state
}

// RefChangeLogEntry returns a log entry as a result of a reference update operation. This helper handles the on-disk
// format differences between file backend and reftable backend. As a result, it shortens test assertions significantly.
func RefChangeLogEntry(relativePath string, ref string, oid git.ObjectID) *gitalypb.LogEntry {
	entry := &gitalypb.LogEntry{
		RelativePath: relativePath,
		ReferenceTransactions: []*gitalypb.LogEntry_ReferenceTransaction{
			{
				Changes: []*gitalypb.LogEntry_ReferenceTransaction_Change{
					{
						ReferenceName: []byte(ref),
						NewOid:        []byte(oid),
					},
				},
			},
		},
		Operations: []*gitalypb.LogEntry_Operation{
			{
				Operation: &gitalypb.LogEntry_Operation_CreateHardLink_{
					CreateHardLink: &gitalypb.LogEntry_Operation_CreateHardLink{
						SourcePath:      []byte("1"),
						DestinationPath: []byte(filepath.Join(relativePath, ref)),
					},
				},
			},
		},
	}

	if testhelper.IsReftableEnabled() {
		entry.Operations = append(entry.Operations, &gitalypb.LogEntry_Operation{
			Operation: &gitalypb.LogEntry_Operation_RemoveDirectoryEntry_{
				RemoveDirectoryEntry: &gitalypb.LogEntry_Operation_RemoveDirectoryEntry{
					Path: []byte(filepath.Join(relativePath, "reftable/tables.list")),
				},
			},
		}, &gitalypb.LogEntry_Operation{
			Operation: &gitalypb.LogEntry_Operation_CreateHardLink_{
				CreateHardLink: &gitalypb.LogEntry_Operation_CreateHardLink{
					SourcePath:      []byte("2"),
					DestinationPath: []byte(filepath.Join(relativePath, "reftable/tables.list")),
				},
			},
		})
	}

	return entry
}

// RefChange represents a reference change operation.
type RefChange struct {
	Ref string
	Oid git.ObjectID
}

// MultipleRefChangesLogEntry returns a log entry as a result of multiple reference update operations.
func MultipleRefChangesLogEntry(relativePath string, changes []RefChange) *gitalypb.LogEntry {
	entry := &gitalypb.LogEntry{RelativePath: relativePath}

	for i, change := range changes {
		entry.ReferenceTransactions = append(entry.ReferenceTransactions, &gitalypb.LogEntry_ReferenceTransaction{
			Changes: []*gitalypb.LogEntry_ReferenceTransaction_Change{
				{
					ReferenceName: []byte(change.Ref),
					NewOid:        []byte(change.Oid),
				},
			},
		})
		entry.Operations = append(entry.Operations, &gitalypb.LogEntry_Operation{
			Operation: &gitalypb.LogEntry_Operation_CreateHardLink_{
				CreateHardLink: &gitalypb.LogEntry_Operation_CreateHardLink{
					SourcePath:      []byte(fmt.Sprintf("%d", i+1)),
					DestinationPath: []byte(filepath.Join(relativePath, change.Ref)),
				},
			},
		})
	}

	if testhelper.IsReftableEnabled() {
		entry.Operations = append(entry.Operations, &gitalypb.LogEntry_Operation{
			Operation: &gitalypb.LogEntry_Operation_RemoveDirectoryEntry_{
				RemoveDirectoryEntry: &gitalypb.LogEntry_Operation_RemoveDirectoryEntry{
					Path: []byte(filepath.Join(relativePath, "reftable/tables.list")),
				},
			},
		}, &gitalypb.LogEntry_Operation{
			Operation: &gitalypb.LogEntry_Operation_CreateHardLink_{
				CreateHardLink: &gitalypb.LogEntry_Operation_CreateHardLink{
					SourcePath:      []byte("2"),
					DestinationPath: []byte(filepath.Join(relativePath, "reftable/tables.list")),
				},
			},
		})
	}

	return entry
}

// KVOperation represents a KV operation. Setting Value to nil is translated to key deletion.
type KVOperation struct {
	Key   []byte
	Value []byte
}

// MultipleKVLogEntry returns a log entry as a result of multiple key-value operations.
func MultipleKVLogEntry(relativePath string, ops []KVOperation) *gitalypb.LogEntry {
	entry := &gitalypb.LogEntry{RelativePath: relativePath}

	for _, op := range ops {
		if op.Value == nil {
			entry.Operations = append(entry.Operations, &gitalypb.LogEntry_Operation{
				Operation: &gitalypb.LogEntry_Operation_DeleteKey_{
					DeleteKey: &gitalypb.LogEntry_Operation_DeleteKey{
						Key: op.Key,
					},
				},
			})
		} else {
			entry.Operations = append(entry.Operations, &gitalypb.LogEntry_Operation{
				Operation: &gitalypb.LogEntry_Operation_SetKey_{
					SetKey: &gitalypb.LogEntry_Operation_SetKey{
						Key:   op.Key,
						Value: op.Value,
					},
				},
			})
		}
	}
	return entry
}
