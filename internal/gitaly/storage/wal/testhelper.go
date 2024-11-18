package wal

import (
	"fmt"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/mode"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
	"google.golang.org/protobuf/proto"
)

// ManifestDirectoryEntry asserts the manifest file inside the log entry directory.
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

// RefChange represents a reference change operation.
type RefChange struct {
	Ref string
	Oid git.ObjectID
}

// MultiplerefChangesLogEntry returns a log entry as a result of multiple reference update operations.
func MultiplerefChangesLogEntry(relativePath string, changes []RefChange) *gitalypb.LogEntry {
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
