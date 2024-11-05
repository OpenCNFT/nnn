package storage

import (
	"context"

	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/keyvalue"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
)

// WriteAheadLog is an abstract interface for Gitaly's WAL.
// It defines methods to manage log entries in a write-ahead log system for use with Raft consensus.
type WriteAheadLog interface {
	// AppendLogEntry appends a new log entry at the specified LSN (Log Sequence Number),
	// using the provided log entry data and file path, ensuring the entry is durably written.
	AppendLogEntry(ctx context.Context, lsn LSN, entry *gitalypb.LogEntry, filePath string) error

	// CommitLogEntry marks the log entry at the given LSN as committed, which means it has been accepted
	// by a majority of Raft members and can be safely applied to the state machine.
	CommitLogEntry(ctx context.Context, lsn LSN) error

	// NotifyNewCommittedEntry notifies WAL that a log entry has been committed, allowing them to proceed with
	// processing. The caller should only notify new commits if WAL is idle or the new committed entries are
	// issued by Raft itself. Otherwise, WAL is waken up too frequently.
	NotifyNewCommittedEntry()

	// InsertLogEntry inserts a KV log entry into the WAL at the specified LSN. The caller can manipulate the data
	// by passing a modification function. It's responsibility of the caller NOT TO append/insert any log entries
	// while this function is running. All adjacent log entries at and after inserting position are wiped.
	InsertLogEntry(ctx context.Context, lsn LSN, txnFunc func(keyvalue.ReadWriter) error, metadata *gitalypb.LogEntry_Metadata) (*gitalypb.LogEntry, error)

	// ReadLogEntry reads and returns the log entry stored at the specified LSN within the WAL.
	ReadLogEntry(lsn LSN) (*gitalypb.LogEntry, error)

	// LastLSN returns the last LSN that has been appended to the WAL, indicating the latest position.
	LastLSN() LSN

	// FirstLSN retrieves the first LSN available in the WAL, useful for identifying the oldest log entry retained.
	FirstLSN() LSN

	// LSNDirPath provides the directory path where files corresponding to a specific LSN are stored.
	LSNDirPath(lsn LSN) string
}
