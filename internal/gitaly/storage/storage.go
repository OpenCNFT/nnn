// Package storage contains the storage layer of Gitaly.
//
// Each Gitaly node contains one or more storages. Each storage has a name,
// and points to a directory on the file system where it stores its state.
//
// Each storage can contain one or more partitions. Storages are a collection of
// partitions. Data is stored within partitions.
//
// Partitions are accessed through transactions.
package storage

import (
	"context"

	"gitlab.com/gitlab-org/gitaly/v16/internal/git"
	housekeepingcfg "gitlab.com/gitlab-org/gitaly/v16/internal/git/housekeeping/config"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/keyvalue"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
)

// Transaction is a single unit-of-work that executes as a whole.
type Transaction interface {
	// Commit commits the transaction. It returns once the transaction's
	// changes have been durably persisted.
	Commit(context.Context) error
	// Rollback aborts the transactions and discards all of its changes.
	Rollback() error
	// SnapshotLSN returns the Log Sequence Number (LSN) of the transaction's snapshot. This value is used to track and order transactions.
	SnapshotLSN() LSN
	// Root is the absolute path to the root of the transaction's file system snapshot.
	Root() string
	// KV returns a ReadWriter that can be used to read or write the key-value state
	// in the transaction's snapshot.
	KV() keyvalue.ReadWriter
	// UpdateReferences updates the given references as part of the transaction. Each call is treated as
	// a different reference transaction. This allows for performing directory-file conflict inducing
	// changes in a transaction. For example:
	//
	// - First call  - delete 'refs/heads/parent'
	// - Second call - create 'refs/heads/parent/child'
	//
	// If a reference is updated multiple times during a transaction, its first recorded old OID used as
	// the old OID when verifying the reference update, and the last recorded new OID is used as the new
	// OID in the final commit. This means updates like 'oid-1 -> oid-2 -> oid-3' will ultimately be
	// committed as 'oid-1 -> oid-3'. The old OIDs of the intermediate states are not verified when
	// committing the write to the actual repository and are discarded from the final committed log
	// entry.
	UpdateReferences(git.ReferenceUpdates)
	// RecordInitialReferenceValues records the initial values of the references for the next UpdateReferences call. If oid is
	// not a zero OID, it's used as the initial value. If oid is a zero value, the reference's actual value is resolved.
	//
	// The reference's first recorded value is used as its old OID in the update. RecordInitialReferenceValues can be used to
	// record the value without staging an update in the transaction. This is useful for example generally recording the initial
	// value in the 'prepare' phase of the reference transaction hook before any changes are made without staging any updates
	// before the 'committed' phase is reached. The recorded initial values are only used for the next UpdateReferences call.
	RecordInitialReferenceValues(context.Context, map[git.ReferenceName]git.Reference) error
	// IncludeObject includes the given object and its dependencies in the transaction's logged pack file even
	// if the object is unreachable from the references.
	IncludeObject(git.ObjectID)
	// MarkDefaultBranchUpdated marks that the default branch has been updated. This is
	// necessary for the changes to HEAD to be committed.
	MarkDefaultBranchUpdated()
	// DeleteRepository deletes the repository when the transaction is committed.
	DeleteRepository()
	// MarkCustomHooksUpdated marks that the custom hooks have been updated. This is
	// necessary for the changes to custom hooks to be committed.
	MarkCustomHooksUpdated()
	// MarkAlternateUpdated marks that the 'objects/info/alternates' file has been updated or removed.
	// This is necessary for changes to be committed.
	MarkAlternateUpdated()
	// PackRefs runs reference repacking housekeeping when the transaction commits. If this
	// is called, the transaction is limited to running only other housekeeping tasks. No other
	// updates are allowed.
	PackRefs()
	// Repack runs object repacking housekeeping task when the transaction commits. If this
	// is called, the transaction is limited to running only other housekeeping tasks. No other
	// updates are allowed.
	Repack(housekeepingcfg.RepackObjectsConfig)
	// WriteCommitGraphs rewrites the commit graphs when the transaction commits. If this
	// is called, the transaction is limited to running only other housekeeping tasks. No other
	// updates are allowed.
	WriteCommitGraphs(housekeepingcfg.WriteCommitGraphConfig)
	// RewriteRepository rewrites the repository to point to the transaction's snapshot.
	RewriteRepository(*gitalypb.Repository) *gitalypb.Repository
	// OriginalRepository returns the repository as it was before rewriting it to point to the snapshot.
	OriginalRepository(*gitalypb.Repository) *gitalypb.Repository
}

// BeginOptions are used to configure a transaction that is being started.
type BeginOptions struct {
	// Write indicates whether this is a write transaction. Transactions
	// are read-only by default.
	Write bool
	// RelativePaths can be set to filter the relative paths that are included
	// in the transaction's snapshot. When set, only the contained relative paths
	// are included in the transaction's disk snapshot. If empty, nothing is
	// included in the transactions disk snapshot. If nil, no filtering is done
	// and the partition's full disk state is included in the snapshot.
	//
	// The first relative path is the target repository, and is the only repository
	// that can be written into.
	RelativePaths []string
	// ForceExclusiveSnapshot forces the transaction to use an exclusive snapshot.
	// This is a temporary workaround for some RPCs that do not work well with shared
	// read-only snapshots yet.
	ForceExclusiveSnapshot bool
}
