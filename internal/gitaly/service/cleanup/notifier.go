package cleanup

import (
	"context"

	"gitlab.com/gitlab-org/gitaly/v16/internal/git"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/catfile"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/gitcmd"
	"gitlab.com/gitlab-org/gitaly/v16/internal/helper/chunk"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
)

// notifier sends messages stating that an OID has been rewritten, looking
// up the type of the OID if necessary. It is not safe for concurrent use
type notifier struct {
	objectInfoReader catfile.ObjectInfoReader
	chunker          *chunk.Chunker
}

// newNotifier instantiates a new notifier.
func newNotifier(ctx context.Context, catfileCache catfile.Cache, repo gitcmd.RepositoryExecutor, chunker *chunk.Chunker) (*notifier, func(), error) {
	objectInfoReader, cancel, err := catfileCache.ObjectInfoReader(ctx, repo)
	if err != nil {
		return nil, nil, err
	}

	return &notifier{objectInfoReader: objectInfoReader, chunker: chunker}, cancel, nil
}

// notify builds a new message and sends it to the chunker
func (n *notifier) notify(ctx context.Context, oldOid, newOid string, isInternalRef bool) error {
	objectType := n.lookupType(ctx, newOid, isInternalRef)

	entry := &gitalypb.ApplyBfgObjectMapStreamResponse_Entry{
		Type:   objectType,
		OldOid: oldOid,
		NewOid: newOid,
	}

	return n.chunker.Send(entry)
}

func (n *notifier) lookupType(ctx context.Context, oid string, isInternalRef bool) gitalypb.ObjectType {
	if isInternalRef {
		return gitalypb.ObjectType_COMMIT
	}

	info, err := n.objectInfoReader.Info(ctx, git.Revision(oid))
	if err != nil {
		return gitalypb.ObjectType_UNKNOWN
	}

	switch info.Type {
	case "commit":
		return gitalypb.ObjectType_COMMIT
	case "blob":
		return gitalypb.ObjectType_BLOB
	case "tree":
		return gitalypb.ObjectType_TREE
	case "tag":
		return gitalypb.ObjectType_TAG
	default:
		return gitalypb.ObjectType_UNKNOWN
	}
}
