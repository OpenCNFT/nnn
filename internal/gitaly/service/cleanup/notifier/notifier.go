package notifier

import (
	"context"

	"gitlab.com/gitlab-org/gitaly/v15/internal/git"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/catfile"
	"gitlab.com/gitlab-org/gitaly/v15/internal/helper/chunk"
	"gitlab.com/gitlab-org/gitaly/v15/proto/go/gitalypb"
)

// Notifier sends messages stating that an OID has been rewritten, looking
// up the type of the OID if necessary. It is not safe for concurrent use
type Notifier struct {
	objectReader catfile.ObjectReader
	chunker      *chunk.Chunker
}

// New instantiates a new Notifier
func New(ctx context.Context, catfileCache catfile.Cache, repo git.RepositoryExecutor, chunker *chunk.Chunker) (*Notifier, func(), error) {
	objectReader, cancel, err := catfileCache.ObjectReader(ctx, repo)
	if err != nil {
		return nil, nil, err
	}

	return &Notifier{objectReader: objectReader, chunker: chunker}, cancel, nil
}

// Notify builds a new message and sends it to the chunker
func (n *Notifier) Notify(ctx context.Context, oldOid, newOid string, isInternalRef bool) error {
	objectType := n.lookupType(ctx, newOid, isInternalRef)

	entry := &gitalypb.ApplyBfgObjectMapStreamResponse_Entry{
		Type:   objectType,
		OldOid: oldOid,
		NewOid: newOid,
	}

	return n.chunker.Send(entry)
}

func (n *Notifier) lookupType(ctx context.Context, oid string, isInternalRef bool) gitalypb.ObjectType {
	if isInternalRef {
		return gitalypb.ObjectType_COMMIT
	}

	info, err := n.objectReader.Info(ctx, git.Revision(oid))
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
