package blob

import (
	"context"
	"errors"
	"io"

	"gitlab.com/gitlab-org/gitaly/v16/internal/git"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/catfile"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/gitpipe"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v16/internal/helper/chunk"
	"gitlab.com/gitlab-org/gitaly/v16/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/v16/streamio"
	"google.golang.org/protobuf/proto"
)

func verifyListBlobsRequest(ctx context.Context, locator storage.Locator, req *gitalypb.ListBlobsRequest) error {
	if err := locator.ValidateRepository(ctx, req.GetRepository()); err != nil {
		return err
	}
	if len(req.GetRevisions()) == 0 {
		return errors.New("missing revisions")
	}
	for _, revision := range req.GetRevisions() {
		if err := git.ValidateRevision([]byte(revision), git.AllowPathScopedRevision(), git.AllowPseudoRevision()); err != nil {
			return structerr.NewInvalidArgument("invalid revision: %w", err).WithMetadata("revision", revision)
		}
	}
	return nil
}

// ListBlobs finds all blobs which are transitively reachable via a graph walk of the given set of
// revisions.
func (s *server) ListBlobs(req *gitalypb.ListBlobsRequest, stream gitalypb.BlobService_ListBlobsServer) error {
	if err := verifyListBlobsRequest(stream.Context(), s.locator, req); err != nil {
		return structerr.NewInvalidArgument("%w", err)
	}

	ctx := stream.Context()
	repo := s.localrepo(req.GetRepository())

	chunker := chunk.New(&blobSender{
		send: func(blobs []*gitalypb.ListBlobsResponse_Blob) error {
			return stream.Send(&gitalypb.ListBlobsResponse{
				Blobs: blobs,
			})
		},
	})

	revlistOptions := []gitpipe.RevlistOption{
		gitpipe.WithObjects(),
		gitpipe.WithObjectTypeFilter(gitpipe.ObjectTypeBlob),
	}

	revlistIter := gitpipe.Revlist(ctx, repo, req.GetRevisions(), revlistOptions...)

	if err := s.processBlobs(ctx, repo, revlistIter, nil, req.GetLimit(), req.GetBytesLimit(),
		func(oid string, size int64, contents []byte, path []byte) error {
			if !req.GetWithPaths() {
				path = nil
			}

			return chunker.Send(&gitalypb.ListBlobsResponse_Blob{
				Oid:  oid,
				Size: size,
				Data: contents,
				Path: path,
			})
		},
	); err != nil {
		return structerr.NewInternal("processing blobs: %w", err)
	}

	if err := chunker.Flush(); err != nil {
		return structerr.NewInternal("%w", err)
	}

	return nil
}

func (s *server) processBlobs(
	ctx context.Context,
	repo *localrepo.Repo,
	objectIter gitpipe.ObjectIterator,
	catfileInfoIter gitpipe.CatfileInfoIterator,
	blobsLimit uint32,
	bytesLimit int64,
	callback func(oid string, size int64, contents []byte, path []byte) error,
) error {
	// If we have a zero bytes limit, then the caller didn't request any blob contents at all.
	// We can thus skip reading blob contents completely.
	if bytesLimit == 0 {
		// This is a bit untidy, but some callers may already use an object info iterator to
		// enumerate objects, where it thus wouldn't make sense to recreate it via the
		// object iterator. We thus support an optional `catfileInfoIter` parameter: if set,
		// we just use that one and ignore the object iterator.
		if catfileInfoIter == nil {
			objectInfoReader, cancel, err := s.catfileCache.ObjectInfoReader(ctx, repo)
			if err != nil {
				return structerr.NewInternal("creating object info reader: %w", err)
			}
			defer cancel()

			catfileInfoIter, err = gitpipe.CatfileInfo(ctx, objectInfoReader, objectIter)
			if err != nil {
				return structerr.NewInternal("creating object info iterator: %w", err)
			}
		}

		var i uint32
		for catfileInfoIter.Next() {
			blob := catfileInfoIter.Result()

			if err := callback(
				blob.ObjectID().String(),
				blob.ObjectSize(),
				nil,
				blob.ObjectName,
			); err != nil {
				return structerr.NewInternal("sending blob chunk: %w", err)
			}

			i++
			if blobsLimit > 0 && i >= blobsLimit {
				break
			}
		}

		if err := catfileInfoIter.Err(); err != nil {
			return structerr.NewInternal("%w", err)
		}
	} else {
		objectReader, cancel, err := s.catfileCache.ObjectReader(ctx, repo)
		if err != nil {
			return structerr.NewInternal("creating object reader: %w", err)
		}
		defer cancel()

		catfileObjectIter, err := gitpipe.CatfileObject(ctx, objectReader, objectIter)
		if err != nil {
			return structerr.NewInternal("creating object iterator: %w", err)
		}

		var i uint32
		for catfileObjectIter.Next() {
			blob := catfileObjectIter.Result()

			headerSent := false
			dataChunker := streamio.NewWriter(func(p []byte) error {
				var oid string
				var size int64

				if !headerSent {
					oid = blob.ObjectID().String()
					size = blob.ObjectSize()
					headerSent = true
				}

				pCopy := make([]byte, len(p))
				copy(pCopy, p)

				if err := callback(oid, size, pCopy, blob.ObjectName); err != nil {
					return structerr.NewInternal("sending blob chunk: %w", err)
				}

				return nil
			})

			readLimit := bytesLimit
			if readLimit < 0 {
				readLimit = blob.ObjectSize()
			}

			_, err := io.CopyN(dataChunker, blob, readLimit)
			if err != nil && !errors.Is(err, io.EOF) {
				return structerr.NewInternal("sending blob data: %w", err)
			}

			// Discard trailing blob data in case the blob is bigger than the read
			// limit. We only do so in case we haven't yet seen `io.EOF`: if we did,
			// then the object may be closed already.
			if !errors.Is(err, io.EOF) {
				_, err = io.Copy(io.Discard, blob)
				if err != nil {
					return structerr.NewInternal("discarding blob data: %w", err)
				}
			}

			// If we still didn't send any header, then it probably means that the blob
			// itself didn't contain any data. Let's be prepared and send out the blob
			// header manually in that case.
			if !headerSent {
				if err := callback(
					blob.ObjectID().String(),
					blob.ObjectSize(),
					nil,
					blob.ObjectName,
				); err != nil {
					return structerr.NewInternal("sending blob chunk: %w", err)
				}
			}

			i++
			if blobsLimit > 0 && i >= blobsLimit {
				break
			}
		}

		if err := catfileObjectIter.Err(); err != nil {
			return structerr.NewInternal("%w", err)
		}
	}

	return nil
}

type blobSender struct {
	blobs []*gitalypb.ListBlobsResponse_Blob
	send  func([]*gitalypb.ListBlobsResponse_Blob) error
}

func (t *blobSender) Reset() {
	t.blobs = t.blobs[:0]
}

func (t *blobSender) Append(m proto.Message) {
	t.blobs = append(t.blobs, m.(*gitalypb.ListBlobsResponse_Blob))
}

func (t *blobSender) Send() error {
	return t.send(t.blobs)
}

// ListAllBlobs finds all blobs which exist in the repository, including those which are not
// reachable via graph walks.
func (s *server) ListAllBlobs(req *gitalypb.ListAllBlobsRequest, stream gitalypb.BlobService_ListAllBlobsServer) error {
	ctx := stream.Context()

	repository := req.GetRepository()
	if err := s.locator.ValidateRepository(stream.Context(), repository); err != nil {
		return err
	}

	repo := s.localrepo(repository)

	chunker := chunk.New(&allBlobsSender{
		send: func(blobs []*gitalypb.ListAllBlobsResponse_Blob) error {
			return stream.Send(&gitalypb.ListAllBlobsResponse{
				Blobs: blobs,
			})
		},
	})

	catfileInfoIter := gitpipe.CatfileInfoAllObjects(ctx, repo,
		gitpipe.WithSkipCatfileInfoResult(func(objectInfo *catfile.ObjectInfo) bool {
			return objectInfo.Type != "blob"
		}),
	)

	if err := s.processBlobs(ctx, repo, catfileInfoIter, catfileInfoIter, req.GetLimit(), req.GetBytesLimit(),
		func(oid string, size int64, contents []byte, path []byte) error {
			return chunker.Send(&gitalypb.ListAllBlobsResponse_Blob{
				Oid:  oid,
				Size: size,
				Data: contents,
			})
		},
	); err != nil {
		return structerr.NewInternal("processing blobs: %w", err)
	}

	if err := chunker.Flush(); err != nil {
		return structerr.NewInternal("flushing blobs: %w", err)
	}

	return nil
}

type allBlobsSender struct {
	blobs []*gitalypb.ListAllBlobsResponse_Blob
	send  func([]*gitalypb.ListAllBlobsResponse_Blob) error
}

func (t *allBlobsSender) Reset() {
	t.blobs = t.blobs[:0]
}

func (t *allBlobsSender) Append(m proto.Message) {
	t.blobs = append(t.blobs, m.(*gitalypb.ListAllBlobsResponse_Blob))
}

func (t *allBlobsSender) Send() error {
	return t.send(t.blobs)
}
