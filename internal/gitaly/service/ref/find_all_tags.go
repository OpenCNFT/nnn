package ref

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"

	"gitlab.com/gitlab-org/gitaly/v16/internal/git/catfile"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/gitpipe"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v16/internal/helper/chunk"
	"gitlab.com/gitlab-org/gitaly/v16/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
	"google.golang.org/protobuf/proto"
)

func (s *server) FindAllTags(in *gitalypb.FindAllTagsRequest, stream gitalypb.RefService_FindAllTagsServer) error {
	ctx := stream.Context()

	if err := s.validateFindAllTagsRequest(ctx, in); err != nil {
		return structerr.NewInvalidArgument("%w", err)
	}

	sortField, err := getTagSortField(in.GetSortBy())
	if err != nil {
		return structerr.NewInvalidArgument("%w", err)
	}

	opts := buildPaginationOpts(ctx, in.GetPaginationParams())

	repo := s.localrepo(in.GetRepository())

	if err := s.findAllTags(ctx, repo, sortField, stream, opts); err != nil {
		return structerr.NewInternal("%w", err)
	}

	return nil
}

func (s *server) findAllTags(ctx context.Context, repo *localrepo.Repo, sortField string, stream gitalypb.RefService_FindAllTagsServer, opts *paginationOpts) error {
	objectReader, cancel, err := s.catfileCache.ObjectReader(ctx, repo)
	if err != nil {
		return fmt.Errorf("creating object reader: %w", err)
	}
	defer cancel()

	forEachRefIter := gitpipe.ForEachRef(
		ctx,
		repo,
		[]string{"refs/tags/"},
		gitpipe.WithSortField(sortField),
		gitpipe.WithForEachRefFormat("%(objectname) %(refname)%(if)%(*objectname)%(then)\n%(objectname)^{} PEELED%(end)"),
	)

	catfileObjectsIter, err := gitpipe.CatfileObject(ctx, objectReader, forEachRefIter)
	if err != nil {
		return fmt.Errorf("creating cat-file object iterator: %w", err)
	}

	chunker := chunk.New(&tagSender{stream: stream})

	// If `PageToken` is not provided, then `IsPageToken` will always return `true`
	// and disable pagination logic. If `PageToken` is set, then we will skip all tags
	// until we reach the tag equal to `PageToken`. After that, tags will be returned
	// as usual.
	pastPageToken := opts.IsPageToken([]byte{})
	limit := opts.Limit
	i := 0

	parser := catfile.NewParser()

	for catfileObjectsIter.Next() {
		tag := catfileObjectsIter.Result()

		if i >= limit {
			break
		}

		var result *gitalypb.Tag
		switch tag.ObjectType() {
		case "tag":
			var err error
			result, err = parser.ParseTag(tag)
			if err != nil {
				return fmt.Errorf("parsing annotated tag: %w", err)
			}
			catfile.TrimTagMessage(result)

			// For each tag, we expect both the tag itself as well as its
			// potentially-peeled tagged object.
			if !catfileObjectsIter.Next() {
				return errors.New("expected peeled tag")
			}

			peeledTag := catfileObjectsIter.Result()

			// We only need to parse the tagged object in case we have an annotated tag
			// which refers to a commit object. Otherwise, we discard the object's
			// contents.
			if peeledTag.ObjectType() == "commit" {
				commit, err := parser.ParseCommit(peeledTag)
				if err != nil {
					return fmt.Errorf("parsing tagged commit: %w", err)
				}
				result.TargetCommit = commit.GitCommit
			} else {
				if _, err := io.Copy(io.Discard, peeledTag); err != nil {
					return fmt.Errorf("discarding tagged object contents: %w", err)
				}
			}
		case "commit":
			commit, err := parser.ParseCommit(tag)
			if err != nil {
				return fmt.Errorf("parsing tagged commit: %w", err)
			}

			result = &gitalypb.Tag{
				Id:           tag.ObjectID().String(),
				TargetCommit: commit.GitCommit,
			}
		default:
			if _, err := io.Copy(io.Discard, tag); err != nil {
				return fmt.Errorf("discarding tag object contents: %w", err)
			}

			result = &gitalypb.Tag{
				Id: tag.ObjectID().String(),
			}
		}

		// In case we can deduce the tag name from the object name (which should typically
		// be the case), we always want to return the tag name. While annotated tags do have
		// their name encoded in the object itself, we instead want to default to the name
		// of the reference such that we can discern multiple refs pointing to the same tag.
		if tagName := bytes.TrimPrefix(tag.ObjectName, []byte("refs/tags/")); len(tagName) > 0 {
			result.Name = tagName
		}

		if !pastPageToken {
			pastPageToken = opts.IsPageToken(tag.ObjectName)
			continue
		}

		if err := chunker.Send(result); err != nil {
			return fmt.Errorf("sending tag: %w", err)
		}

		i++
	}

	if !pastPageToken {
		return structerr.NewInvalidArgument("could not find page token")
	}

	if err := catfileObjectsIter.Err(); err != nil {
		return fmt.Errorf("iterating over tags: %w", err)
	}

	if err := chunker.Flush(); err != nil {
		return fmt.Errorf("flushing chunker: %w", err)
	}

	return nil
}

func (s *server) validateFindAllTagsRequest(ctx context.Context, request *gitalypb.FindAllTagsRequest) error {
	repository := request.GetRepository()
	if err := s.locator.ValidateRepository(ctx, repository); err != nil {
		return err
	}

	return nil
}

type tagSender struct {
	tags   []*gitalypb.Tag
	stream gitalypb.RefService_FindAllTagsServer
}

func (t *tagSender) Reset() {
	t.tags = t.tags[:0]
}

func (t *tagSender) Append(m proto.Message) {
	t.tags = append(t.tags, m.(*gitalypb.Tag))
}

func (t *tagSender) Send() error {
	return t.stream.Send(&gitalypb.FindAllTagsResponse{
		Tags: t.tags,
	})
}

// getTagSortField returns a field that needs to be used to sort the tags.
// If sorting is not provided the default sorting is used: by refname.
func getTagSortField(sortBy *gitalypb.FindAllTagsRequest_SortBy) (string, error) {
	if sortBy == nil {
		return "", nil
	}

	var dir string
	switch sortBy.GetDirection() {
	case gitalypb.SortDirection_ASCENDING:
		dir = ""
	case gitalypb.SortDirection_DESCENDING:
		dir = "-"
	default:
		return "", fmt.Errorf("unsupported sorting direction: %s", sortBy.GetDirection())
	}

	var key string
	switch sortBy.GetKey() {
	case gitalypb.FindAllTagsRequest_SortBy_REFNAME:
		key = "refname"
	case gitalypb.FindAllTagsRequest_SortBy_CREATORDATE:
		key = "creatordate"
	case gitalypb.FindAllTagsRequest_SortBy_VERSION_REFNAME:
		key = "version:refname"
	default:
		return "", fmt.Errorf("unsupported sorting key: %s", sortBy.GetKey())
	}

	return dir + key, nil
}
