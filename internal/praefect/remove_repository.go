package praefect

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v16/internal/log"
	"gitlab.com/gitlab-org/gitaly/v16/internal/praefect/datastore"
	"gitlab.com/gitlab-org/gitaly/v16/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

// RemoveRepositoryHandler intercepts RemoveRepository calls, deletes the database records and
// deletes the repository from every backing Gitaly node.
func RemoveRepositoryHandler(rs datastore.RepositoryStore, logger log.Logger, conns Connections) grpc.StreamHandler {
	return removeRepositoryHandler(rs, logger, conns,
		func(stream grpc.ServerStream) (*gitalypb.Repository, error) {
			var req gitalypb.RemoveRepositoryRequest
			if err := stream.RecvMsg(&req); err != nil {
				return nil, fmt.Errorf("receive request: %w", err)
			}

			repo := req.GetRepository()
			if repo == nil {
				return nil, structerr.NewInvalidArgument("%w", storage.ErrRepositoryNotSet)
			}

			return repo, nil
		},
		func(ctx context.Context, conn *grpc.ClientConn, rewritten *gitalypb.Repository) error {
			_, err := gitalypb.NewRepositoryServiceClient(conn).RemoveRepository(ctx, &gitalypb.RemoveRepositoryRequest{
				Repository: rewritten,
			})
			return err
		},
		func() proto.Message { return &gitalypb.RemoveRepositoryResponse{} },
		true,
	)
}

type requestParser func(grpc.ServerStream) (*gitalypb.Repository, error)

type requestProxier func(context.Context, *grpc.ClientConn, *gitalypb.Repository) error

type responseFactory func() proto.Message

func removeRepositoryHandler(rs datastore.RepositoryStore, logger log.Logger, conns Connections, parseRequest requestParser, proxyRequest requestProxier, buildResponse responseFactory, errorOnNotFound bool) grpc.StreamHandler {
	return func(_ interface{}, stream grpc.ServerStream) error {
		repo, err := parseRequest(stream)
		if err != nil {
			return err
		}

		ctx := stream.Context()

		virtualStorage := repo.GetStorageName()
		replicaPath, storages, err := rs.DeleteRepository(ctx, virtualStorage, repo.GetRelativePath())
		if err != nil {
			if errors.Is(err, datastore.ErrRepositoryNotFound) {
				if errorOnNotFound {
					return structerr.NewNotFound("repository does not exist")
				}

				return stream.SendMsg(buildResponse())
			}

			return fmt.Errorf("delete repository: %w", err)
		}

		var wg sync.WaitGroup

		// It's not critical these deletions complete as the background crawler will identify these repos as deleted.
		// To rather return a successful code to the client, we limit the timeout here to 10s.
		ctx, cancel := context.WithTimeout(stream.Context(), 10*time.Second)
		defer cancel()

		for _, storage := range storages {
			conn, ok := conns[virtualStorage][storage]
			if !ok {
				// There may be database records for object pools which exist on storages that are not configured in the
				// local Praefect. We'll just ignore them here and not explicitly attempt to delete them. They'll be handled
				// by the background cleaner like any other stale repository if the storages are returned to the configuration.
				continue
			}

			wg.Add(1)
			go func(rewrittenStorage string, conn *grpc.ClientConn) {
				defer wg.Done()

				rewritten := proto.Clone(repo).(*gitalypb.Repository)
				rewritten.StorageName = rewrittenStorage
				rewritten.RelativePath = replicaPath

				if err := proxyRequest(ctx, conn, rewritten); err != nil {
					logger.WithFields(log.Fields{
						"virtual_storage": virtualStorage,
						"relative_path":   repo.GetRelativePath(),
						"storage":         rewrittenStorage,
					}).WithError(err).ErrorContext(ctx, "failed deleting repository")
				}
			}(storage, conn)
		}

		wg.Wait()

		return stream.SendMsg(buildResponse())
	}
}
