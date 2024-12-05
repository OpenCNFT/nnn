package transactiontest

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
	"google.golang.org/grpc"
)

// ForceWALSyncWriteRef provides a temporary workaround that allows tests to assert against the state of a repository
// after any pending changes are applied. It does this by invoking an RPC that will start a new transaction, which
// will be forced to wait for previous changes to be applied before the RPC returns.
func ForceWALSyncWriteRef(tb testing.TB, ctx context.Context, revision string, conn *grpc.ClientConn, repo *gitalypb.Repository) {
	client := gitalypb.NewRepositoryServiceClient(conn)
	_, err := client.WriteRef(ctx, &gitalypb.WriteRefRequest{
		Repository: repo,
		Ref:        []byte(fmt.Sprintf("refs/heads/temp-%d", time.Now().UnixNano())), // Use unique ref name,
		Revision:   []byte(revision),
	})
	require.NoError(tb, err)
}
