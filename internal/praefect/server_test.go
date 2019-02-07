package praefect_test

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/mwitkow/grpc-proxy/proxy"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly-proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/client"
	"gitlab.com/gitlab-org/gitaly/internal/praefect"
	"google.golang.org/grpc"
)

func TestServerRouting(t *testing.T) {
	prf := praefect.NewServer(nil, testLogger{t})

	listener, port := listenAvailPort(t)
	t.Logf("proxy listening on port %d", port)
	defer listener.Close()

	errQ := make(chan error)

	go func() {
		errQ <- prf.Start(listener)
	}()

	cc, err := dialLocalPort(t, port)
	require.NoError(t, err)
	defer cc.Close()

	mCli, _, cleanup := newMockDownstream(t)
	defer cleanup() // clean up mock downstream server resources

	prf.RegisterNode("test", mCli)

	gCli := gitalypb.NewRepositoryServiceClient(cc)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	resp, err := gCli.RepositoryExists(ctx, &gitalypb.RepositoryExistsRequest{})
	require.NoError(t, err)
	t.Logf("CalculateChecksum response: %#v", resp)

	t.Logf("Shutdown compelted: %s", prf.Shutdown(ctx))
	t.Logf("Server teminated: %s", <-errQ)
}

func listenAvailPort(tb testing.TB) (net.Listener, int) {
	listener, err := net.Listen("tcp", ":0")
	require.NoError(tb, err)

	return listener, listener.Addr().(*net.TCPAddr).Port
}

func dialLocalPort(tb testing.TB, port int) (*grpc.ClientConn, error) {
	return client.Dial(
		fmt.Sprintf("tcp://localhost:%d", port),
		[]grpc.DialOption{
			grpc.WithBlock(),
			grpc.WithDefaultCallOptions(grpc.CallCustomCodec(proxy.Codec())),
		},
	)
}

type testLogger struct {
	testing.TB
}

func (tl testLogger) Debugf(format string, args ...interface{}) {
	tl.TB.Logf(format, args...)
}

// initializes and returns a client to downstream server, downstream server, and cleanup function
func newMockDownstream(tb testing.TB) (*grpc.ClientConn, gitalypb.RepositoryServiceServer, func()) {
	// setup mock server
	m := &mockRepoSvc{
		srv: grpc.NewServer(),
	}
	gitalypb.RegisterRepositoryServiceServer(m.srv, m)
	lis, port := listenAvailPort(tb)

	// set up client to mock server
	cc, err := dialLocalPort(tb, port)
	require.NoError(tb, err)

	errQ := make(chan error)

	go func() {
		errQ <- m.srv.Serve(lis)
	}()

	cleanup := func() {
		m.srv.GracefulStop()
		lis.Close()
		cc.Close()
		require.NoError(tb, <-errQ)
	}

	return cc, m, cleanup
}
