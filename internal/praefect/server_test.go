package praefect_test

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/mwitkow/grpc-proxy/proxy"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/client"
	"gitlab.com/gitlab-org/gitaly/internal/praefect"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/mock"
	"google.golang.org/grpc"
)

// TestServerSimpleUnaryUnary verifies that the Praefect server is capable of
// routing a specific unary request to and unary response from a backend server
func TestServerSimpleUnaryUnary(t *testing.T) {
	testCases := []struct {
		name string

		// callback is the actual RPC implementation
		callback simpleUnaryUnaryCallback

		// all inputs and outputs for RPC SimpleUnaryUnary
		request      *mock.SimpleRequest
		expectResp   *mock.SimpleResponse
		expectErrStr string
	}{
		{
			name:     "simple request with response",
			callback: callbackIncrement,
			request: &mock.SimpleRequest{
				Value: 1,
			},
			expectResp: &mock.SimpleResponse{
				Value: 2,
			},
		},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			prf := praefect.NewServer(nil, testLogger{t})

			listener, port := listenAvailPort(t)
			t.Logf("proxy listening on port %d", port)
			defer listener.Close()

			errQ := make(chan error)

			go func() {
				errQ <- prf.Start(listener)
			}()

			// dial client to praefect
			cc := dialLocalPort(t, port, false)
			defer cc.Close()
			cli := mock.NewSimpleServiceClient(cc)

			backend, cleanup := newMockDownstream(t, tt.callback)
			defer cleanup() // clean up mock downstream server resources

			prf.RegisterNode("test", backend)

			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()

			resp, err := cli.SimpleUnaryUnary(ctx, tt.request)
			if err != nil {
				require.EqualError(t, err, tt.expectErrStr)
			}
			require.Equal(t, tt.expectResp, resp)

			err = prf.Shutdown(ctx)
			require.NoError(t, err)
			require.NoError(t, <-errQ)
		})
	}
}

func callbackIncrement(_ context.Context, req *mock.SimpleRequest) (*mock.SimpleResponse, error) {
	return &mock.SimpleResponse{
		Value: req.Value + 1,
	}, nil
}

func TestRegisteringSecondStorageLocation(t *testing.T) {
	prf := praefect.NewServer(nil, testLogger{t})

	mCli, cleanup := newMockDownstream(t, nil)
	defer cleanup() // clean up mock downstream server resources

	assert.NoError(t, prf.RegisterNode("1", mCli))
	assert.Error(t, prf.RegisterNode("2", mCli))

}

func listenAvailPort(tb testing.TB) (net.Listener, int) {
	listener, err := net.Listen("tcp", ":0")
	require.NoError(tb, err)

	return listener, listener.Addr().(*net.TCPAddr).Port
}

func dialLocalPort(tb testing.TB, port int, backend bool) *grpc.ClientConn {
	opts := []grpc.DialOption{
		grpc.WithBlock(),
	}
	if backend {
		opts = append(
			opts,
			grpc.WithDefaultCallOptions(grpc.CallCustomCodec(proxy.Codec())),
		)
	}

	cc, err := client.Dial(
		fmt.Sprintf("tcp://localhost:%d", port),
		opts,
	)
	require.NoError(tb, err)

	return cc
}

type testLogger struct {
	testing.TB
}

func (tl testLogger) Debugf(format string, args ...interface{}) {
	tl.TB.Logf(format, args...)
}

// initializes and returns a client to downstream server, downstream server, and cleanup function
func newMockDownstream(tb testing.TB, callback simpleUnaryUnaryCallback) (string, func()) {
	// setup mock server
	m := &mockSvc{
		simpleUnaryUnary: callback,
	}

	srv := grpc.NewServer()
	mock.RegisterSimpleServiceServer(srv, m)

	// client to backend service
	lis, port := listenAvailPort(tb)

	errQ := make(chan error)

	go func() {
		errQ <- srv.Serve(lis)
	}()

	cleanup := func() {
		srv.GracefulStop()
		lis.Close()

		// If the server is shutdown before Serve() is called on it
		// the Serve() calls will return the ErrServerStopped
		if err := <-errQ; err != nil && err != grpc.ErrServerStopped {
			require.NoError(tb, err)
		}
	}

	return fmt.Sprintf("tcp://localhost:%d", port), cleanup
}
