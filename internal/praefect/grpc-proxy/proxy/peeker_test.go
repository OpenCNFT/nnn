package proxy_test

import (
	"io"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/grpc-proxy/proxy"
	"golang.org/x/net/context"
	"google.golang.org/grpc"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	testservice "gitlab.com/gitlab-org/gitaly/internal/praefect/grpc-proxy/testdata"
)

// TestStreamPeeking demonstrates that a director function is able to peek
// into a stream. Further more, it demonstrates that peeking into a stream
// will not disturb the stream sent from the proxy client to the backend.
func TestStreamPeeking(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	backendCC, backendSrvr, cleanupPinger := newBackendPinger(t, ctx)
	defer cleanupPinger()

	pingReqSent := &testservice.PingRequest{Value: "hi"}

	// director will peek into stream before routing traffic
	director := func(ctx context.Context, fullMethodName string, peeker proxy.StreamModifier) (context.Context, *grpc.ClientConn, func(), error) {
		t.Logf("director routing method %s to backend", fullMethodName)

		peekedMsg, err := peeker.Peek()
		require.NoError(t, err)

		peekedRequest := new(testservice.PingRequest)
		err = proto.Unmarshal(peekedMsg, peekedRequest)
		require.NoError(t, err)
		require.Equal(t, pingReqSent, peekedRequest)

		return ctx, backendCC, nil, nil
	}

	pingResp := &testservice.PingResponse{
		Counter: 1,
	}

	// we expect the backend server to receive the peeked message
	backendSrvr.pingStream = func(stream testservice.TestService_PingStreamServer) error {
		pingReqReceived, err := stream.Recv()
		assert.NoError(t, err)
		assert.Equal(t, pingReqSent, pingReqReceived)

		return stream.Send(pingResp)
	}

	proxyCC, cleanupProxy := newProxy(t, ctx, director, "mwitkow.testproto.TestService", "PingStream")
	defer cleanupProxy()

	proxyClient := testservice.NewTestServiceClient(proxyCC)

	proxyClientPingStream, err := proxyClient.PingStream(ctx)
	require.NoError(t, err)
	defer proxyClientPingStream.CloseSend()

	require.NoError(t,
		proxyClientPingStream.Send(pingReqSent),
	)

	resp, err := proxyClientPingStream.Recv()
	require.NoError(t, err)
	require.Equal(t, resp, pingResp)

	_, err = proxyClientPingStream.Recv()
	require.Error(t, io.EOF, err)
}

func TestStreamInjecting(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	backendCC, backendSrvr, cleanupPinger := newBackendPinger(t, ctx)
	defer cleanupPinger()

	pingReqSent := &testservice.PingRequest{Value: "hi"}
	newValue := "bye"

	// director will peek into stream and change some frames
	director := func(ctx context.Context, fullMethodName string, peeker proxy.StreamModifier) (context.Context, *grpc.ClientConn, func(), error) {
		t.Logf("modifying request for method %s", fullMethodName)

		peekedMsg, err := peeker.Peek()
		require.NoError(t, err)

		peekedRequest := new(testservice.PingRequest)
		require.NoError(t, proto.Unmarshal(peekedMsg, peekedRequest))
		require.Equal(t, "hi", peekedRequest.GetValue())

		peekedRequest.Value = newValue

		newPayload, err := proto.Marshal(peekedRequest)
		require.NoError(t, err)

		require.NoError(t, peeker.Modify(newPayload))

		return ctx, backendCC, nil, nil
	}

	pingResp := &testservice.PingResponse{
		Counter: 1,
	}

	// we expect the backend server to receive the modified message
	backendSrvr.pingStream = func(stream testservice.TestService_PingStreamServer) error {
		pingReqReceived, err := stream.Recv()
		assert.NoError(t, err)
		assert.Equal(t, newValue, pingReqReceived.GetValue())

		return stream.Send(pingResp)
	}

	proxyCC, cleanupProxy := newProxy(t, ctx, director, "mwitkow.testproto.TestService", "PingStream")
	defer cleanupProxy()

	proxyClient := testservice.NewTestServiceClient(proxyCC)

	proxyClientPingStream, err := proxyClient.PingStream(ctx)
	require.NoError(t, err)
	defer proxyClientPingStream.CloseSend()

	require.NoError(t,
		proxyClientPingStream.Send(pingReqSent),
	)

	resp, err := proxyClientPingStream.Recv()
	require.NoError(t, err)
	require.Equal(t, resp, pingResp)

	_, err = proxyClientPingStream.Recv()
	require.Error(t, io.EOF, err)
}
