package smarthttp

import (
	"net"
	"testing"

	"gitlab.com/gitlab-org/gitaly-proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

const (
	pktFlushStr = "0000"
)

func runSmartHTTPServer(t *testing.T) (*grpc.Server, string) {
	server := testhelper.NewTestGrpcServer(t, nil, nil)

	serverSocketPath := testhelper.GetTemporaryGitalySocketFileName()
	listener, err := net.Listen("unix", serverSocketPath)
	if err != nil {
		t.Fatal(err)
	}

	gitalypb.RegisterSmartHTTPServiceServer(server, NewServer())
	reflection.Register(server)

	go server.Serve(listener)

	return server, "unix://" + serverSocketPath
}

func newSmartHTTPClient(t *testing.T, serverSocketPath string) (gitalypb.SmartHTTPServiceClient, *grpc.ClientConn) {
	connOpts := []grpc.DialOption{
		grpc.WithInsecure(),
	}
	conn, err := grpc.Dial(serverSocketPath, connOpts...)
	if err != nil {
		t.Fatal(err)
	}

	return gitalypb.NewSmartHTTPServiceClient(conn), conn
}
