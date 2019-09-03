package blob

import (
	"log"
	"net"
	"os"
	"testing"

	"gitlab.com/gitlab-org/gitaly/internal/rubyserver"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

var rubyServer = &rubyserver.Server{}

func TestMain(m *testing.M) {
	os.Exit(testMain(m))
}

func testMain(m *testing.M) int {
	defer testhelper.MustHaveNoChildProcess()

	testhelper.ConfigureRuby()
	if err := rubyServer.Start(); err != nil {
		log.Fatal(err)
	}
	defer rubyServer.Stop()

	return m.Run()
}

func runBlobServer(t *testing.T) (*grpc.Server, string) {
	grpcServer := testhelper.NewTestGrpcServer(t, nil, nil)

	serverSocketPath := testhelper.GetTemporaryGitalySocketFileName()
	listener, err := net.Listen("unix", serverSocketPath)

	if err != nil {
		t.Fatal(err)
	}

	gitalypb.RegisterBlobServiceServer(grpcServer, &server{rubyServer})
	reflection.Register(grpcServer)

	go grpcServer.Serve(listener)

	return grpcServer, "unix://" + serverSocketPath
}

func newBlobClient(t *testing.T, serverSocketPath string) (gitalypb.BlobServiceClient, *grpc.ClientConn) {
	connOpts := []grpc.DialOption{
		grpc.WithInsecure(),
	}
	conn, err := grpc.Dial(serverSocketPath, connOpts...)
	if err != nil {
		t.Fatal(err)
	}

	return gitalypb.NewBlobServiceClient(conn), conn
}
