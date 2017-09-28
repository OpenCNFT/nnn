package commit

import (
	"bytes"
	"net"
	"os"
	"testing"
	"time"

	"gitlab.com/gitlab-org/gitaly/internal/linguist"
	"gitlab.com/gitlab-org/gitaly/internal/rubyserver"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"

	"github.com/golang/protobuf/ptypes/timestamp"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	pb "gitlab.com/gitlab-org/gitaly-proto/go"
)

var (
	testRepo         = testhelper.TestRepository()
	serverSocketPath = testhelper.GetTemporaryGitalySocketFileName()
)

func TestMain(m *testing.M) {
	os.Exit(testMain(m))
}

var rubyServer *rubyserver.Server

func testMain(m *testing.M) int {
	defer testhelper.MustHaveNoChildProcess()

	testhelper.ConfigureRuby()
	if err := linguist.LoadColors(); err != nil {
		log.Fatal(err)
	}

	var err error
	rubyServer, err = rubyserver.Start()
	if err != nil {
		log.Fatal(err)
	}
	defer rubyServer.Stop()
	return m.Run()
}

func startTestServices(t *testing.T) *grpc.Server {
	server := testhelper.NewTestGrpcServer(t, nil, nil)

	if err := os.RemoveAll(serverSocketPath); err != nil {
		t.Fatal(err)
	}

	listener, err := net.Listen("unix", serverSocketPath)
	if err != nil {
		t.Fatal("failed to start server")
	}

	pb.RegisterCommitServiceServer(server, NewServer(rubyServer))
	reflection.Register(server)

	go server.Serve(listener)
	return server
}

func newCommitServiceClient(t *testing.T, serviceSocketPath string) (pb.CommitServiceClient, *grpc.ClientConn) {
	connOpts := []grpc.DialOption{
		grpc.WithInsecure(),
		grpc.WithDialer(func(addr string, _ time.Duration) (net.Conn, error) {
			return net.Dial("unix", addr)
		}),
	}
	conn, err := grpc.Dial(serviceSocketPath, connOpts...)
	if err != nil {
		t.Fatal(err)
	}

	return pb.NewCommitServiceClient(conn), conn
}

func treeEntriesEqual(a, b *pb.TreeEntry) bool {
	return a.CommitOid == b.CommitOid && a.Oid == b.Oid && a.Mode == b.Mode &&
		bytes.Equal(a.Path, b.Path) && bytes.Equal(a.FlatPath, b.FlatPath) &&
		a.RootOid == b.RootOid && a.Type == b.Type
}

func dummyCommitAuthor(ts int64) *pb.CommitAuthor {
	return &pb.CommitAuthor{
		Name:  []byte("Ahmad Sherif"),
		Email: []byte("ahmad+gitlab-test@gitlab.com"),
		Date:  &timestamp.Timestamp{Seconds: ts},
	}
}
