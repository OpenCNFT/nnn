package ssh

import (
	"net"
	"os"
	"path"
	"testing"
	"time"

	log "github.com/sirupsen/logrus"

	pb "gitlab.com/gitlab-org/gitaly-proto/go"

	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

const (
	testPath     = "testdata"
	testRepoRoot = testPath + "/data"
)

var (
	testRepo      *pb.Repository
	gitalySSHPath string
	cwd           string
)

func TestMain(m *testing.M) {
	os.Exit(testMain(m))
}

func testMain(m *testing.M) int {
	defer testhelper.MustHaveNoChildProcess()

	cwd = mustGetCwd()

	err := os.RemoveAll(testPath)
	if err != nil {
		log.Fatal(err)
	}

	testRepo = testhelper.TestRepository()

	// Build the test-binary that we need
	os.Remove("gitaly-ssh")
	testhelper.MustRunCommand(nil, nil, "go", "build", "gitlab.com/gitlab-org/gitaly/cmd/gitaly-ssh")
	defer os.Remove("gitaly-ssh")
	gitalySSHPath = path.Join(cwd, "gitaly-ssh")

	return m.Run()
}

func mustGetCwd() string {
	wd, err := os.Getwd()
	if err != nil {
		log.Panic(err)
	}
	return wd
}

func runSSHServer(t *testing.T) (*grpc.Server, string) {
	server := testhelper.NewTestGrpcServer(t, nil, nil)

	serverSocketPath := testhelper.GetTemporaryGitalySocketFileName()
	listener, err := net.Listen("unix", serverSocketPath)
	if err != nil {
		t.Fatal(err)
	}

	pb.RegisterSSHServiceServer(server, NewServer())
	reflection.Register(server)

	go server.Serve(listener)

	return server, serverSocketPath
}

func newSSHClient(t *testing.T, serverSocketPath string) (pb.SSHServiceClient, *grpc.ClientConn) {
	connOpts := []grpc.DialOption{
		grpc.WithInsecure(),
		grpc.WithDialer(func(addr string, timeout time.Duration) (net.Conn, error) {
			return net.DialTimeout("unix", addr, timeout)
		}),
	}
	conn, err := grpc.Dial(serverSocketPath, connOpts...)
	if err != nil {
		t.Fatal(err)
	}

	return pb.NewSSHServiceClient(conn), conn
}
