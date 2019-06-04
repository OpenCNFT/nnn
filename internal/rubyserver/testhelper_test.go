package rubyserver

import (
	"os"
	"testing"

	"gitlab.com/gitlab-org/gitaly-proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
)

var (
	testRepo *gitalypb.Repository
)

func TestMain(m *testing.M) {
	os.Exit(testMain(m))
}

func testMain(m *testing.M) int {
	defer testhelper.MustHaveNoChildProcess()

	testRepo = testhelper.TestRepository()

	return m.Run()
}
