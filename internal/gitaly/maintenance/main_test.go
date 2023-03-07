//go:build !gitaly_test_sha256

package maintenance

import (
	"testing"

	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper"
)

func TestMain(m *testing.M) {
	testhelper.Run(m)
}
