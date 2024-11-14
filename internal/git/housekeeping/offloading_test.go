package housekeeping

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/transaction"
	"gitlab.com/gitlab-org/gitaly/v16/internal/helper/text"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper/testcfg"
)

func TestSetOffloadingGitConfig(t *testing.T) {
	t.Parallel()
	ctx := testhelper.Context(t)
	cfg := testcfg.Build(t)

	for _, tc := range []struct {
		desc           string
		url            string
		filter         string
		expectedErr    error
		expectedConfig []string
	}{
		{
			desc:        "can set offload remote",
			url:         "fake://offload/promisor/url",
			filter:      "blob:none",
			expectedErr: nil,
			expectedConfig: []string{
				"remote.offload.fetch +refs/heads/*:refs/remotes/offload/*",
				"remote.offload.partialclonefilter blob:none",
				"remote.offload.url fake://offload/promisor/url",
				"remote.offload.promisor true",
			},
		},
		{
			desc:           "fail when remote url is empty",
			url:            "",
			filter:         "blob:none",
			expectedErr:    fmt.Errorf("set offloading config: promisor remote url missing"),
			expectedConfig: []string{},
		},
		{
			desc:           "fail when filter is empty",
			url:            "fake://offload/promisor/url",
			filter:         "",
			expectedErr:    fmt.Errorf("set offloading config: filter missing"),
			expectedConfig: []string{},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg,
				gittest.CreateRepositoryConfig{
					SkipCreationViaService: true,
				})
			repo := localrepo.NewTestRepo(t, cfg, repoProto)

			require.Equal(t, tc.expectedErr,
				SetOffloadingGitConfig(ctx, repo, tc.url, tc.filter,
					&transaction.MockManager{}))

			if tc.expectedErr == nil {
				regex := fmt.Sprintf("remote.%s", OffloadingPromisorRemote)
				output := gittest.Exec(t, cfg, "-C", repoPath, "config", "--get-regexp", regex)
				require.ElementsMatch(t,
					tc.expectedConfig,
					strings.Split(text.ChompBytes(output), "\n"),
				)
			}
		})
	}
}
