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

func TestResetOffloadingGitConfig(t *testing.T) {
	t.Parallel()
	ctx := testhelper.Context(t)
	cfg := testcfg.Build(t)

	t.Run("only remove offloading promisor Git configurations", func(t *testing.T) {
		repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg,
			gittest.CreateRepositoryConfig{
				SkipCreationViaService: true,
			})
		repo := localrepo.NewTestRepo(t, cfg, repoProto)

		// Get a snapshot of repository configuration before applying configuration changes.
		output := gittest.Exec(t, cfg, "-C", repoPath, "config", "--list", "--local")
		beforeConfigs := strings.Split(text.ChompBytes(output), "\n")

		// Apply an offload configuration.
		url := "fake://offload/promisor/url"
		filter := "blob:none"
		require.NoError(t, SetOffloadingGitConfig(ctx, repo, url, filter, &transaction.MockManager{}))
		require.NoError(t, SetOffloadingGitConfig(ctx, repo, url, filter, &transaction.MockManager{}))

		// Ensure that the configuration changes have been successfully applied.
		output = gittest.Exec(t, cfg, "-C", repoPath, "config", "--list", "--local")
		modifiedConfigs := strings.Split(text.ChompBytes(output), "\n")
		require.ElementsMatch(t, append([]string{
			fmt.Sprintf("remote.%s.fetch=+refs/heads/*:refs/remotes/%s/*", OffloadingPromisorRemote, OffloadingPromisorRemote),
			fmt.Sprintf("remote.%s.partialclonefilter=%s", OffloadingPromisorRemote, filter),
			fmt.Sprintf("remote.%s.url=%s", OffloadingPromisorRemote, url),
			fmt.Sprintf("remote.%s.promisor=true", OffloadingPromisorRemote),
		}, beforeConfigs...), modifiedConfigs)

		// Execute the reset operation to roll back the configuration.
		require.NoError(t, ResetOffloadingGitConfig(ctx, repo, &transaction.MockManager{}))

		// Other unrelated configurations remain unaffected.
		// The modified configurations are restored to their original state.
		output = gittest.Exec(t, cfg, "-C", repoPath, "config", "--list", "--local")
		afterConfigs := strings.Split(text.ChompBytes(output), "\n")
		require.ElementsMatch(t, beforeConfigs, afterConfigs)
	})
}
