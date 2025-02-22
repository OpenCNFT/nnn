package hook

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v16/internal/command"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/gitcmd"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/transaction"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v16/internal/transaction/voting"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
)

func TestMain(m *testing.M) {
	testhelper.Run(m)
}

func getExpectedEnv(tb testing.TB, ctx context.Context, locator storage.Locator, gitCmdFactory gitcmd.CommandFactory, repo *gitalypb.Repository) []string {
	repoPath, err := locator.GetRepoPath(ctx, repo, storage.WithRepositoryVerificationSkipped())
	require.NoError(tb, err)

	expectedEnv := map[string]string{
		"GIT_DIR":             repoPath,
		"GIT_TERMINAL_PROMPT": "0",
		"GL_ID":               "1234",
		"GL_PROJECT_PATH":     repo.GetGlProjectPath(),
		"GL_PROTOCOL":         "web",
		"GL_REPOSITORY":       repo.GetGlRepository(),
		"GL_USERNAME":         "user",
		"PWD":                 repoPath,
	}

	execEnv := gitCmdFactory.GetExecutionEnvironment(ctx)

	// This is really quite roundabout given that we'll convert it back to an array next, but
	// we need to deduplicate environment variables here.
	for _, allowedEnvVar := range append(command.AllowedEnvironment(os.Environ()), execEnv.EnvironmentVariables...) {
		kv := strings.SplitN(allowedEnvVar, "=", 2)
		require.Len(tb, kv, 2)
		expectedEnv[kv[0]] = kv[1]
	}

	expectedEnv["PATH"] = fmt.Sprintf("%s:%s", filepath.Dir(execEnv.BinaryPath), os.Getenv("PATH"))

	result := make([]string, 0, len(expectedEnv))
	for key, value := range expectedEnv {
		result = append(result, fmt.Sprintf("%s=%s", key, value))
	}
	sort.Strings(result)

	return result
}

func synchronizedVote(hook string) transaction.PhasedVote {
	return transaction.PhasedVote{
		Vote:  voting.VoteFromData([]byte("synchronize " + hook + " hook")),
		Phase: voting.Synchronized,
	}
}
