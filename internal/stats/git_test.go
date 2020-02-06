package stats

import (
	"bytes"
	"encoding/json"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus/ctxlogrus"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/helper"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
)

func TestLogObjectInfo(t *testing.T) {
	repo1, repoPath1, cleanup1 := testhelper.NewTestRepo(t)
	defer cleanup1()

	repo2, repoPath2, cleanup2 := testhelper.NewTestRepo(t)
	defer cleanup2()

	ctx, cancel := testhelper.Context()
	defer cancel()

	logBuffer := &bytes.Buffer{}
	log := &logrus.Logger{Out: logBuffer, Formatter: new(logrus.JSONFormatter), Level: logrus.InfoLevel}
	testCtx := ctxlogrus.ToContext(ctx, log.WithField("test", "logging"))

	requireLog := func(msg string) map[string]interface{} {
		var out map[string]interface{}
		require.NoError(t, json.NewDecoder(strings.NewReader(msg)).Decode(&out))
		const key = "count_objects"
		require.Contains(t, out, key, "there is no any information about statistics")
		countObjects := out[key].(map[string]interface{})
		require.Contains(t, countObjects, "count")
		require.Contains(t, countObjects, "size")
		require.Contains(t, countObjects, "in-pack")
		require.Contains(t, countObjects, "packs")
		require.Contains(t, countObjects, "size-pack")
		require.Contains(t, countObjects, "garbage")
		require.Contains(t, countObjects, "size-garbage")
		return countObjects
	}

	t.Run("shared repo with multiple alternates", func(t *testing.T) {
		storagePath, err := helper.GetStorageByName(repo1.StorageName)
		require.NoError(t, err)

		tmpDir, err := ioutil.TempDir(storagePath, "")
		require.NoError(t, err)
		defer func() { require.NoError(t, os.RemoveAll(tmpDir)) }()

		// clone existing local repo with two alternates
		testhelper.MustRunCommand(t, nil, "git", "clone", "--shared", repoPath1, "--reference", repoPath1, "--reference", repoPath2, tmpDir)

		logBuffer.Reset()
		LogObjectsInfo(testCtx, &gitalypb.Repository{
			StorageName:  repo1.StorageName,
			RelativePath: filepath.Join(strings.TrimPrefix(tmpDir, storagePath), ".git"),
		})

		countObjects := requireLog(logBuffer.String())
		require.ElementsMatch(t, []string{repoPath1 + "/objects", repoPath2 + "/objects"}, countObjects["alternate"])
	})

	t.Run("repo without alternates", func(t *testing.T) {
		logBuffer.Reset()
		LogObjectsInfo(testCtx, repo2)

		countObjects := requireLog(logBuffer.String())
		require.Contains(t, countObjects, "prune-packable")
	})
}
