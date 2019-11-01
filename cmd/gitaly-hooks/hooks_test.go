package main

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/command"
	"gitlab.com/gitlab-org/gitaly/internal/config"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gopkg.in/yaml.v2"
)

func TestMain(m *testing.M) {
	os.Exit(testMain(m))
}

func testMain(m *testing.M) int {
	defer testhelper.MustHaveNoChildProcess()

	configureGitalyHooksBinary()

	return m.Run()
}

func TestHooksPrePostReceive(t *testing.T) {
	secretToken := "secret token"
	key := 1234
	glRepository := "some_repo"

	tempGitlabShellDir, cleanup := createTempGitlabShellDir(t)
	defer cleanup()

	changes := "abc"

	ts := gitlabTestServer(t, "", "", secretToken, key, glRepository, changes, true)
	defer ts.Close()

	writeTemporaryConfigFile(t, tempGitlabShellDir, GitlabShellConfig{GitlabURL: ts.URL})
	writeShellSecretFile(t, tempGitlabShellDir, secretToken)

	for _, hook := range []string{"pre-receive", "post-receive"} {
		for envName, env := range map[string][]string{"new": env(t, glRepository, tempGitlabShellDir, key), "old": oldEnv(t, glRepository, tempGitlabShellDir, key)} {
			t.Run(hook+"."+envName, func(t *testing.T) {
				var stderr, stdout bytes.Buffer
				stdin := bytes.NewBuffer([]byte(changes))
				cmd := exec.Command(fmt.Sprintf("../../ruby/git-hooks/%s", hook))
				cmd.Stderr = &stderr
				cmd.Stdout = &stdout
				cmd.Stdin = stdin
				cmd.Env = env

				require.NoError(t, cmd.Run())
				require.Empty(t, stderr.String())
				require.Empty(t, stdout.String())
			})
		}
	}
}

func TestHooksUpdate(t *testing.T) {
	key := 1234
	glRepository := "some_repo"

	tempGitlabShellDir, cleanup := createTempGitlabShellDir(t)
	defer cleanup()

	writeTemporaryConfigFile(t, tempGitlabShellDir, GitlabShellConfig{GitlabURL: "http://www.example.com"})
	writeShellSecretFile(t, tempGitlabShellDir, "the wrong token")

	require.NoError(t, os.MkdirAll(filepath.Join(tempGitlabShellDir, "hooks", "update.d"), 0755))
	testhelper.MustRunCommand(t, nil, "cp", "testdata/update", filepath.Join(tempGitlabShellDir, "hooks", "update.d", "update"))

	for envName, env := range map[string][]string{"new": env(t, glRepository, tempGitlabShellDir, key), "old": oldEnv(t, glRepository, tempGitlabShellDir, key)} {
		t.Run(envName, func(t *testing.T) {
			refval, oldval, newval := "refval", "oldval", "newval"
			var stdout, stderr bytes.Buffer

			cmd := exec.Command(fmt.Sprintf("../../ruby/git-hooks/%s", "update"), refval, oldval, newval)
			cmd.Env = env
			cmd.Stdout = &stdout
			cmd.Stderr = &stderr

			require.NoError(t, cmd.Run())
			require.FileExists(t, "testdata/tempfile")
			require.Empty(t, stdout.String())
			require.Empty(t, stderr.String())

			var inputs []string

			f, err := os.Open("testdata/tempfile")
			require.NoError(t, err)
			require.NoError(t, json.NewDecoder(f).Decode(&inputs))
			require.Equal(t, []string{refval, oldval, newval}, inputs)
			require.NoError(t, os.Remove("testdata/tempfile"))
		})
	}
}

func TestHooksPostReceiveFailed(t *testing.T) {
	secretToken := "secret token"
	key := 1234
	glRepository := "some_repo"

	tempGitlabShellDir, cleanup := createTempGitlabShellDir(t)
	defer cleanup()

	// By setting the last parameter to false, the post-receive API call will
	// send back {"reference_counter_increased": false}, indicating something went wrong
	// with the call

	ts := gitlabTestServer(t, "", "", secretToken, key, glRepository, "", false)
	defer ts.Close()

	writeTemporaryConfigFile(t, tempGitlabShellDir, GitlabShellConfig{GitlabURL: ts.URL})
	writeShellSecretFile(t, tempGitlabShellDir, secretToken)

	for envName, env := range map[string][]string{"new": env(t, glRepository, tempGitlabShellDir, key), "old": oldEnv(t, glRepository, tempGitlabShellDir, key)} {
		t.Run(envName, func(t *testing.T) {
			var stdout, stderr bytes.Buffer

			cmd := exec.Command(fmt.Sprintf("../../ruby/git-hooks/%s", "post-receive"))
			cmd.Env = env
			cmd.Stdout = &stdout
			cmd.Stderr = &stderr

			err := cmd.Run()
			code, ok := command.ExitStatus(err)

			require.True(t, ok, "expect exit status in %v", err)
			require.Equal(t, 1, code, "exit status")
			require.Empty(t, stdout.String())
			require.Empty(t, stderr.String())
		})
	}
}

func TestHooksNotAllowed(t *testing.T) {
	secretToken := "secret token"
	key := 1234
	glRepository := "some_repo"

	tempGitlabShellDir, cleanup := createTempGitlabShellDir(t)
	defer cleanup()

	ts := gitlabTestServer(t, "", "", secretToken, key, glRepository, "", true)
	defer ts.Close()

	writeTemporaryConfigFile(t, tempGitlabShellDir, GitlabShellConfig{GitlabURL: ts.URL})
	writeShellSecretFile(t, tempGitlabShellDir, "the wrong token")

	for envName, env := range map[string][]string{"new": env(t, glRepository, tempGitlabShellDir, key), "old": oldEnv(t, glRepository, tempGitlabShellDir, key)} {
		t.Run(envName, func(t *testing.T) {
			var stderr, stdout bytes.Buffer

			cmd := exec.Command(fmt.Sprintf("../../ruby/git-hooks/%s", "pre-receive"))
			cmd.Stderr = &stderr
			cmd.Stdout = &stdout
			cmd.Env = env

			require.Error(t, cmd.Run())
			require.Equal(t, "GitLab: 401 Unauthorized\n", stderr.String())
			require.Equal(t, "", stdout.String())
		})
	}
}

func TestCheckOK(t *testing.T) {
	user, password := "user123", "password321"

	ts := gitlabTestServer(t, user, password, "", 0, "", "", false)
	defer ts.Close()

	tempDir, err := ioutil.TempDir("", t.Name())
	require.NoError(t, err)
	defer func() {
		os.RemoveAll(tempDir)
	}()

	configPath := writeTemporaryConfigFile(t, tempDir, GitlabShellConfig{GitlabURL: ts.URL, HTTPSettings: HTTPSettings{User: user, Password: password}})

	cmd := exec.Command(fmt.Sprintf("%s/gitaly-hooks", config.Config.BinDir), "check", configPath)

	var stderr, stdout bytes.Buffer
	cmd.Stderr = &stderr
	cmd.Stdout = &stdout

	require.NoError(t, cmd.Run())
	require.Empty(t, stderr.String())
	require.Equal(t, "OK", stdout.String())
}

func TestCheckBadCreds(t *testing.T) {
	user, password := "user123", "password321"

	ts := gitlabTestServer(t, user, password, "", 0, "", "", false)
	defer ts.Close()

	tempDir, err := ioutil.TempDir("", t.Name())
	require.NoError(t, err)
	defer func() {
		os.RemoveAll(tempDir)
	}()

	configPath := writeTemporaryConfigFile(t, tempDir, GitlabShellConfig{GitlabURL: ts.URL, HTTPSettings: HTTPSettings{User: user + "wrong", Password: password}})

	cmd := exec.Command(fmt.Sprintf("%s/gitaly-hooks", config.Config.BinDir), "check", configPath)

	var stderr, stdout bytes.Buffer
	cmd.Stderr = &stderr
	cmd.Stdout = &stdout

	require.Error(t, cmd.Run())
	require.Equal(t, "FAILED. code: 401", stderr.String())
	require.Empty(t, stdout.String())
}

func handleAllowed(t *testing.T, secretToken string, key int, glRepository, changes string) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		require.NoError(t, r.ParseForm())
		require.Equal(t, http.MethodPost, r.Method)
		require.Equal(t, "application/x-www-form-urlencoded", r.Header.Get("Content-Type"))
		require.Equal(t, strconv.Itoa(key), r.Form.Get("key_id"))
		require.Equal(t, glRepository, r.Form.Get("gl_repository"))
		require.Equal(t, "ssh", r.Form.Get("protocol"))
		require.Equal(t, changes, r.Form.Get("changes"))

		w.Header().Set("Content-Type", "application/json")
		if r.Form.Get("secret_token") == secretToken {
			w.Write([]byte(`{"status":true}`))
			return
		}
		w.WriteHeader(http.StatusUnauthorized)
		w.Write([]byte(`{"message":"401 Unauthorized"}`))
	}
}

func handlePreReceive(t *testing.T, secretToken, glRepository string) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		require.NoError(t, r.ParseForm())
		require.Equal(t, http.MethodPost, r.Method)
		require.Equal(t, "application/x-www-form-urlencoded", r.Header.Get("Content-Type"))
		require.Equal(t, glRepository, r.Form.Get("gl_repository"))
		require.Equal(t, secretToken, r.Form.Get("secret_token"))

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"reference_counter_increased": true}`))
	}
}

func handlePostReceive(t *testing.T, secretToken string, key int, glRepository, changes string, counterDecreased bool) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		require.NoError(t, r.ParseForm())
		require.Equal(t, http.MethodPost, r.Method)
		require.Equal(t, "application/x-www-form-urlencoded", r.Header.Get("Content-Type"))
		require.Equal(t, glRepository, r.Form.Get("gl_repository"))
		require.Equal(t, secretToken, r.Form.Get("secret_token"))
		require.Equal(t, fmt.Sprintf("key-%d", key), r.Form.Get("identifier"))
		require.Equal(t, changes, r.Form.Get("changes"))

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(fmt.Sprintf(`{"reference_counter_decreased": %v}`, counterDecreased)))
	}
}

func handleCheck(t *testing.T, user, password string) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		auth := strings.SplitN(r.Header.Get("Authorization"), " ", 2)

		if len(auth) != 2 || auth[0] != "Basic" {
			http.Error(w, "authorization failed", http.StatusUnauthorized)
			return
		}

		payload, _ := base64.StdEncoding.DecodeString(auth[1])
		pair := strings.SplitN(string(payload), ":", 2)

		if pair[0] != user || pair[1] != password {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}

		w.WriteHeader(http.StatusOK)
	}
}

func gitlabTestServer(t *testing.T,
	user, password, secretToken string,
	key int,
	glRepository,
	changes string,
	postReceiveCounterDecreased bool) *httptest.Server {
	mux := http.NewServeMux()
	mux.Handle("/api/v4/internal/allowed", http.HandlerFunc(handleAllowed(t, secretToken, key, glRepository, changes)))
	mux.Handle("/api/v4/internal/pre_receive", http.HandlerFunc(handlePreReceive(t, secretToken, glRepository)))
	mux.Handle("/api/v4/internal/post_receive", http.HandlerFunc(handlePostReceive(t, secretToken, key, glRepository, changes, postReceiveCounterDecreased)))
	mux.Handle("/api/v4/internal/check", http.HandlerFunc(handleCheck(t, user, password)))

	return httptest.NewServer(mux)
}

func createTempGitlabShellDir(t *testing.T) (string, func()) {
	tempDir, err := ioutil.TempDir("", "gitlab-shell")
	require.NoError(t, err)
	return tempDir, func() {
		require.NoError(t, os.RemoveAll(tempDir))
	}
}

func writeTemporaryConfigFile(t *testing.T, dir string, config GitlabShellConfig) string {
	out, err := yaml.Marshal(&config)
	require.NoError(t, err)

	path := filepath.Join(dir, "config.yml")
	require.NoError(t, ioutil.WriteFile(path, out, 0644))

	return path
}

func env(t *testing.T, glRepo, gitlabShellDir string, key int) []string {
	rubyDir, err := filepath.Abs("../../ruby")
	require.NoError(t, err)

	return append(oldEnv(t, glRepo, gitlabShellDir, key), []string{
		"GITALY_BIN_DIR=testdata/gitaly-libexec",
		fmt.Sprintf("GITALY_RUBY_DIR=%s", rubyDir),
	}...)
}

func oldEnv(t *testing.T, glRepo, gitlabShellDir string, key int) []string {
	return append([]string{
		fmt.Sprintf("GL_ID=key-%d", key),
		fmt.Sprintf("GL_REPOSITORY=%s", glRepo),
		"GL_PROTOCOL=ssh",
		fmt.Sprintf("GITALY_GITLAB_SHELL_DIR=%s", gitlabShellDir),
		fmt.Sprintf("GITALY_LOG_DIR=%s", gitlabShellDir),
		"GITALY_LOG_LEVEL=info",
		"GITALY_LOG_FORMAT=json",
		fmt.Sprintf("GITALY_LOG_DIR=%s", gitlabShellDir),
	}, os.Environ()...)
}

func writeShellSecretFile(t *testing.T, dir, secretToken string) {
	require.NoError(t, ioutil.WriteFile(filepath.Join(dir, ".gitlab_shell_secret"), []byte(secretToken), 0644))
}

// configureGitalyHooksBinary builds gitaly-hooks command for tests
func configureGitalyHooksBinary() {
	var err error

	config.Config.BinDir, err = filepath.Abs("testdata/gitaly-libexec")
	if err != nil {
		log.Fatal(err)
	}

	goBuildArgs := []string{
		"build",
		"-o",
		path.Join(config.Config.BinDir, "gitaly-hooks"),
		"gitlab.com/gitlab-org/gitaly/cmd/gitaly-hooks",
	}
	testhelper.MustRunCommand(nil, nil, "go", goBuildArgs...)
}
