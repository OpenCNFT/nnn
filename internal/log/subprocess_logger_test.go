// log_test is an external test package due to testhelper importing the log package.
package log_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v16/internal/log"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper/testcfg"
)

func TestSubprocessLogger(t *testing.T) {
	t.Parallel()

	// LogLine contains the fields we're interested in asserting. There are also other fields
	// logged such as `time` and `pid` but we don't assert them due to them being indeterministic.
	type LogLine struct {
		Message     string `json:"msg"`
		Level       string `json:"level"`
		CustomField string `json:"custom_field"`
	}

	for _, tc := range []struct {
		desc            string
		run             func(t *testing.T, socketPath string, newLogger func() (log.Logger, io.Closer))
		requireLogLines func(t *testing.T, actual []LogLine)
	}{
		{
			desc: "logging works",
			run: func(t *testing.T, socketPath string, newLogger func() (log.Logger, io.Closer)) {
				logger, closer := newLogger()
				defer testhelper.MustClose(t, closer)

				logger.WithField("custom_field", "info value").Info("info message")
				logger.WithField("custom_field", "error value").Error("error message")
				logger.Debug("ignored due to too low logging level")
			},
			requireLogLines: func(t *testing.T, actual []LogLine) {
				// Lines from a single process are logged in order.
				require.Equal(t, []LogLine{
					{
						Message:     "info message",
						Level:       "info",
						CustomField: "info value",
					},
					{
						Message:     "error message",
						Level:       "error",
						CustomField: "error value",
					},
				}, actual)
			},
		},
		{
			desc: "concurrent logging works",
			run: func(t *testing.T, socketPath string, newLogger func() (log.Logger, io.Closer)) {
				start := make(chan struct{})
				for i := 0; i < 5; i++ {
					i := i
					logger, closer := newLogger()
					go func() {
						defer func() { assert.NoError(t, closer.Close()) }()

						<-start
						logger.WithField("custom_field", strconv.Itoa(i)).Info(fmt.Sprintf("message %d", i))
					}()
				}
				close(start)
			},
			requireLogLines: func(t *testing.T, actual []LogLine) {
				// Lines from multiple are handled in indeterministic order.
				require.ElementsMatch(t, []LogLine{
					{
						Message:     "message 0",
						Level:       "info",
						CustomField: "0",
					},
					{
						Message:     "message 1",
						Level:       "info",
						CustomField: "1",
					},
					{
						Message:     "message 2",
						Level:       "info",
						CustomField: "2",
					},
					{
						Message:     "message 3",
						Level:       "info",
						CustomField: "3",
					},
					{
						Message:     "message 4",
						Level:       "info",
						CustomField: "4",
					},
				}, actual)
			},
		},
		{
			desc: "partial lines are not logged",
			run: func(t *testing.T, socketPath string, newLogger func() (log.Logger, io.Closer)) {
				conn, err := net.Dial("unix", socketPath)
				require.NoError(t, err)
				defer testhelper.MustClose(t, conn)

				_, err = conn.Write([]byte(`{"msg": "valid JSON without newline character at end"}`))
				require.NoError(t, err)
			},
			requireLogLines: func(t *testing.T, actual []LogLine) {
				require.Empty(t, actual)
			},
		},
	} {
		tc := tc
		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			cfg := testcfg.Build(t)

			output := &bytes.Buffer{}
			srv, err := log.NewServer(testhelper.SharedLogger(t), cfg.RuntimeDir, log.NewSyncWriter(output))
			require.NoError(t, err)
			// The error return is not important as this only cleans up during failures. We close the
			// server further down in the test.
			defer func() { _ = srv.Close() }()

			runErr := make(chan error, 1)
			go func() { runErr <- srv.Run() }()

			ctx := testhelper.Context(t)

			// Use json as it is easier to assert the output of the logger.
			cfg.Logging.Config.Format = "json"

			tc.run(t, log.SocketPath(cfg.RuntimeDir), func() (log.Logger, io.Closer) {
				logger, closer, err := log.NewSubprocessLogger(ctx, func(variableName string) string {
					subprocessCfg, err := log.SubprocessConfiguration(cfg.RuntimeDir, cfg.Logging.Config)
					require.NoError(t, err)

					key, value, found := strings.Cut(subprocessCfg, "=")
					require.Equal(t, key, variableName)
					require.True(t, found)

					return value
				}, "file-name-ignored", "format-ignored")
				require.NoError(t, err)

				return logger, closer
			})

			// Synchronize by closing the server. This ensures the connections have been handled and
			// the log output written.
			testhelper.MustClose(t, srv)
			require.NoError(t, <-runErr)

			var actualLines []LogLine
			if output.Len() > 0 {
				for _, rawLine := range strings.Split(strings.TrimSpace(output.String()), "\n") {
					var line LogLine

					require.NoError(t, json.Unmarshal([]byte(rawLine), &line))
					actualLines = append(actualLines, line)
				}
			}

			tc.requireLogLines(t, actualLines)
		})
	}
}
