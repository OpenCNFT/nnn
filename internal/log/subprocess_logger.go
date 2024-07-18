package log

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"

	"gitlab.com/gitlab-org/gitaly/v16/internal/helper/perm"
	"gitlab.com/gitlab-org/labkit/correlation"
)

const (
	// EnvLogConfiguration is the environment variable under which the logging configuration is
	// passed to subprocesses.
	EnvLogConfiguration = "GITALY_LOG_CONFIGURATION"
)

type subprocessConfiguration struct {
	// SocketPath is an absolute path to the Unix socket where the logs should be written to.
	SocketPath string
	// Config is the configuration for the logger.
	Config Config
}

// SubprocessConfiguration returns the logging configuration to pass to the
// subprocess as an environment variable.
func SubprocessConfiguration(runtimeDir string, config Config) (string, error) {
	cfg, err := json.Marshal(subprocessConfiguration{
		SocketPath: SocketPath(runtimeDir),
		Config:     config,
	})
	if err != nil {
		return "", fmt.Errorf("marshal: %w", err)
	}

	return fmt.Sprintf("%s=%s", EnvLogConfiguration, string(cfg)), nil
}

// NewSubprocessLogger returns a logger than can be used in a subprocess spawned by Gitaly. It extracts the log
// directory from the environment using getEnv and outputs logs in the given format log dir under a file defined
// by logFileName. If `GITALY_LOG_CONFIGURATION` is set, it outputs the logs to the log server using the configuration
// from the environment variable.
func NewSubprocessLogger(ctx context.Context, getEnv func(string) string, logFileName, format string) (_ Logger, _ io.Closer, returnedErr error) {
	cfg := subprocessConfiguration{
		Config: Config{
			Level:  "info",
			Format: format,
		},
	}

	if value := getEnv(EnvLogConfiguration); value != "" {
		if err := json.Unmarshal([]byte(value), &cfg); err != nil {
			return nil, nil, fmt.Errorf("unmarshal: %w", err)
		}
	}

	output, err := newSubprocessLoggerOutput(getEnv, logFileName, cfg.SocketPath)
	if err != nil {
		return nil, nil, fmt.Errorf("new subprocess logger output: %w", err)
	}

	defer func() {
		if returnedErr != nil {
			if err := output.Close(); err != nil {
				returnedErr = errors.Join(returnedErr, fmt.Errorf("close: %w", err))
			}
		}
	}()

	logger, err := Configure(output, cfg.Config.Format, cfg.Config.Level)
	if err != nil {
		return nil, nil, fmt.Errorf("configure: %w", err)
	}

	if cfg.SocketPath != "" {
		// If the log server is in use, use the log file's name as the component for now to identify
		// which log this entry comes from.
		logger = logger.WithField("component", filepath.Base(logFileName))
	}

	return logger.WithField(correlation.FieldName, correlation.ExtractFromContext(ctx)), output, nil
}

type nopCloser struct{ io.Writer }

func (nopCloser) Close() error { return nil }

func newNopCloser(w io.Writer) io.WriteCloser {
	return nopCloser{Writer: w}
}

func newSubprocessLoggerOutput(getEnv func(string) string, logFileName, socketPath string) (io.WriteCloser, error) {
	if socketPath != "" {
		conn, err := net.Dial("unix", socketPath)
		if err != nil {
			return nil, fmt.Errorf("dial: %w", err)
		}

		// The server closes its writing side after accepting the connection. Wait for it here.
		// This demonstrates the server has accepted the connection from the kernel's accept queue.
		// Logs written to the connection before the server has accepted it could be lost if the
		// server is shutting down and didn't accept the connection before closing the listener.
		if _, err := conn.Read(make([]byte, 1)); !errors.Is(err, io.EOF) {
			return nil, fmt.Errorf("read: %w", err)
		}

		return conn, nil
	}

	output := newNopCloser(io.Discard)
	if logDir := getEnv(GitalyLogDirEnvKey); logDir != "" {
		if logFile, err := os.OpenFile(filepath.Join(logDir, logFileName), os.O_CREATE|os.O_APPEND|os.O_WRONLY, perm.SharedFile); err != nil {
			// Ignore this error as we cannot do anything about it anyway. We cannot write anything to
			// stdout or stderr as that might break hooks, and we have no other destination to log to.
		} else {
			output = logFile
		}
	}

	return output, nil
}
