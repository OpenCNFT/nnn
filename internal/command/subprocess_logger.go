package command

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"gitlab.com/gitlab-org/gitaly/v16/internal/helper/perm"
	"gitlab.com/gitlab-org/gitaly/v16/internal/log"
	"gitlab.com/gitlab-org/labkit/correlation"
)

const (
	// EnvLogConfiguration is the environment variable under which the logging configuration is
	// passed to subprocesses.
	EnvLogConfiguration = "GITALY_LOG_CONFIGURATION"
)

// subprocessConfiguration is logger configuration passed to the command if subprocess logger
// is enabled. It contains the general logging configuration and the file descriptor the logs
// should be written to.
type subprocessConfiguration struct {
	// FileDescriptor is the file descriptor where logs should be written.
	FileDescriptor uintptr
	// Config is the configuration for the logger.
	Config log.Config
}

// envSubprocessLoggerConfiguration returns the logging configuration to pass to the
// subprocess as an environment variable.
func envSubprocessLoggerConfiguration(cfg subprocessConfiguration) (string, error) {
	marshaled, err := json.Marshal(cfg)
	if err != nil {
		return "", fmt.Errorf("marshal: %w", err)
	}

	return fmt.Sprintf("%s=%s", EnvLogConfiguration, string(marshaled)), nil
}

// NewSubprocessLogger returns a logger than can be used in a subprocess spawned by Gitaly. It extracts the log
// directory from the environment using getEnv and outputs logs in the given format log dir under a file defined
// by logFileName. If `GITALY_LOG_CONFIGURATION` is set, it outputs the logs to the file descriptor using the configuration
// from the environment variable.
func NewSubprocessLogger(ctx context.Context, getEnv func(string) string, logFileName, format string) (_ log.Logger, _ io.Closer, returnedErr error) {
	cfg := subprocessConfiguration{
		Config: log.Config{
			Level:  "info",
			Format: format,
		},
	}

	if value := getEnv(EnvLogConfiguration); value != "" {
		if err := json.Unmarshal([]byte(value), &cfg); err != nil {
			return nil, nil, fmt.Errorf("unmarshal: %w", err)
		}
	}

	output, err := newSubprocessLoggerOutput(getEnv, logFileName, cfg.FileDescriptor)
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

	logger, err := log.Configure(output, cfg.Config.Format, cfg.Config.Level)
	if err != nil {
		return nil, nil, fmt.Errorf("configure: %w", err)
	}

	if cfg.FileDescriptor > 0 {
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

func newSubprocessLoggerOutput(getEnv func(string) string, logFileName string, fileDescriptor uintptr) (io.WriteCloser, error) {
	if fileDescriptor > 0 {
		logWriter := os.NewFile(fileDescriptor, "log-writer")
		if logWriter == nil {
			return nil, fmt.Errorf("invalid log writer file descriptor: %d", fileDescriptor)
		}

		return logWriter, nil
	}

	output := newNopCloser(io.Discard)
	if logDir := getEnv(log.GitalyLogDirEnvKey); logDir != "" {
		if logFile, err := os.OpenFile(filepath.Join(logDir, logFileName), os.O_CREATE|os.O_APPEND|os.O_WRONLY, perm.SharedFile); err != nil {
			// Ignore this error as we cannot do anything about it anyway. We cannot write anything to
			// stdout or stderr as that might break hooks, and we have no other destination to log to.
		} else {
			output = logFile
		}
	}

	return output, nil
}
