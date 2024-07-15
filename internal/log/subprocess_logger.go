package log

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"gitlab.com/gitlab-org/gitaly/v16/internal/helper/perm"
	"gitlab.com/gitlab-org/labkit/correlation"
)

// NewSubprocessLogger returns a logger than can be used in a subprocess spawned by Gitaly. It extracts the log
// directory from the environment using getEnv and outputs logs in the given format log dir under a file defined
// by logFileName.
func NewSubprocessLogger(ctx context.Context, getEnv func(string) string, logFileName, format string) (_ Logger, _ io.Closer, returnedErr error) {
	output := newSubprocessLoggerOutput(getEnv, logFileName)
	defer func() {
		if returnedErr != nil {
			if err := output.Close(); err != nil {
				returnedErr = errors.Join(returnedErr, fmt.Errorf("close: %w", err))
			}
		}
	}()

	logger, err := Configure(output, format, "info")
	if err != nil {
		return nil, nil, fmt.Errorf("configure: %w", err)
	}

	return logger.WithField(correlation.FieldName, correlation.ExtractFromContext(ctx)), output, nil
}

type nopCloser struct{ io.Writer }

func (nopCloser) Close() error { return nil }

func newNopCloser(w io.Writer) io.WriteCloser {
	return nopCloser{Writer: w}
}

func newSubprocessLoggerOutput(getEnv func(string) string, logFileName string) io.WriteCloser {
	output := newNopCloser(io.Discard)

	if logDir := getEnv(GitalyLogDirEnvKey); logDir != "" {
		if logFile, err := os.OpenFile(filepath.Join(logDir, logFileName), os.O_CREATE|os.O_APPEND|os.O_WRONLY, perm.SharedFile); err != nil {
			// Ignore this error as we cannot do anything about it anyway. We cannot write anything to
			// stdout or stderr as that might break hooks, and we have no other destination to log to.
		} else {
			output = logFile
		}
	}

	return output
}
