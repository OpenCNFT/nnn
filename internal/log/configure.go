package log //nolint:gitaly-linters // Importing testhelper creates a cyclic dependency.

import (
	"fmt"
	"io"
	"os"

	grpcmwlogrus "github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus"
	"github.com/sirupsen/logrus"
)

// SkipReplacingGlobalLoggers will cause `Configure()` to skip replacing global loggers. This is mostly a hack: command
// line applications are expected to call `log.Configure()` in their subcommand actions, and that should indeed always
// replace global loggers, as well. But when running tests, we invoke the subcommand actions multiple times, which is
// thus re-configuring the logger repeatedly. Because global logger are per definition a global shared resource, the
// consequence is that we might end up replacing the global loggers while tests are using them, and this race rightfully
// gets detected by Go's race detector.
//
// This variable should thus only be set in the testhelper's setup routines such that we configure the global logger a
// single time for all of our tests, only.
var SkipReplacingGlobalLoggers bool

// Config contains logging configuration values
type Config struct {
	Format string `json:"format" toml:"format,omitempty"`
	Level  string `json:"level"  toml:"level,omitempty"`
}

// Configure configures the default and gRPC loggers. The gRPC logger's log level will be mapped in order to decrease
// its default verbosity. Returns the configured default logger that would also be returned by `Default()`.
func Configure(out io.Writer, format string, level string, hooks ...logrus.Hook) (Logger, error) {
	l := logrus.New() //nolint:forbidigo

	if err := configure(l, out, format, level, hooks...); err != nil {
		return nil, fmt.Errorf("configuring logger: %w", err)
	}

	if !SkipReplacingGlobalLoggers {
		// Replace the logrus standard logger. While we shouldn't ever be using it in our own codebase, there
		// will very likely be cases where dependencies use it.
		//
		//nolint:forbidigo
		if err := configure(logrus.StandardLogger(), out, format, level, hooks...); err != nil {
			return nil, fmt.Errorf("configuring global logrus logger: %w", err)
		}

		// We replace the gRPC logger with a custom one because the default one is too chatty.
		grpcLogger := logrus.New() //nolint:forbidigo

		if err := configure(grpcLogger, out, format, mapGRPCLogLevel(level), hooks...); err != nil {
			return nil, fmt.Errorf("configuring global gRPC logger: %w", err)
		}

		grpcmwlogrus.ReplaceGrpcLogger(grpcLogger.WithField("pid", os.Getpid()))
	}

	return FromLogrusEntry(l.WithField("pid", os.Getpid())), nil
}

// ConfigureCommand configures the logging infrastructure such that it can be used with simple one-off commands. This
// configuration is supposed to be opinionated and ensures that all one-off commands behave in a sane way:
//
//   - The server configuration does not influence logs generated by the command. This is done intentionally as you don't
//     want to force administrators to adapt the server configuration to influence normal commands.
//
//   - The output always goes to stderr such that output that is supposed to be consumed can be separated from log
//     messages.
//
//   - The output uses text format as it is supposed to be human-readable, not machine-readable.
//
//   - The default log level is set to "error" such that we don't generate tons of log messages that are ultimately
//     uninteresting.
//
// Servers and commands with special requirements should instead use `Configure()`.
func ConfigureCommand() Logger {
	logger, err := Configure(os.Stderr, "text", "error")
	if err != nil {
		// The configuration can't really return an error as we invoke it with known-good parameters.
		panic(err)
	}

	return logger
}

func configure(logger *logrus.Logger, out io.Writer, format, level string, hooks ...logrus.Hook) error {
	var formatter logrus.Formatter
	switch format {
	case "json":
		formatter = UTCJsonFormatter()
	case "text":
		formatter = UTCTextFormatter()
	default:
		return fmt.Errorf("invalid logger format %q", format)
	}

	logrusLevel, err := logrus.ParseLevel(level)
	if err != nil {
		return fmt.Errorf("parse level: %w", err)
	}

	logger.Out = out
	logger.SetLevel(logrusLevel)
	logger.Formatter = formatter
	for _, hook := range hooks {
		logger.Hooks.Add(hook)
	}

	return nil
}

func mapGRPCLogLevel(level string) string {
	// Honor grpc-go's debug settings: https://github.com/grpc/grpc-go#how-to-turn-on-logging
	switch os.Getenv("GRPC_GO_LOG_SEVERITY_LEVEL") {
	case "ERROR", "error":
		return "error"
	case "WARNING", "warning":
		return "warning"
	case "INFO", "info":
		return "info"
	}

	// grpc-go is too verbose at level 'info'. So when config.toml requests
	// level info, we tell grpc-go to log at 'warn' instead.
	if level == "info" || level == "" {
		return "warning"
	}

	return level
}
