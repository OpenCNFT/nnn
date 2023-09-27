package sentry

import (
	"fmt"

	sentry "github.com/getsentry/sentry-go"
	"gitlab.com/gitlab-org/gitaly/v16/internal/grpc/middleware/panichandler"
	"gitlab.com/gitlab-org/gitaly/v16/internal/log"
)

// Config contains configuration for sentry
type Config struct {
	DSN         string `toml:"sentry_dsn,omitempty" json:"sentry_dsn"`
	Environment string `toml:"sentry_environment,omitempty" json:"sentry_environment"`
}

// ConfigureSentry configures the sentry DSN
func ConfigureSentry(logger log.Logger, version string, sentryConf Config) {
	if sentryConf.DSN == "" {
		return
	}

	err := sentry.Init(sentry.ClientOptions{
		Dsn:         sentryConf.DSN,
		Environment: sentryConf.Environment,
		Release:     "v" + version,
	})
	if err != nil {
		logger.WithError(err).Warn("unable to initialize sentry client")
		return
	}

	logger.Debug("Using sentry logging")

	panichandler.InstallPanicHandler(func(grpcMethod string, _err interface{}) {
		err, ok := _err.(error)
		if !ok {
			err = fmt.Errorf("%v", _err)
		}

		sentry.WithScope(func(scope *sentry.Scope) {
			scope.SetTag("grpcMethod", grpcMethod)
			scope.SetTag("panic", "1")
			sentry.CaptureException(err)
		})
	})
}
