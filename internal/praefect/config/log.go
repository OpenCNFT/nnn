package config

import (
	"github.com/sirupsen/logrus"
	"gitlab.com/gitlab-org/gitaly/internal/log"
)

// ConfigureLogger applies the settings from the configuration file to the
// logger, setting the log level and format.
func (c Config) ConfigureLogger() *logrus.Logger {
	log.Configure(c.Logging.Format, c.Logging.Level)

	return log.Default
}
