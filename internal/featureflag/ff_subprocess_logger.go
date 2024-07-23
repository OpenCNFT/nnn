package featureflag

// SubprocessLogger enables the use of subprocess logger by gitaly-hooks and gitaly-lfs-smudge.
// Instead of logging into their own dedicated files, they'll send the logs to Gitaly over a pipe
// and Gitaly then logs their output through stdout with all the other logs
var SubprocessLogger = NewFeatureFlag(
	"subprocess_logger",
	"v17.3.0",
	"https://gitlab.com/gitlab-org/gitaly/-/issues/6222",
	false,
)
