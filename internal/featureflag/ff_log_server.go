package featureflag

// LogServer enables the use of log server by gitaly-hooks and gitaly-lfs-smudge. Instead
// of logging into their own dedicated files, they'll send the logs to Gitaly and Gitaly
// logs their output through stderr with all the other logs.
var LogServer = NewFeatureFlag(
	"log_server",
	"v17.3.0",
	"https://gitlab.com/gitlab-org/gitaly/-/issues/6222",
	false,
)
