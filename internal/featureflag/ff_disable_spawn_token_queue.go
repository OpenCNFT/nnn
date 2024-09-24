package featureflag

// DisableSpawnTokenQueue disables the spawn token queue mechanism limiting
// the number of processes that can be spawned concurrently.
var DisableSpawnTokenQueue = NewFeatureFlag(
	"disable_spawn_token_queue",
	"v16.5.0",
	"https://gitlab.com/gitlab-org/gitaly/-/issues/6394",
	false,
)
