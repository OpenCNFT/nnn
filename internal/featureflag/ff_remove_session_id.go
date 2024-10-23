package featureflag

// RemoveCatfileCacheSessionID removes the gitaly-session-id from being used as
// a cache key for the cat-file cache.
var RemoveCatfileCacheSessionID = NewFeatureFlag(
	"remove_catfile_cache_session_id",
	"v17.6.0",
	"https://gitlab.com/gitlab-org/gitaly/-/issues/6404",
	true,
)
