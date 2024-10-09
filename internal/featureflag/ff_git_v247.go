package featureflag

// GitV247 enables the use Git v2.47.
var GitV247 = NewFeatureFlag(
	"git_v247",
	"v17.5.0",
	"https://gitlab.com/gitlab-org/gitaly/-/issues/6419",
	false,
)
