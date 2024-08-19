package featureflag

// GitV246 enables the use Git v2.46.
var GitV246 = NewFeatureFlag(
	"git_v246",
	"v17.4.0",
	"https://gitlab.com/gitlab-org/gitaly/-/issues/6302",
	false,
)
