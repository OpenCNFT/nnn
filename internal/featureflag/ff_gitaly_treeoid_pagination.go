package featureflag

// TreeOIDPagination enables the use of root tree OID instead of revision in subsequent pagination requests.
var TreeOIDPagination = NewFeatureFlag(
	"treeoid_pagination",
	"v17.3.0",
	"https://gitlab.com/gitlab-org/gitaly/-/issues/6242",
	false,
)
