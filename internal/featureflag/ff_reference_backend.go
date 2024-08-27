package featureflag

// NewRepoReftableBackend is used to set 'reftable' as the reference
// backend for new repositories.
var NewRepoReftableBackend = NewFeatureFlag(
	"new_repo_reftable_backend",
	"v17.8.0",
	"https://gitlab.com/gitlab-org/gitaly/-/issues/6331",
	false,
)
