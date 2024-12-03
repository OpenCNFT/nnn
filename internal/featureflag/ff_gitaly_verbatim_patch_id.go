package featureflag

// VerbatimPatchId disables the use of --verbatim option for patch-id call
var VerbatimPatchId = NewFeatureFlag(
	"verbatim_patch_id",
	"v17.7.0",
	"https://gitlab.com/gitlab-org/gitaly/-/issues/6537",
	true,
)
