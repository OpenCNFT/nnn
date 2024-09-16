package featureflag

// UserFFBranchStructuredErrors when enabled returns structured errors from
// UserFFBranch instead of legacy error messages in the payload of a successful
// response.
var UserFFBranchStructuredErrors = NewFeatureFlag(
	"user_ff_branch_structured_errors",
	"v17.3.0",
	"https://gitlab.com/gitlab-org/gitaly/-/issues/6267",
	true,
)
