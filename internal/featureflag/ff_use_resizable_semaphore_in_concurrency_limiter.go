package featureflag

// UseResizableSemaphoreInConcurrencyLimiter enables the usage of limiter.resizableSemaphore in
// limiter.ConcurrencyLimiter. After the flag is enabled, the resizable semaphore implementation
// will be used, but with a static limit. There shouldn't be any changes in terms of functionality
// and resource usage.
var UseResizableSemaphoreInConcurrencyLimiter = NewFeatureFlag(
	"use_resizable_semaphore_in_concurrency_limiter",
	"v16.5.0",
	"https://gitlab.com/gitlab-org/gitaly/-/issues/5581",
	false,
)
