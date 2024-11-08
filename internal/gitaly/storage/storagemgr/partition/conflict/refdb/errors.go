package refdb

import (
	"gitlab.com/gitlab-org/gitaly/v16/internal/git"
)

// ChildReferencesExistError is raised when attempting to create a reference
// in a node which has child references. With the files backend, this corresponds
// to a directory-file conflict where `refs/heads/main/child` exists but we're
// attempting to create a `refs/heads/main`.
type ChildReferencesExistError struct {
	// TargetReference is the name of the reference failed to be created.
	TargetReference git.ReferenceName
}

// Error returns the error message.
func (ChildReferencesExistError) Error() string {
	return "child references exist"
}

// ParentReferenceExistsError is raised when attempting to create a reference
// but a parent reference exists already in the hierarchy. With the files backend,
// this corresponds to a directory-file conflict where `refs/heads/main` exists but
// we're attempting to create a `refs/heads/main/child`.
type ParentReferenceExistsError struct {
	// ExistingReference is the name of the reference that already exists.
	ExistingReference git.ReferenceName
	// TargetReference is the name of the reference that failed to be created.
	TargetReference git.ReferenceName
}

// Error returns the error message.
func (ParentReferenceExistsError) Error() string {
	return "parent reference exists"
}

// UnexpectedOldValueError is returned when the reference's old value does not match
// the expected.
type UnexpectedOldValueError struct {
	// TargetReference is the name of the reference that failed to be created, deleted
	// or updated.
	TargetReference git.ReferenceName
	// ExpectedValue is the expected value of the reference prior to the update.
	ExpectedValue string
	// ActualValue is the actual value of the reference prior to the update.
	ActualValue string
}

// Error returns the error message.
func (err UnexpectedOldValueError) Error() string {
	return "unexpected old value"
}
