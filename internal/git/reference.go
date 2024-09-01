package git

import (
	"fmt"
	"strings"
)

// ReferenceUpdate describes the state of a reference's old and new tip in an update.
type ReferenceUpdate struct {
	// OldOID is the old OID the reference is expected to point to prior to updating it.
	// If the reference does not point to the old value, the reference verification fails.
	OldOID ObjectID
	// NewOID is the new desired OID to point the reference to.
	NewOID ObjectID
	// OldTarget is the expected target for a symbolic reference.
	OldTarget ReferenceName
	// NewTarget stores the desired target for a symbolic reference.
	NewTarget ReferenceName
}

// IsRegularUpdate is used to check if the update is a regular reference
// update, which updates a ref from an old OID to a new OID.
//
// This excludes symrefs and regular <> symref conversions.
func (r *ReferenceUpdate) IsRegularUpdate() bool {
	return r.OldTarget == "" && r.NewTarget == ""
}

// ReferenceUpdates contains references to update. Reference name is used as the key and the value
// is the expected old tip and the desired new tip.
type ReferenceUpdates map[ReferenceName]ReferenceUpdate

// InternalReferenceType is the type of an internal reference.
type InternalReferenceType int

const (
	// InternalReferenceTypeHidden indicates that a reference should never be advertised or
	// writeable.
	InternalReferenceTypeHidden = InternalReferenceType(iota + 1)
	// InternalReferenceTypeReadonly indicates that a reference should be advertised, but not
	// writeable.
	InternalReferenceTypeReadonly
)

// InternalRefPrefixes is an array of all reference prefixes which are used internally by GitLab.
// These need special treatment in some cases, e.g. to restrict writing to them.
var InternalRefPrefixes = map[string]InternalReferenceType{
	// Environments may be interesting to the user in case they want to figure out what exact
	// reference an environment has been constructed from.
	"refs/environments/": InternalReferenceTypeReadonly,

	// Keep-around references are only used internally to keep alive objects, and thus they
	// shouldn't be exposed to the user.
	"refs/keep-around/": InternalReferenceTypeHidden,

	// Merge request references should be readable by the user so that they can still fetch the
	// changes of specific merge requests.
	"refs/merge-requests/": InternalReferenceTypeReadonly,

	// Pipelines may be interesting to the user in case they want to figure out what exact
	// reference a specific pipeline has been running with.
	"refs/pipelines/": InternalReferenceTypeReadonly,

	// Remote references shouldn't typically exist in repositories nowadays anymore, and there
	// is no reason to expose them to the user.
	"refs/remotes/": InternalReferenceTypeHidden,

	// Temporary references are used internally by Rails for various operations and should not
	// be exposed to the user.
	"refs/tmp/": InternalReferenceTypeHidden,
}

// ObjectPoolRefNamespace is the namespace used for the references of the primary pool member part
// of an object pool.
const ObjectPoolRefNamespace = "refs/remotes/origin"

// Revision represents anything that resolves to either a commit, multiple
// commits or to an object different than a commit. This could be e.g.
// "master", "master^{commit}", an object hash or similar. See gitrevisions(1)
// for supported syntax.
type Revision string

// String returns the string representation of the Revision.
func (r Revision) String() string {
	return string(r)
}

// ReferenceName represents the name of a git reference, e.g.
// "refs/heads/master". It does not support extended revision notation like a
// Revision does and must always contain a fully qualified reference.
type ReferenceName string

// NewReferenceNameFromBranchName returns a new ReferenceName from a given
// branch name. Note that branch is treated as an unqualified branch name.
// This function will thus always prepend "refs/heads/".
func NewReferenceNameFromBranchName(branch string) ReferenceName {
	return ReferenceName("refs/heads/" + branch)
}

// String returns the string representation of the ReferenceName.
func (r ReferenceName) String() string {
	return string(r)
}

// Revision converts the ReferenceName to a Revision. This is safe to do as a
// reference is always also a revision.
func (r ReferenceName) Revision() Revision {
	return Revision(r)
}

// Branch returns `true` and the branch name if the reference is a branch. E.g.
// if ReferenceName is "refs/heads/master", it will return "master". If it is
// not a branch, `false` is returned.
func (r ReferenceName) Branch() (string, bool) {
	if strings.HasPrefix(r.String(), "refs/heads/") {
		branch := r.String()[len("refs/heads/"):]
		if branch != "" {
			return branch, true
		}
	}
	return "", false
}

// Reference represents a Git reference.
type Reference struct {
	// Name is the name of the reference
	Name ReferenceName
	// Target is the target of the reference. For direct references it
	// contains the object ID, for symbolic references it contains the
	// target branch name.
	Target string
	// IsSymbolic tells whether the reference is direct or symbolic
	IsSymbolic bool
}

// NewReference creates a direct reference to an object.
func NewReference(name ReferenceName, target ObjectID) Reference {
	return Reference{
		Name:       name,
		Target:     string(target),
		IsSymbolic: false,
	}
}

// NewSymbolicReference creates a symbolic reference to another reference.
func NewSymbolicReference(name ReferenceName, target ReferenceName) Reference {
	return Reference{
		Name:       name,
		Target:     string(target),
		IsSymbolic: true,
	}
}

// ValidateReference checks whether a reference looks valid. It aims to be a faithful implementation of Git's own
// `check_or_sanitize_refname()` function and all divergent behaviour is considered to be a bug. Please also refer to
// https://git-scm.com/docs/git-check-ref-format#_description for further information on the actual rules.
func ValidateReference(name string) error {
	if name == "HEAD" {
		return fmt.Errorf("HEAD reference not allowed")
	}

	// TODO: this can eventually be converted to use `strings.CutPrefix()`.
	if !strings.HasPrefix(name, "refs/") {
		return fmt.Errorf("reference is not fully qualified")
	}
	name = name[len("refs/"):]

	if len(name) == 0 {
		return fmt.Errorf("refs/ is not a valid reference")
	}

	if strings.HasSuffix(name, "/") {
		return fmt.Errorf("reference must not end with slash")
	}

	if strings.HasSuffix(name, ".") {
		return fmt.Errorf("reference must not end with dot")
	}

	if strings.Contains(name, "@{") {
		return fmt.Errorf("reference must not contain @{")
	}

	if strings.Contains(name, "..") {
		return fmt.Errorf("reference must not contain double dots")
	}

	for _, c := range name {
		switch c {
		case ' ', '\t', '\n':
			return fmt.Errorf("reference must not contain space characters")
		case ':', '?', '[', '\\', '^', '~', '*', '\177':
			return fmt.Errorf("reference must not contain special characters")
		}

		// Note that we treat some of the characters below 32 specially in the switch above so
		// that we can report back more precise error messages.
		if c < 32 {
			return fmt.Errorf("reference must not contain control characters")
		}
	}

	// We need to check the components individually as components aren't allowed to have some specific constructs.
	for {
		component, tail, _ := strings.Cut(name, "/")

		if component == "" {
			if tail != "" {
				return fmt.Errorf("empty component is not allowed")
			}

			// Otherwise, if both component and tail are empty, we have fully verified the complete
			// reference and can thus return successfully.
			return nil
		}

		if strings.HasPrefix(component, ".") {
			return fmt.Errorf("component must not start with dot")
		}

		if strings.HasSuffix(component, ".lock") {
			return fmt.Errorf("component must not end with .lock")
		}

		name = tail
	}
}
