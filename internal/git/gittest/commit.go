package gittest

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/mode"
	"gitlab.com/gitlab-org/gitaly/v16/internal/helper/text"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var (
	// DefaultCommitterName is the default name of the committer and author used to create
	// commits.
	DefaultCommitterName = "Scrooge McDuck"
	// DefaultCommitterMail is the default mail of the committer and author used to create
	// commits.
	DefaultCommitterMail = "scrooge@mcduck.com"
	// DefaultCommitTime is the default time used as written by WriteCommit().
	DefaultCommitTime = time.Date(2019, 11, 3, 11, 27, 59, 0, time.FixedZone("UTC+1", 1*60*60))
	// DefaultCommitterSignature is the default signature in the format like it would be present
	// in commits: "$name <$email> $unixtimestamp $timezone".
	DefaultCommitterSignature = fmt.Sprintf(
		"%s <%s> %s", DefaultCommitterName, DefaultCommitterMail, git.FormatSignatureTime(DefaultCommitTime),
	)
	// DefaultCommitAuthor is the Protobuf message representation of the default committer and
	// author used to create commits.
	DefaultCommitAuthor = &gitalypb.CommitAuthor{
		Name:     []byte(DefaultCommitterName),
		Email:    []byte(DefaultCommitterMail),
		Date:     timestamppb.New(DefaultCommitTime),
		Timezone: []byte("+0100"),
	}
)

type writeCommitConfig struct {
	reference          string
	parents            []git.ObjectID
	authorDate         time.Time
	authorName         string
	committerName      string
	committerDate      time.Time
	message            string
	treeEntries        []TreeEntry
	treeID             git.ObjectID
	alternateObjectDir string
	encoding           string
}

// WriteCommitOption is an option which can be passed to WriteCommit.
type WriteCommitOption func(*writeCommitConfig)

// WithReference is an option for WriteCommit which will cause it to update the given reference to
// point to the new commit. This function requires the fully-qualified reference name.
func WithReference(reference string) WriteCommitOption {
	return func(cfg *writeCommitConfig) {
		cfg.reference = reference
	}
}

// WithBranch is an option for WriteCommit which will cause it to update the given branch name to
// the new commit.
func WithBranch(branch string) WriteCommitOption {
	return WithReference("refs/heads/" + branch)
}

// WithMessage is an option for WriteCommit which will set the commit message.
func WithMessage(message string) WriteCommitOption {
	return func(cfg *writeCommitConfig) {
		cfg.message = message
	}
}

// WithParents is an option for WriteCommit which will set the parent OIDs of the resulting commit.
func WithParents(parents ...git.ObjectID) WriteCommitOption {
	return func(cfg *writeCommitConfig) {
		if parents != nil {
			cfg.parents = parents
		} else {
			// We're explicitly initializing parents here such that we can discern the
			// case where the commit should be created with no parents.
			cfg.parents = []git.ObjectID{}
		}
	}
}

// WithTreeEntries is an option for WriteCommit which will cause it to create a new tree and use it
// as root tree of the resulting commit.
func WithTreeEntries(entries ...TreeEntry) WriteCommitOption {
	return func(cfg *writeCommitConfig) {
		cfg.treeEntries = entries
	}
}

// WithTree is an option for WriteCommit which will cause it to use the given object ID as the root
// tree of the resulting commit.
// as root tree of the resulting commit.
func WithTree(treeID git.ObjectID) WriteCommitOption {
	return func(cfg *writeCommitConfig) {
		cfg.treeID = treeID
	}
}

// WithAuthorName is an option for WriteCommit which will set the author name.
func WithAuthorName(name string) WriteCommitOption {
	return func(cfg *writeCommitConfig) {
		cfg.authorName = name
	}
}

// WithAuthorDate is an option for WriteCommit which will set the author date.
func WithAuthorDate(date time.Time) WriteCommitOption {
	return func(cfg *writeCommitConfig) {
		cfg.authorDate = date
	}
}

// WithCommitterName is an option for WriteCommit which will set the committer name.
func WithCommitterName(name string) WriteCommitOption {
	return func(cfg *writeCommitConfig) {
		cfg.committerName = name
	}
}

// WithCommitterDate is an option for WriteCommit which will set the committer date.
func WithCommitterDate(date time.Time) WriteCommitOption {
	return func(cfg *writeCommitConfig) {
		cfg.committerDate = date
	}
}

// WithAlternateObjectDirectory will cause the commit to be written into the given alternate object
// directory. This can either be an absolute path or a relative path. In the latter case the path
// is considered to be relative to the repository path.
func WithAlternateObjectDirectory(alternateObjectDir string) WriteCommitOption {
	return func(cfg *writeCommitConfig) {
		cfg.alternateObjectDir = alternateObjectDir
	}
}

// WithEncoding asks Git to set the commit message encoding to the given string.
func WithEncoding(encoding string) WriteCommitOption {
	return func(cfg *writeCommitConfig) {
		cfg.encoding = encoding
	}
}

// WriteCommit writes a new commit into the target repository.
func WriteCommit(tb testing.TB, cfg config.Cfg, repoPath string, opts ...WriteCommitOption) git.ObjectID {
	tb.Helper()

	var writeCommitConfig writeCommitConfig
	for _, opt := range opts {
		opt(&writeCommitConfig)
	}

	message := "message"
	if writeCommitConfig.message != "" {
		message = writeCommitConfig.message
	}
	stdin := bytes.NewBufferString(message)

	if len(writeCommitConfig.treeEntries) > 0 && writeCommitConfig.treeID != "" {
		require.FailNow(tb, "cannot set tree entries and tree ID at the same time")
	}

	var tree string
	if writeCommitConfig.treeEntries != nil {
		tree = WriteTree(tb, cfg, repoPath, writeCommitConfig.treeEntries).String()
	} else if writeCommitConfig.treeID != "" {
		tree = writeCommitConfig.treeID.String()
	} else if len(writeCommitConfig.parents) == 0 {
		tree = WriteTree(tb, cfg, repoPath, []TreeEntry{}).String()
	} else {
		tree = writeCommitConfig.parents[0].String() + "^{tree}"
	}

	if writeCommitConfig.authorName == "" {
		writeCommitConfig.authorName = DefaultCommitterName
	}

	if writeCommitConfig.authorDate.IsZero() {
		writeCommitConfig.authorDate = DefaultCommitTime
	}

	if writeCommitConfig.committerName == "" {
		writeCommitConfig.committerName = DefaultCommitterName
	}

	if writeCommitConfig.committerDate.IsZero() {
		writeCommitConfig.committerDate = DefaultCommitTime
	}

	// Use 'commit-tree' instead of 'commit' because we are in a bare
	// repository. What we do here is the same as "commit -m message
	// --allow-empty".
	commitArgs := []string{
		"-c", fmt.Sprintf("user.name=%s", writeCommitConfig.committerName),
		"-c", fmt.Sprintf("user.email=%s", DefaultCommitterMail),
	}

	if writeCommitConfig.encoding != "" {
		commitArgs = append(commitArgs, "-c", "i18n.commitEncoding="+writeCommitConfig.encoding)
	}

	commitArgs = append(commitArgs,
		"-C", repoPath,
		"commit-tree", "-F", "-", tree,
	)

	var env []string
	if writeCommitConfig.alternateObjectDir != "" {
		require.True(tb, filepath.IsAbs(writeCommitConfig.alternateObjectDir),
			"alternate object directory must be an absolute path")
		require.NoError(tb, os.MkdirAll(writeCommitConfig.alternateObjectDir, mode.Directory))

		env = append(env,
			fmt.Sprintf("GIT_OBJECT_DIRECTORY=%s", writeCommitConfig.alternateObjectDir),
			fmt.Sprintf("GIT_ALTERNATE_OBJECT_DIRECTORIES=%s", filepath.Join(repoPath, "objects")),
		)
	}

	env = append(env,
		fmt.Sprintf("GIT_AUTHOR_DATE=%s", git.FormatTime(writeCommitConfig.authorDate)),
		fmt.Sprintf("GIT_AUTHOR_NAME=%s", writeCommitConfig.authorName),
		fmt.Sprintf("GIT_AUTHOR_EMAIL=%s", DefaultCommitterMail),
		fmt.Sprintf("GIT_COMMITTER_DATE=%s", git.FormatTime(writeCommitConfig.committerDate)),
		fmt.Sprintf("GIT_COMMITTER_NAME=%s", writeCommitConfig.committerName),
		fmt.Sprintf("GIT_COMMITTER_EMAIL=%s", DefaultCommitterMail),
	)

	for _, parent := range writeCommitConfig.parents {
		commitArgs = append(commitArgs, "-p", parent.String())
	}

	stdout := ExecOpts(tb, cfg, ExecConfig{
		Stdin: stdin,
		Env:   env,
	}, commitArgs...)
	oid, err := DefaultObjectHash.FromHex(text.ChompBytes(stdout))
	require.NoError(tb, err)

	if writeCommitConfig.reference != "" {
		ExecOpts(tb, cfg, ExecConfig{
			Env: env,
		}, "-C", repoPath, "update-ref", writeCommitConfig.reference, oid.String())
	}

	return oid
}

func authorEqualIgnoringDate(tb testing.TB, expected *gitalypb.CommitAuthor, actual *gitalypb.CommitAuthor) {
	tb.Helper()
	require.Equal(tb, expected.GetName(), actual.GetName(), "author name does not match")
	require.Equal(tb, expected.GetEmail(), actual.GetEmail(), "author mail does not match")
}

// CommitEqual tests if two `GitCommit`s are equal
func CommitEqual(tb testing.TB, expected, actual *gitalypb.GitCommit) {
	tb.Helper()

	authorEqualIgnoringDate(tb, expected.GetAuthor(), actual.GetAuthor())
	authorEqualIgnoringDate(tb, expected.GetCommitter(), actual.GetCommitter())
	require.Equal(tb, expected.GetBody(), actual.GetBody(), "body does not match")
	require.Equal(tb, expected.GetSubject(), actual.GetSubject(), "subject does not match")
	require.Equal(tb, expected.GetId(), actual.GetId(), "object ID does not match")
	require.Equal(tb, expected.GetParentIds(), actual.GetParentIds(), "parent IDs do not match")
}
