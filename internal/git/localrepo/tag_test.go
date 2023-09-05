package localrepo

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v16/internal/helper/text"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
)

func TestFormatTag(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		desc       string
		objectID   git.ObjectID
		objectType string
		tagName    []byte
		tagBody    []byte
		author     *gitalypb.User
		authorDate time.Time
		err        error
	}{
		// Just trivial tests here, most of this is tested in
		// internal/gitaly/service/operations/tags_test.go
		{
			desc:       "basic signature",
			objectID:   gittest.DefaultObjectHash.ZeroOID,
			objectType: "commit",
			tagName:    []byte("my-tag"),
			author: &gitalypb.User{
				Name:  []byte("root"),
				Email: []byte("root@localhost"),
			},
			tagBody: []byte(""),
		},
		{
			desc:       "basic signature",
			objectID:   gittest.DefaultObjectHash.ZeroOID,
			objectType: "commit",
			tagName:    []byte("my-tag\ninjection"),
			tagBody:    []byte(""),
			author: &gitalypb.User{
				Name:  []byte("root"),
				Email: []byte("root@localhost"),
			},
			err: FormatTagError{expectedLines: 4, actualLines: 5},
		},
		{
			desc:       "signature with fixed time",
			objectID:   gittest.DefaultObjectHash.ZeroOID,
			objectType: "commit",
			tagName:    []byte("my-tag"),
			tagBody:    []byte(""),
			author: &gitalypb.User{
				Name:  []byte("root"),
				Email: []byte("root@localhost"),
			},
			authorDate: time.Unix(12345, 0),
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			signature, err := FormatTag(tc.objectID, tc.objectType, tc.tagName, tc.tagBody, tc.author, tc.authorDate)
			if err != nil {
				require.Equal(t, tc.err, err)
				require.Equal(t, "", signature)
			} else {
				require.NoError(t, err)
				require.Contains(t, signature, "object ")
				require.Contains(t, signature, "tag ")
				require.Contains(t, signature, "tagger ")
			}
		})
	}
}

func TestRepo_WriteTag(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)

	cfg, repo, repoPath := setupRepo(t)

	commitID := gittest.WriteCommit(t, cfg, repoPath)

	for _, tc := range []struct {
		desc        string
		objectID    git.ObjectID
		objectType  string
		tagName     []byte
		tagBody     []byte
		author      *gitalypb.User
		authorDate  time.Time
		expectedTag string
	}{
		// Just trivial tests here, most of this is tested in
		// internal/gitaly/service/operations/tags_test.go
		{
			desc:       "basic signature",
			objectID:   commitID,
			objectType: "commit",
			tagName:    []byte("my-tag"),
			tagBody:    []byte(""),
			author: &gitalypb.User{
				Name:  []byte("root"),
				Email: []byte("root@localhost"),
			},
		},
		{
			desc:       "signature with time",
			objectID:   commitID,
			objectType: "commit",
			tagName:    []byte("tag-with-timestamp"),
			tagBody:    []byte(""),
			author: &gitalypb.User{
				Name:  []byte("root"),
				Email: []byte("root@localhost"),
			},
			authorDate: time.Unix(12345, 0).UTC(),
			expectedTag: fmt.Sprintf(`object %s
type commit
tag tag-with-timestamp
tagger root <root@localhost> 12345 +0000
`, commitID),
		},
		{
			desc:       "signature with time and timezone",
			objectID:   commitID,
			objectType: "commit",
			tagName:    []byte("tag-with-timezone"),
			tagBody:    []byte(""),
			author: &gitalypb.User{
				Name:  []byte("root"),
				Email: []byte("root@localhost"),
			},
			authorDate: time.Unix(12345, 0).In(time.FixedZone("myzone", -60*60)),
			expectedTag: fmt.Sprintf(`object %s
type commit
tag tag-with-timezone
tagger root <root@localhost> 12345 -0100
`, commitID),
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			tagObjID, err := repo.WriteTag(ctx, tc.objectID, tc.objectType, tc.tagName, tc.tagBody, tc.author, tc.authorDate)
			require.NoError(t, err)

			repoTagObjID := gittest.Exec(t, cfg, "-C", repoPath, "rev-parse", tagObjID.String())
			require.Equal(t, text.ChompBytes(repoTagObjID), tagObjID.String())

			if tc.expectedTag != "" {
				tag := gittest.Exec(t, cfg, "-C", repoPath, "cat-file", "-p", tagObjID.String())
				require.Equal(t, tc.expectedTag, text.ChompBytes(tag))
			}
		})
	}
}
