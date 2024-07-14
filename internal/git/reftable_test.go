package git_test

import (
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper/testcfg"
)

func getReftables(repoPath string) []string {
	tables := []string{}

	reftablePath := filepath.Join(repoPath, "reftable")

	files, err := os.ReadDir(reftablePath)
	if err != nil {
		log.Fatal(err)
	}

	for _, file := range files {
		if filepath.Base(file.Name()) == "tables.list" {
			continue
		}

		tables = append(tables, filepath.Join(reftablePath, file.Name()))
	}

	return tables
}

func TestParseReftable(t *testing.T) {
	t.Parallel()

	if !testhelper.IsReftableEnabled() {
		t.Skip("tests are reftable specific")
	}

	ctx := testhelper.Context(t)
	cfg := testcfg.Build(t)

	tableName := [4]byte{}
	n, err := strings.NewReader("REFT").Read(tableName[:])
	require.NoError(t, err)
	require.Equal(t, 4, n)

	type setupData struct {
		repoPath string
		updates  git.ReferenceUpdates
	}

	for _, tc := range []struct {
		name        string
		setup       func() setupData
		expectedErr error
	}{
		{
			name: "single ref",
			setup: func() setupData {
				_, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
					SkipCreationViaService: true,
				})

				mainCommit := gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("main"))

				return setupData{
					repoPath: repoPath,
					updates: git.ReferenceUpdates{
						"HEAD":            {NewTarget: "refs/heads/main"},
						"refs/heads/main": {NewOID: mainCommit},
					},
				}
			},
		},
		{
			name: "single ref + annotated tag",
			setup: func() setupData {
				_, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
					SkipCreationViaService: true,
				})

				mainCommit := gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("main"))
				annotatedTag := gittest.WriteTag(t, cfg, repoPath, "v2.0.0", mainCommit.Revision(), gittest.WriteTagConfig{
					Message: "annotated tag",
				})

				return setupData{
					repoPath: repoPath,
					updates: git.ReferenceUpdates{
						"HEAD":             {NewTarget: "refs/heads/main"},
						"refs/heads/main":  {NewOID: mainCommit},
						"refs/tags/v2.0.0": {NewOID: annotatedTag},
					},
				}
			},
		},
		{
			name: "two refs without prefix compression",
			setup: func() setupData {
				_, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
					SkipCreationViaService: true,
				})

				mainCommit := gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("main"))
				rootRefCommit := gittest.WriteCommit(t, cfg, repoPath, gittest.WithReference("ROOTREF"))

				return setupData{
					repoPath: repoPath,
					updates: git.ReferenceUpdates{
						"HEAD":            {NewTarget: "refs/heads/main"},
						"refs/heads/main": {NewOID: mainCommit},
						"ROOTREF":         {NewOID: rootRefCommit},
					},
				}
			},
		},
		{
			name: "two refs with prefix compression",
			setup: func() setupData {
				_, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
					SkipCreationViaService: true,
				})

				mainCommit := gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("main"))
				masterCommit := gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("master"))

				return setupData{
					repoPath: repoPath,
					updates: git.ReferenceUpdates{
						"HEAD":              {NewTarget: "refs/heads/main"},
						"refs/heads/main":   {NewOID: mainCommit},
						"refs/heads/master": {NewOID: masterCommit},
					},
				}
			},
		},
		{
			name: "multiple refs with different commit IDs",
			setup: func() setupData {
				_, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
					SkipCreationViaService: true,
				})

				mainCommit := gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("main"))
				masterCommit := gittest.WriteCommit(t, cfg, repoPath, gittest.WithParents(mainCommit), gittest.WithBranch("master"))

				return setupData{
					repoPath: repoPath,
					updates: git.ReferenceUpdates{
						"HEAD":              {NewTarget: "refs/heads/main"},
						"refs/heads/main":   {NewOID: mainCommit},
						"refs/heads/master": {NewOID: masterCommit},
					},
				}
			},
		},
		{
			name: "multiple blocks in table",
			setup: func() setupData {
				_, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
					SkipCreationViaService: true,
				})

				updates := make(map[git.ReferenceName]git.ReferenceUpdate)

				updates["HEAD"] = git.ReferenceUpdate{NewTarget: "refs/heads/main"}

				for i := 0; i < 200; i++ {
					branch := fmt.Sprintf("branch%d", i)
					updates[git.ReferenceName(fmt.Sprintf("refs/heads/%s", branch))] = git.ReferenceUpdate{
						NewOID: gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch(branch)),
					}
				}

				return setupData{
					repoPath: repoPath,
					updates:  updates,
				}
			},
		},
	} {
		tc := tc

		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			setup := tc.setup()

			repoPath := setup.repoPath

			// pack-refs so there is only one table
			gittest.Exec(t, cfg, "-C", repoPath, "pack-refs")
			reftablePath := getReftables(repoPath)[0]

			file, err := os.Open(reftablePath)
			require.NoError(t, err)
			defer file.Close()

			buf, err := io.ReadAll(file)
			require.NoError(t, err)

			table, err := git.NewReftable(buf)
			require.NoError(t, err)

			u, err := table.IterateRefs()
			require.NoError(t, err)

			require.Equal(t, len(setup.updates), len(u))
			for ref, expectedUpdate := range setup.updates {
				update, ok := u[ref]
				require.True(t, ok)
				require.Equal(t, expectedUpdate, update)
			}
		})
	}
}
