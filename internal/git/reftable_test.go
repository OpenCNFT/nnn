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
		repoPath   string
		references []git.Reference
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
					references: []git.Reference{
						{
							Name:       "HEAD",
							Target:     "refs/heads/main",
							IsSymbolic: true,
						},
						{
							Name:   "refs/heads/main",
							Target: mainCommit.String(),
						},
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
					references: []git.Reference{
						{
							Name:       "HEAD",
							Target:     "refs/heads/main",
							IsSymbolic: true,
						},
						{
							Name:   "refs/heads/main",
							Target: mainCommit.String(),
						},
						{
							Name:   "refs/tags/v2.0.0",
							Target: annotatedTag.String(),
						},
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
					references: []git.Reference{
						{
							Name:       "HEAD",
							Target:     "refs/heads/main",
							IsSymbolic: true,
						},
						{
							Name:   "ROOTREF",
							Target: rootRefCommit.String(),
						},
						{
							Name:   "refs/heads/main",
							Target: mainCommit.String(),
						},
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
					references: []git.Reference{
						{
							Name:       "HEAD",
							Target:     "refs/heads/main",
							IsSymbolic: true,
						},
						{
							Name:   "refs/heads/main",
							Target: mainCommit.String(),
						},
						{
							Name:   "refs/heads/master",
							Target: masterCommit.String(),
						},
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
					references: []git.Reference{
						{
							Name:       "HEAD",
							Target:     "refs/heads/main",
							IsSymbolic: true,
						},
						{
							Name:   "refs/heads/main",
							Target: mainCommit.String(),
						},
						{
							Name:   "refs/heads/master",
							Target: masterCommit.String(),
						},
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

				var references []git.Reference

				references = append(references, git.Reference{
					Name:       "HEAD",
					Target:     "refs/heads/main",
					IsSymbolic: true,
				})

				for i := 0; i < 100; i++ {
					branch := fmt.Sprintf("branch%02d", i)
					references = append(references, git.Reference{
						Name:   git.ReferenceName(fmt.Sprintf("refs/heads/%s", branch)),
						Target: gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch(branch)).String(),
					})
				}

				return setupData{
					repoPath:   repoPath,
					references: references,
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

			references, err := table.IterateRefs()
			require.NoError(t, err)

			require.Equal(t, setup.references, references)
		})
	}
}
