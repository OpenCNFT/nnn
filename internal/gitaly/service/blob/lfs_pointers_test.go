package blob

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/catfile"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/gitcmd"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/quarantine"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/storagemgr"
	"gitlab.com/gitlab-org/gitaly/v16/internal/helper/text"
	"gitlab.com/gitlab-org/gitaly/v16/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
	"google.golang.org/grpc/metadata"
)

func TestListLFSPointers(t *testing.T) {
	ctx := testhelper.Context(t)
	cfg, client := setup(t, ctx)
	repo, _, repoInfo := setupRepoWithLFS(t, ctx, cfg)

	ctx = testhelper.MergeOutgoingMetadata(ctx,
		metadata.Pairs(catfile.SessionIDField, "1"),
	)

	for _, tc := range []struct {
		desc             string
		revs             []string
		limit            int32
		expectedPointers []*gitalypb.LFSPointer
		expectedErr      error
	}{
		{
			desc:        "missing revisions",
			revs:        []string{},
			expectedErr: structerr.NewInvalidArgument("missing revisions"),
		},
		{
			desc: "invalid revision",
			revs: []string{"-dashed"},
			expectedErr: testhelper.WithInterceptedMetadata(
				structerr.NewInvalidArgument("invalid revision: revision can't start with '-'"),
				"revision", "-dashed",
			),
		},
		{
			desc: "object IDs",
			revs: []string{
				repoInfo.lfsPointers[0].GetOid(),
				repoInfo.lfsPointers[1].GetOid(),
				repoInfo.lfsPointers[2].GetOid(),
				repoInfo.defaultTreeID.String(),   // tree
				repoInfo.defaultCommitID.String(), // commit
			},
			expectedPointers: []*gitalypb.LFSPointer{
				repoInfo.lfsPointers[0],
				repoInfo.lfsPointers[1],
				repoInfo.lfsPointers[2],
			},
		},
		{
			desc: "revision",
			revs: []string{"refs/heads/master"},
			expectedPointers: []*gitalypb.LFSPointer{
				repoInfo.lfsPointers[0],
			},
		},
		{
			desc: "pseudo-revisions",
			revs: []string{"refs/heads/master", "--not", "--all"},
		},
		{
			desc: "partial graph walk",
			revs: []string{"--all", "--not", "refs/heads/master"},
			expectedPointers: []*gitalypb.LFSPointer{
				repoInfo.lfsPointers[1],
				repoInfo.lfsPointers[2],
				repoInfo.lfsPointers[3],
				repoInfo.lfsPointers[4],
				repoInfo.lfsPointers[5],
			},
		},
		{
			desc:  "partial graph walk with matching limit",
			revs:  []string{"--all", "--not", "refs/heads/master"},
			limit: 5,
			expectedPointers: []*gitalypb.LFSPointer{
				repoInfo.lfsPointers[1],
				repoInfo.lfsPointers[2],
				repoInfo.lfsPointers[3],
				repoInfo.lfsPointers[4],
				repoInfo.lfsPointers[5],
			},
		},
		{
			desc:  "partial graph walk with limiting limit",
			revs:  []string{"--all", "--not", "refs/heads/master"},
			limit: 3,
			expectedPointers: []*gitalypb.LFSPointer{
				repoInfo.lfsPointers[3],
				repoInfo.lfsPointers[4],
				repoInfo.lfsPointers[5],
			},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			stream, err := client.ListLFSPointers(ctx, &gitalypb.ListLFSPointersRequest{
				Repository: repo,
				Revisions:  tc.revs,
				Limit:      tc.limit,
			})
			require.NoError(t, err)

			var actualLFSPointers []*gitalypb.LFSPointer
			for {
				resp, err := stream.Recv()
				if errors.Is(err, io.EOF) {
					break
				}
				testhelper.RequireGrpcError(t, tc.expectedErr, err)
				if err != nil {
					break
				}

				actualLFSPointers = append(actualLFSPointers, resp.GetLfsPointers()...)
			}
			lfsPointersEqual(t, tc.expectedPointers, actualLFSPointers)
		})
	}
}

func TestListAllLFSPointers(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg, client := setup(t, ctx)

	lfsPointerContents := `version https://git-lfs.github.com/spec/v1
oid sha256:1111111111111111111111111111111111111111111111111111111111111111
size 12345`

	type setupData struct {
		repo             *gitalypb.Repository
		expectedErr      error
		expectedPointers []*gitalypb.LFSPointer
	}

	for _, tc := range []struct {
		desc  string
		setup func(t *testing.T) setupData
	}{
		{
			desc: "missing repository",
			setup: func(t *testing.T) setupData {
				return setupData{
					expectedErr: structerr.NewInvalidArgument("%w", storage.ErrRepositoryNotSet),
				}
			},
		},
		{
			desc: "normal repository",
			setup: func(t *testing.T) setupData {
				repo, _, repoInfo := setupRepoWithLFS(t, ctx, cfg)

				return setupData{
					repo: repo,
					expectedPointers: []*gitalypb.LFSPointer{
						repoInfo.lfsPointers[0],
						repoInfo.lfsPointers[1],
						repoInfo.lfsPointers[2],
						repoInfo.lfsPointers[3],
						repoInfo.lfsPointers[4],
						repoInfo.lfsPointers[5],
					},
				}
			},
		},
		{
			desc: "dangling pointer",
			setup: func(t *testing.T) setupData {
				repo, repoPath, repoInfo := setupRepoWithLFS(t, ctx, cfg)

				hash := gittest.ExecOpts(t, cfg, gittest.ExecConfig{Stdin: strings.NewReader(lfsPointerContents)},
					"-C", repoPath, "hash-object", "-w", "--stdin",
				)
				lfsPointerOID := text.ChompBytes(hash)

				return setupData{
					repo: repo,
					expectedPointers: []*gitalypb.LFSPointer{
						{
							Size:     int64(len(lfsPointerContents)),
							Data:     []byte(lfsPointerContents),
							Oid:      lfsPointerOID,
							FileSize: 12345,
							FileOid:  []byte("1111111111111111111111111111111111111111111111111111111111111111"),
						},
						repoInfo.lfsPointers[0],
						repoInfo.lfsPointers[1],
						repoInfo.lfsPointers[2],
						repoInfo.lfsPointers[3],
						repoInfo.lfsPointers[4],
						repoInfo.lfsPointers[5],
					},
				}
			},
		},
		{
			desc: "empty quarantine directory",
			setup: func(t *testing.T) setupData {
				repo, _, _ := setupRepoWithLFS(t, ctx, cfg)

				quarantineDir, err := quarantine.New(ctx, gittest.RewrittenRepository(t, ctx, cfg, repo), testhelper.NewLogger(t), config.NewLocator(cfg))
				require.NoError(t, err)

				repo.GitObjectDirectory = quarantineDir.QuarantinedRepo().GetGitObjectDirectory()

				// There are no quarantined objects yet, so none should be returned
				// here.
				return setupData{
					repo:             repo,
					expectedPointers: nil,
				}
			},
		},
		{
			desc: "populated quarantine directory",
			setup: func(t *testing.T) setupData {
				repo, _, _ := setupRepoWithLFS(t, ctx, cfg)

				// We're emulating the case where git is receiving data via a push,
				// where objects are stored in a separate quarantine environment. In
				// this case, LFS pointer checks may want to inspect all newly
				// pushed objects, denoted by a repository proto message which only
				// has its object directory set to the quarantine directory.
				quarantineDir, err := quarantine.New(ctx, gittest.RewrittenRepository(t, ctx, cfg, repo), testhelper.NewLogger(t), config.NewLocator(cfg))
				require.NoError(t, err)

				// Note that we need to continue using the non-rewritten repository
				// here as `localrepo.NewTestRepo()` already will try to rewrite it
				// again.
				repo.GitObjectDirectory = quarantineDir.QuarantinedRepo().GetGitObjectDirectory()

				// Write a new object into the repository. Because we set
				// GIT_OBJECT_DIRECTORY to the quarantine directory, objects will be
				// written in there instead of into the repository's normal object
				// directory.
				quarantineRepo := localrepo.NewTestRepo(t, cfg, repo)
				var buffer bytes.Buffer
				require.NoError(t, quarantineRepo.ExecAndWait(ctx, gitcmd.Command{
					Name: "hash-object",
					Flags: []gitcmd.Option{
						gitcmd.Flag{Name: "-w"},
						gitcmd.Flag{Name: "--stdin"},
					},
				}, gitcmd.WithStdin(strings.NewReader(lfsPointerContents)), gitcmd.WithStdout(&buffer)))

				return setupData{
					repo: repo,
					expectedPointers: []*gitalypb.LFSPointer{
						{
							Oid:      text.ChompBytes(buffer.Bytes()),
							Data:     []byte(lfsPointerContents),
							Size:     int64(len(lfsPointerContents)),
							FileOid:  []byte("1111111111111111111111111111111111111111111111111111111111111111"),
							FileSize: 12345,
						},
					},
				}
			},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			setup := tc.setup(t)

			ctx := ctx
			if setup.repo.GetGitObjectDirectory() != "" {
				// Rails sends the repository's relative path from the access checks as provided by Gitaly. If transactions are enabled,
				// this is the snapshot's relative path. Include the metadata in the test as well as we're testing requests with quarantine
				// as if they were coming from access checks.
				ctx = metadata.AppendToOutgoingContext(ctx, storagemgr.MetadataKeySnapshotRelativePath,
					// Gitaly sends the snapshot's relative path to Rails from `pre-receive` and Rails
					// sends it back to Gitaly when it performs requests in the access checks. The repository
					// would have already been rewritten by Praefect, so we have to adjust for that as well.
					gittest.RewrittenRepository(t, ctx, cfg, setup.repo).GetRelativePath(),
				)
			}

			stream, err := client.ListAllLFSPointers(ctx, &gitalypb.ListAllLFSPointersRequest{
				Repository: setup.repo,
			})
			require.NoError(t, err)

			var pointers []*gitalypb.LFSPointer
			for {
				resp, err := stream.Recv()
				if errors.Is(err, io.EOF) {
					break
				}

				testhelper.RequireGrpcError(t, setup.expectedErr, err)
				if err != nil {
					break
				}

				pointers = append(pointers, resp.GetLfsPointers()...)
			}
			lfsPointersEqual(t, setup.expectedPointers, pointers)
		})
	}
}

func TestGetLFSPointers(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg, client := setup(t, ctx)
	repo, _, repoInfo := setupRepoWithLFS(t, ctx, cfg)

	for _, tc := range []struct {
		desc             string
		request          *gitalypb.GetLFSPointersRequest
		expectedErr      error
		expectedPointers []*gitalypb.LFSPointer
	}{
		{
			desc: "unset repository",
			request: &gitalypb.GetLFSPointersRequest{
				Repository: nil,
				BlobIds:    []string{"f00"},
			},
			expectedErr: structerr.NewInvalidArgument("%w", storage.ErrRepositoryNotSet),
		},
		{
			desc: "empty BlobIds",
			request: &gitalypb.GetLFSPointersRequest{
				Repository: repo,
				BlobIds:    nil,
			},
			expectedErr: structerr.NewInvalidArgument("empty BlobIds"),
		},
		{
			desc: "successful request",
			request: &gitalypb.GetLFSPointersRequest{
				Repository: repo,
				BlobIds: []string{
					repoInfo.lfsPointers[0].GetOid(),
					repoInfo.lfsPointers[1].GetOid(),
					repoInfo.lfsPointers[2].GetOid(),
				},
			},
			expectedPointers: []*gitalypb.LFSPointer{
				repoInfo.lfsPointers[0],
				repoInfo.lfsPointers[1],
				repoInfo.lfsPointers[2],
			},
		},
		{
			desc: "mixed pointers and blobs",
			request: &gitalypb.GetLFSPointersRequest{
				Repository: repo,
				BlobIds: []string{
					repoInfo.lfsPointers[0].GetOid(),
					repoInfo.lfsPointers[1].GetOid(),
					repoInfo.defaultTreeID.String(),
					repoInfo.lfsPointers[2].GetOid(),
					repoInfo.defaultCommitID.String(),
				},
			},
			expectedPointers: []*gitalypb.LFSPointer{
				repoInfo.lfsPointers[0],
				repoInfo.lfsPointers[1],
				repoInfo.lfsPointers[2],
			},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			stream, err := client.GetLFSPointers(ctx, tc.request)
			require.NoError(t, err)

			var receivedPointers []*gitalypb.LFSPointer
			for {
				resp, err := stream.Recv()
				if errors.Is(err, io.EOF) {
					break
				}

				testhelper.RequireGrpcError(t, tc.expectedErr, err)
				if err != nil {
					break
				}

				receivedPointers = append(receivedPointers, resp.GetLfsPointers()...)
			}

			lfsPointersEqual(t, receivedPointers, tc.expectedPointers)
		})
	}
}

func lfsPointersEqual(tb testing.TB, expected, actual []*gitalypb.LFSPointer) {
	tb.Helper()

	for _, slice := range [][]*gitalypb.LFSPointer{expected, actual} {
		sort.Slice(slice, func(i, j int) bool {
			return strings.Compare(slice[i].GetOid(), slice[j].GetOid()) < 0
		})
	}

	require.Equal(tb, len(expected), len(actual))
	for i := range expected {
		testhelper.ProtoEqual(tb, expected[i], actual[i])
	}
}

type lfsRepoInfo struct {
	// defaultCommitID is the object ID of the commit pointed to by the default branch.
	defaultCommitID git.ObjectID
	// defaultTreeID is the object ID of the tree pointed to by the default branch.
	defaultTreeID git.ObjectID

	lfsPointers []*gitalypb.LFSPointer
}

// setRepoWithLFS configures a git repository with LFS pointers to be used in
// testing. The commit OID and root tree OID of the default branch are returned
// for use with some tests.
func setupRepoWithLFS(tb testing.TB, ctx context.Context, cfg config.Cfg) (*gitalypb.Repository, string, lfsRepoInfo) {
	tb.Helper()

	repo, repoPath := gittest.CreateRepository(tb, ctx, cfg)

	var lfsPointers []*gitalypb.LFSPointer
	for _, lfsPointer := range []struct {
		fileOID  string
		fileSize int64
	}{
		{
			fileOID:  "91eff75a492a3ed0dfcb544d7f31326bc4014c8551849c192fd1e48d4dd2c897",
			fileSize: 1575078,
		},
		{
			fileOID:  "f2b0a1e7550e9b718dafc9b525a04879a766de62e4fbdfc46593d47f7ab74636",
			fileSize: 20,
		},
		{
			fileOID:  "bad71f905b60729f502ca339f7c9f001281a3d12c68a5da7f15de8009f4bd63d",
			fileSize: 18,
		},
		{
			fileOID:  "47997ea7ecff33be61e3ca1cc287ee72a2125161518f1a169f2893a5a82e9d95",
			fileSize: 7501,
		},
		{
			fileOID:  "8c1e8de917525f83104736f6c64d32f0e2a02f5bf2ee57843a54f222cba8c813",
			fileSize: 2797,
		},
		{
			fileOID:  "96f74c6fe7a2979eefb9ec74a5dfc6888fb25543cf99b77586b79afea1da6f97",
			fileSize: 1219696,
		},
	} {
		content := fmt.Sprintf("version https://git-lfs.github.com/spec/v1\noid sha256:%s\nsize %d\n\n", lfsPointer.fileOID, lfsPointer.fileSize)
		lfsPointerOID := gittest.WriteBlob(tb, cfg, repoPath, []byte(content))
		lfsPointers = append(lfsPointers, &gitalypb.LFSPointer{
			Data:     []byte(content),
			Oid:      lfsPointerOID.String(),
			Size:     int64(len(content)),
			FileOid:  []byte(lfsPointer.fileOID),
			FileSize: lfsPointer.fileSize,
		})
	}

	masterTreeID := gittest.WriteTree(tb, cfg, repoPath, []gittest.TreeEntry{
		{Mode: "100644", Path: lfsPointers[0].GetOid(), Content: string(lfsPointers[0].GetData())},
	})
	masterCommitID := gittest.WriteCommit(tb, cfg, repoPath,
		gittest.WithTree(masterTreeID),
		gittest.WithBranch("master"),
	)

	_ = gittest.WriteCommit(tb, cfg, repoPath,
		gittest.WithTreeEntries(
			gittest.TreeEntry{Mode: "100644", Path: lfsPointers[1].GetOid(), Content: string(lfsPointers[1].GetData())},
			gittest.TreeEntry{Mode: "100644", Path: lfsPointers[2].GetOid(), Content: string(lfsPointers[2].GetData())},
		),
		gittest.WithBranch("foo"),
	)

	_ = gittest.WriteCommit(tb, cfg, repoPath,
		gittest.WithTreeEntries(
			gittest.TreeEntry{Mode: "100644", Path: lfsPointers[3].GetOid(), Content: string(lfsPointers[3].GetData())},
			gittest.TreeEntry{Mode: "100644", Path: lfsPointers[4].GetOid(), Content: string(lfsPointers[4].GetData())},
			gittest.TreeEntry{Mode: "100644", Path: lfsPointers[5].GetOid(), Content: string(lfsPointers[5].GetData())},
		),
		gittest.WithBranch("bar"),
	)

	return repo, repoPath, lfsRepoInfo{
		defaultCommitID: masterCommitID,
		defaultTreeID:   masterTreeID,
		lfsPointers:     lfsPointers,
	}
}
