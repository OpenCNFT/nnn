package blob

import (
	"bytes"
	"errors"
	"io"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/quarantine"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/storagemgr"
	"gitlab.com/gitlab-org/gitaly/v16/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"
)

func TestListBlobs(t *testing.T) {
	ctx := testhelper.Context(t)

	cfg, client := setup(t, ctx)
	repoProto, repoPath, repoInfo := setupRepoWithLFS(t, ctx, cfg)

	blobASize := int64(251)
	blobAData := bytes.Repeat([]byte{'a'}, int(blobASize))
	blobAOID := gittest.WriteBlob(t, cfg, repoPath, blobAData)

	blobBSize := int64(64)
	blobBData := bytes.Repeat([]byte{'b'}, int(blobBSize))
	blobBOID := gittest.WriteBlob(t, cfg, repoPath, blobBData)

	treeOID := gittest.WriteTree(t, cfg, repoPath, []gittest.TreeEntry{
		{Path: "a", Mode: "100644", OID: blobAOID},
		{Path: "b", Mode: "100644", OID: blobBOID},
	})

	gittest.WriteCommit(t, cfg, repoPath, gittest.WithParents(repoInfo.defaultCommitID),
		gittest.WithTree(treeOID),
		gittest.WithBranch("master"),
	)

	// Test with a large blob to ensure the response is correctly chunked. Use 8MiB
	// to ensure we exceed maximum gRPC message size of 4MiB.
	var chunkedBlob []byte
	for i := 0; len(chunkedBlob) <= 8*1024*1024; i++ {
		chunkedBlob = append(chunkedBlob, []byte(strconv.Itoa(i)+"\n")...)
	}
	chunkedBlobID := gittest.WriteBlob(t, cfg, repoPath, chunkedBlob)

	for _, tc := range []struct {
		desc          string
		revisions     []string
		limit         uint32
		bytesLimit    int64
		withPaths     bool
		expectedErr   error
		expectedBlobs []*gitalypb.ListBlobsResponse_Blob
		verify        func(*testing.T, []*gitalypb.ListBlobsResponse_Blob)
	}{
		{
			desc:        "missing revisions",
			revisions:   []string{},
			expectedErr: structerr.NewInvalidArgument("missing revisions"),
		},
		{
			desc: "invalid revision",
			revisions: []string{
				"--foobar",
			},
			expectedErr: testhelper.WithInterceptedMetadata(
				structerr.NewInvalidArgument("invalid revision: revision can't start with '-'"),
				"revision", "--foobar",
			),
		},
		{
			desc: "single blob",
			revisions: []string{
				repoInfo.lfsPointers[0].GetOid(),
			},
			expectedBlobs: []*gitalypb.ListBlobsResponse_Blob{
				{Oid: repoInfo.lfsPointers[0].GetOid(), Size: repoInfo.lfsPointers[0].GetSize()},
			},
		},
		{
			desc: "revision and path",
			revisions: []string{
				"master:a",
			},
			expectedBlobs: []*gitalypb.ListBlobsResponse_Blob{
				{Oid: blobAOID.String(), Size: blobASize},
			},
		},
		{
			desc: "single blob with paths",
			revisions: []string{
				repoInfo.lfsPointers[0].GetOid(),
			},
			withPaths: true,
			// When iterating blobs directly, we cannot deduce a path and thus don't get
			// any as response.
			expectedBlobs: []*gitalypb.ListBlobsResponse_Blob{
				{Oid: repoInfo.lfsPointers[0].GetOid(), Size: repoInfo.lfsPointers[0].GetSize()},
			},
		},
		{
			desc: "multiple blobs",
			revisions: []string{
				repoInfo.lfsPointers[0].GetOid(),
				repoInfo.lfsPointers[1].GetOid(),
				repoInfo.lfsPointers[2].GetOid(),
			},
			expectedBlobs: []*gitalypb.ListBlobsResponse_Blob{
				{Oid: repoInfo.lfsPointers[0].GetOid(), Size: repoInfo.lfsPointers[0].GetSize()},
				{Oid: repoInfo.lfsPointers[1].GetOid(), Size: repoInfo.lfsPointers[1].GetSize()},
				{Oid: repoInfo.lfsPointers[2].GetOid(), Size: repoInfo.lfsPointers[2].GetSize()},
			},
		},
		{
			desc: "tree",
			revisions: []string{
				treeOID.String(),
			},
			expectedBlobs: []*gitalypb.ListBlobsResponse_Blob{
				{Oid: blobAOID.String(), Size: blobASize},
				{Oid: blobBOID.String(), Size: blobBSize},
			},
		},
		{
			desc: "tree with paths",
			revisions: []string{
				treeOID.String(),
			},
			withPaths: true,
			expectedBlobs: []*gitalypb.ListBlobsResponse_Blob{
				{Oid: blobAOID.String(), Size: blobASize, Path: []byte("a")},
				{Oid: blobBOID.String(), Size: blobBSize, Path: []byte("b")},
			},
		},
		{
			desc: "revision range",
			revisions: []string{
				"master",
				"^master~",
			},
			expectedBlobs: []*gitalypb.ListBlobsResponse_Blob{
				{Oid: blobAOID.String(), Size: blobASize},
				{Oid: blobBOID.String(), Size: blobBSize},
			},
		},
		{
			desc: "pseudorevisions",
			revisions: []string{
				"master",
				"--not",
				"--all",
			},
			expectedBlobs: nil,
		},
		{
			desc: "revision with limit",
			revisions: []string{
				"master",
			},
			limit: 2,
			expectedBlobs: []*gitalypb.ListBlobsResponse_Blob{
				{Oid: blobAOID.String(), Size: blobASize},
				{Oid: blobBOID.String(), Size: blobBSize},
			},
		},
		{
			desc: "revision with limit and path",
			revisions: []string{
				"master",
			},
			limit:     2,
			withPaths: true,
			expectedBlobs: []*gitalypb.ListBlobsResponse_Blob{
				{Oid: blobAOID.String(), Size: blobASize, Path: []byte("a")},
				{Oid: blobBOID.String(), Size: blobBSize, Path: []byte("b")},
			},
		},
		{
			desc: "revision with limit and bytes limit",
			revisions: []string{
				"master",
			},
			limit:      2,
			bytesLimit: 5,
			expectedBlobs: []*gitalypb.ListBlobsResponse_Blob{
				{Oid: blobAOID.String(), Size: blobASize, Data: []byte("aaaaa")},
				{Oid: blobBOID.String(), Size: blobBSize, Data: []byte("bbbbb")},
			},
		},
		{
			desc: "revision with path",
			revisions: []string{
				"master:b",
			},
			bytesLimit: -1,
			expectedBlobs: []*gitalypb.ListBlobsResponse_Blob{
				{Oid: blobBOID.String(), Size: blobBSize, Data: blobBData},
			},
		},
		{
			desc: "complete contents via negative bytes limit",
			revisions: []string{
				repoInfo.lfsPointers[0].GetOid(),
				repoInfo.lfsPointers[1].GetOid(),
				repoInfo.lfsPointers[2].GetOid(),
			},
			bytesLimit: -1,
			expectedBlobs: []*gitalypb.ListBlobsResponse_Blob{
				{Oid: repoInfo.lfsPointers[0].GetOid(), Size: repoInfo.lfsPointers[0].GetSize(), Data: repoInfo.lfsPointers[0].GetData()},
				{Oid: repoInfo.lfsPointers[1].GetOid(), Size: repoInfo.lfsPointers[1].GetSize(), Data: repoInfo.lfsPointers[1].GetData()},
				{Oid: repoInfo.lfsPointers[2].GetOid(), Size: repoInfo.lfsPointers[2].GetSize(), Data: repoInfo.lfsPointers[2].GetData()},
			},
		},
		{
			desc: "contents truncated by bytes limit",
			revisions: []string{
				repoInfo.lfsPointers[0].GetOid(),
				repoInfo.lfsPointers[1].GetOid(),
				repoInfo.lfsPointers[2].GetOid(),
			},
			bytesLimit: 10,
			expectedBlobs: []*gitalypb.ListBlobsResponse_Blob{
				{Oid: repoInfo.lfsPointers[0].GetOid(), Size: repoInfo.lfsPointers[0].GetSize(), Data: repoInfo.lfsPointers[0].GetData()[:10]},
				{Oid: repoInfo.lfsPointers[1].GetOid(), Size: repoInfo.lfsPointers[1].GetSize(), Data: repoInfo.lfsPointers[1].GetData()[:10]},
				{Oid: repoInfo.lfsPointers[2].GetOid(), Size: repoInfo.lfsPointers[2].GetSize(), Data: repoInfo.lfsPointers[2].GetData()[:10]},
			},
		},
		{
			desc: "bytes limit exceeding total blob content size",
			revisions: []string{
				repoInfo.lfsPointers[0].GetOid(),
				repoInfo.lfsPointers[1].GetOid(),
				repoInfo.lfsPointers[2].GetOid(),
			},
			bytesLimit: 9000,
			expectedBlobs: []*gitalypb.ListBlobsResponse_Blob{
				{Oid: repoInfo.lfsPointers[0].GetOid(), Size: repoInfo.lfsPointers[0].GetSize(), Data: repoInfo.lfsPointers[0].GetData()},
				{Oid: repoInfo.lfsPointers[1].GetOid(), Size: repoInfo.lfsPointers[1].GetSize(), Data: repoInfo.lfsPointers[1].GetData()},
				{Oid: repoInfo.lfsPointers[2].GetOid(), Size: repoInfo.lfsPointers[2].GetSize(), Data: repoInfo.lfsPointers[2].GetData()},
			},
		},
		{
			desc: "bytes limit partially exceeding limit",
			revisions: []string{
				repoInfo.lfsPointers[0].GetOid(),
				repoInfo.lfsPointers[1].GetOid(),
				repoInfo.lfsPointers[2].GetOid(),
			},
			bytesLimit: 128,
			expectedBlobs: []*gitalypb.ListBlobsResponse_Blob{
				{Oid: repoInfo.lfsPointers[0].GetOid(), Size: repoInfo.lfsPointers[0].GetSize(), Data: repoInfo.lfsPointers[0].GetData()[:128]},
				{Oid: repoInfo.lfsPointers[1].GetOid(), Size: repoInfo.lfsPointers[1].GetSize(), Data: repoInfo.lfsPointers[1].GetData()},
				{Oid: repoInfo.lfsPointers[2].GetOid(), Size: repoInfo.lfsPointers[2].GetSize(), Data: repoInfo.lfsPointers[2].GetData()},
			},
		},
		{
			desc:       "blob with content bigger than a gRPC message",
			bytesLimit: -1,
			revisions:  []string{chunkedBlobID.String()},
			verify: func(t *testing.T, blobs []*gitalypb.ListBlobsResponse_Blob) {
				completeBlob := make([]byte, 0, len(chunkedBlob))
				for _, blob := range blobs {
					completeBlob = append(completeBlob, blob.GetData()...)
					blob.Data = nil
				}

				require.Equal(t, chunkedBlob, completeBlob)

				// The first message contains the header.
				testhelper.ProtoEqual(t, &gitalypb.ListBlobsResponse_Blob{
					Oid:  chunkedBlobID.String(),
					Size: int64(len(chunkedBlob)),
				}, blobs[0])

				require.Greater(t, len(blobs), 1, "expected a chunked response")

				// The rest are just data chunks. The actual number doesn't matter as it
				// depends on the buffer sizes used.
				for _, blob := range blobs[1:] {
					testhelper.ProtoEqual(t, &gitalypb.ListBlobsResponse_Blob{}, blob)
				}
			},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			stream, err := client.ListBlobs(ctx, &gitalypb.ListBlobsRequest{
				Repository: repoProto,
				Revisions:  tc.revisions,
				Limit:      tc.limit,
				BytesLimit: tc.bytesLimit,
				WithPaths:  tc.withPaths,
			})
			require.NoError(t, err)

			var blobs []*gitalypb.ListBlobsResponse_Blob
			for {
				resp, err := stream.Recv()
				if err != nil {
					if !errors.Is(err, io.EOF) {
						testhelper.RequireGrpcError(t, tc.expectedErr, err)
					}
					break
				}

				blobs = append(blobs, resp.GetBlobs()...)
			}

			if tc.verify != nil {
				tc.verify(t, blobs)
				return
			}

			testhelper.ProtoEqual(t, tc.expectedBlobs, blobs)
		})
	}
}

func TestListAllBlobs(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg, client := setup(t, ctx)

	repo, _, _ := setupRepoWithLFS(t, ctx, cfg)

	quarantine, err := quarantine.New(ctx, gittest.RewrittenRepository(t, ctx, cfg, repo), testhelper.NewLogger(t), config.NewLocator(cfg))
	require.NoError(t, err)

	// quarantine.New in Gitaly would receive an already rewritten repository. Gitaly would then calculate
	// the quarantine directories based on the rewritten relative path. That quarantine would then be looped
	// through Rails, which would then send a request with the quarantine object directories set based on the
	// rewritten relative path but with the original relative path of the repository. Since we're using the production
	// helpers here, we need to manually substitute the rewritten relative path with the original one when sending
	// it back through the API.
	quarantinedRepo := quarantine.QuarantinedRepo()
	quarantinedRepo.RelativePath = repo.GetRelativePath()

	quarantineRepoWithoutAlternates := proto.Clone(quarantinedRepo).(*gitalypb.Repository)
	quarantineRepoWithoutAlternates.GitAlternateObjectDirectories = []string{}

	emptyRepo, _ := gittest.CreateRepository(t, ctx, cfg)

	singleBlobRepo, singleBlobRepoPath := gittest.CreateRepository(t, ctx, cfg)
	blobID := gittest.WriteBlob(t, cfg, singleBlobRepoPath, []byte("foobar"))

	// Test with a large blob to ensure the response is correctly chunked. Use 8MiB
	// to ensure we exceed maximum gRPC message size of 4MiB.
	chunkedBlobRepo, chunkedBlobRepoPath := gittest.CreateRepository(t, ctx, cfg)
	var chunkedBlob []byte
	for i := 0; len(chunkedBlob) <= 8*1024*1024; i++ {
		chunkedBlob = append(chunkedBlob, []byte(strconv.Itoa(i)+"\n")...)
	}
	chunkedBlobID := gittest.WriteBlob(t, cfg, chunkedBlobRepoPath, chunkedBlob)

	for _, tc := range []struct {
		desc    string
		request *gitalypb.ListAllBlobsRequest
		verify  func(*testing.T, []*gitalypb.ListAllBlobsResponse_Blob)
	}{
		{
			desc: "empty repo",
			request: &gitalypb.ListAllBlobsRequest{
				Repository: emptyRepo,
			},
			verify: func(t *testing.T, blobs []*gitalypb.ListAllBlobsResponse_Blob) {
				require.Empty(t, blobs)
			},
		},
		{
			desc: "repo with single blob",
			request: &gitalypb.ListAllBlobsRequest{
				Repository: singleBlobRepo,
				BytesLimit: -1,
			},
			verify: func(t *testing.T, blobs []*gitalypb.ListAllBlobsResponse_Blob) {
				require.Equal(t, []*gitalypb.ListAllBlobsResponse_Blob{{
					Oid:  blobID.String(),
					Size: 6,
					Data: []byte("foobar"),
				}}, blobs)
			},
		},
		{
			desc: "blob with content bigger than a gRPC message",
			request: &gitalypb.ListAllBlobsRequest{
				Repository: chunkedBlobRepo,
				BytesLimit: -1,
			},
			verify: func(t *testing.T, blobs []*gitalypb.ListAllBlobsResponse_Blob) {
				completeBlob := make([]byte, 0, len(chunkedBlob))
				for _, blob := range blobs {
					completeBlob = append(completeBlob, blob.GetData()...)
					blob.Data = nil
				}

				require.Equal(t, chunkedBlob, completeBlob)

				// The first message contains the header.
				testhelper.ProtoEqual(t, &gitalypb.ListAllBlobsResponse_Blob{
					Oid:  chunkedBlobID.String(),
					Size: int64(len(chunkedBlob)),
				}, blobs[0])

				require.Greater(t, len(blobs), 1, "expected a chunked response")

				// The rest are just data chunks. The actual number doesn't matter as it
				// depends on the buffer sizes used.
				for _, blob := range blobs[1:] {
					testhelper.ProtoEqual(t, &gitalypb.ListAllBlobsResponse_Blob{}, blob)
				}
			},
		},
		{
			desc: "repo with single blob and bytes limit",
			request: &gitalypb.ListAllBlobsRequest{
				Repository: singleBlobRepo,
				BytesLimit: 1,
			},
			verify: func(t *testing.T, blobs []*gitalypb.ListAllBlobsResponse_Blob) {
				require.Equal(t, []*gitalypb.ListAllBlobsResponse_Blob{{
					Oid:  blobID.String(),
					Size: 6,
					Data: []byte("f"),
				}}, blobs)
			},
		},
		{
			desc: "normal repo",
			request: &gitalypb.ListAllBlobsRequest{
				Repository: repo,
			},
			verify: func(t *testing.T, blobs []*gitalypb.ListAllBlobsResponse_Blob) {
				require.Len(t, blobs, 6)
			},
		},
		{
			desc: "normal repo with limit",
			request: &gitalypb.ListAllBlobsRequest{
				Repository: repo,
				Limit:      2,
			},
			verify: func(t *testing.T, blobs []*gitalypb.ListAllBlobsResponse_Blob) {
				require.Len(t, blobs, 2)
			},
		},
		{
			desc: "normal repo with bytes limit",
			request: &gitalypb.ListAllBlobsRequest{
				Repository: repo,
				BytesLimit: 1,
			},
			verify: func(t *testing.T, blobs []*gitalypb.ListAllBlobsResponse_Blob) {
				require.Equal(t, len(blobs), 6)

				for _, blob := range blobs {
					emptyBlobID := "e69de29bb2d1d6434b8b29ae775ad8c2e48c5391"
					if blob.GetOid() == emptyBlobID {
						require.Empty(t, blob.GetData())
						require.Equal(t, int64(0), blob.GetSize())
					} else {
						require.Len(t, blob.GetData(), 1)
					}
				}
			},
		},
		{
			desc: "quarantine repo with alternates",
			request: &gitalypb.ListAllBlobsRequest{
				Repository: quarantinedRepo,
			},
			verify: func(t *testing.T, blobs []*gitalypb.ListAllBlobsResponse_Blob) {
				require.Len(t, blobs, 6)
			},
		},
		{
			desc: "quarantine repo without alternates",
			request: &gitalypb.ListAllBlobsRequest{
				Repository: quarantineRepoWithoutAlternates,
			},
			verify: func(t *testing.T, blobs []*gitalypb.ListAllBlobsResponse_Blob) {
				require.Empty(t, blobs)
			},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			ctx := ctx
			if tc.request.GetRepository().GetGitObjectDirectory() != "" {
				// Rails sends the repository's relative path from the access checks as provided by Gitaly. If transactions are enabled,
				// this is the snapshot's relative path. Include the metadata in the test as well as we're testing requests with quarantine
				// as if they were coming from access checks.
				ctx = metadata.AppendToOutgoingContext(ctx, storagemgr.MetadataKeySnapshotRelativePath,
					// Gitaly sends the snapshot's relative path to Rails from `pre-receive` and Rails
					// sends it back to Gitaly when it performs requests in the access checks. The repository
					// would have already been rewritten by Praefect, so we have to adjust for that as well.
					gittest.RewrittenRepository(t, ctx, cfg, tc.request.GetRepository()).GetRelativePath(),
				)
			}

			stream, err := client.ListAllBlobs(ctx, tc.request)
			require.NoError(t, err)

			var blobs []*gitalypb.ListAllBlobsResponse_Blob
			for {
				resp, err := stream.Recv()
				if err != nil {
					if !errors.Is(err, io.EOF) {
						require.NoError(t, err)
					}
					break
				}

				blobs = append(blobs, resp.GetBlobs()...)
			}

			tc.verify(t, blobs)
		})
	}
}

func BenchmarkListAllBlobs(b *testing.B) {
	b.StopTimer()
	ctx := testhelper.Context(b)

	cfg, client := setup(b, ctx)
	repoProto, _, _ := setupRepoWithLFS(b, ctx, cfg)

	for _, tc := range []struct {
		desc    string
		request *gitalypb.ListAllBlobsRequest
	}{
		{
			desc: "with contents",
			request: &gitalypb.ListAllBlobsRequest{
				Repository: repoProto,
				BytesLimit: -1,
			},
		},
		{
			desc: "without contents",
			request: &gitalypb.ListAllBlobsRequest{
				Repository: repoProto,
				BytesLimit: 0,
			},
		},
	} {
		b.Run(tc.desc, func(b *testing.B) {
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				stream, err := client.ListAllBlobs(ctx, tc.request)
				require.NoError(b, err)

				for {
					_, err := stream.Recv()
					if errors.Is(err, io.EOF) {
						break
					}
					require.NoError(b, err)
				}
			}
		})
	}
}

func BenchmarkListBlobs(b *testing.B) {
	b.StopTimer()
	ctx := testhelper.Context(b)

	cfg, client := setup(b, ctx)
	repoProto, _, _ := setupRepoWithLFS(b, ctx, cfg)

	for _, tc := range []struct {
		desc    string
		request *gitalypb.ListBlobsRequest
	}{
		{
			desc: "with contents",
			request: &gitalypb.ListBlobsRequest{
				Repository: repoProto,
				Revisions:  []string{"refs/heads/master"},
				BytesLimit: -1,
			},
		},
		{
			desc: "without contents",
			request: &gitalypb.ListBlobsRequest{
				Repository: repoProto,
				Revisions:  []string{"refs/heads/master"},
				BytesLimit: 0,
			},
		},
	} {
		b.Run(tc.desc, func(b *testing.B) {
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				stream, err := client.ListBlobs(ctx, tc.request)
				require.NoError(b, err)

				for {
					_, err := stream.Recv()
					if errors.Is(err, io.EOF) {
						break
					}
					require.NoError(b, err)
				}
			}
		})
	}
}
