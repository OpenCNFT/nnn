package conflicts_test

import (
	"bytes"
	"context"
	"encoding/json"
	"os/exec"
	"strings"
	"testing"

	"github.com/golang/protobuf/ptypes/timestamp"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/rubyserver"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/service/conflicts"
	"gitlab.com/gitlab-org/gitaly/internal/metadata/featureflag"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
)

var (
	user = &gitalypb.User{
		Name:  []byte("John Doe"),
		Email: []byte("johndoe@gitlab.com"),
		GlId:  "user-1",
	}
	conflictResolutionCommitMessage = "Solve conflicts"

	files = []map[string]interface{}{
		{
			"old_path": "files/ruby/popen.rb",
			"new_path": "files/ruby/popen.rb",
			"sections": map[string]string{
				"2f6fcd96b88b36ce98c38da085c795a27d92a3dd_14_14": "head",
			},
		},
		{
			"old_path": "files/ruby/regex.rb",
			"new_path": "files/ruby/regex.rb",
			"sections": map[string]string{
				"6eb14e00385d2fb284765eb1cd8d420d33d63fc9_9_9":   "head",
				"6eb14e00385d2fb284765eb1cd8d420d33d63fc9_21_21": "origin",
				"6eb14e00385d2fb284765eb1cd8d420d33d63fc9_49_49": "origin",
			},
		},
	}
)

func TestWithRubyServer(t *testing.T) {
	cfg := testcfg.Build(t)

	rubySrv := rubyserver.New(cfg)
	require.NoError(t, rubySrv.Start())
	t.Cleanup(rubySrv.Stop)

	t.Run("testSuccessfulResolveConflictsRequest", func(t *testing.T) { testSuccessfulResolveConflictsRequest(t, cfg, rubySrv) })
	t.Run("testResolveConflictsStableID", func(t *testing.T) { testResolveConflictsStableID(t, cfg, rubySrv) })
	t.Run("testFailedResolveConflictsRequestDueToResolutionError", func(t *testing.T) { testFailedResolveConflictsRequestDueToResolutionError(t, cfg, rubySrv) })
	t.Run("testFailedResolveConflictsRequestDueToValidation", func(t *testing.T) { testFailedResolveConflictsRequestDueToValidation(t, cfg, rubySrv) })
}

func testSuccessfulResolveConflictsRequest(t *testing.T, cfg config.Cfg, rubySrv *rubyserver.Server) {
	testhelper.NewFeatureSets([]featureflag.FeatureFlag{
		featureflag.GoResolveConflicts,
	}).Run(t, func(t *testing.T, ctx context.Context) {
		testSuccessfulResolveConflictsRequestFeatured(t, ctx, cfg, rubySrv)
	})
}

func testSuccessfulResolveConflictsRequestFeatured(t *testing.T, ctx context.Context, cfg config.Cfg, rubySrv *rubyserver.Server) {
	cfg, repoProto, repoPath, client := conflicts.SetupConflictsServiceWithRuby(t, cfg, rubySrv, true)

	repo := localrepo.New(git.NewExecCommandFactory(cfg), repoProto, cfg)

	mdGS := testhelper.GitalyServersMetadata(t, cfg.SocketPath)
	mdFF, _ := metadata.FromOutgoingContext(ctx)
	ctx = metadata.NewOutgoingContext(ctx, metadata.Join(mdGS, mdFF))

	missingAncestorPath := "files/missing_ancestor.txt"
	files := []map[string]interface{}{
		{
			"old_path": "files/ruby/popen.rb",
			"new_path": "files/ruby/popen.rb",
			"sections": map[string]string{
				"2f6fcd96b88b36ce98c38da085c795a27d92a3dd_14_14": "head",
			},
		},
		{
			"old_path": "files/ruby/regex.rb",
			"new_path": "files/ruby/regex.rb",
			"sections": map[string]string{
				"6eb14e00385d2fb284765eb1cd8d420d33d63fc9_9_9":   "head",
				"6eb14e00385d2fb284765eb1cd8d420d33d63fc9_21_21": "origin",
				"6eb14e00385d2fb284765eb1cd8d420d33d63fc9_49_49": "origin",
			},
		},
		{
			"old_path": missingAncestorPath,
			"new_path": missingAncestorPath,
			"sections": map[string]string{
				"b760bfd3b1b1da380b4276eb30fb3b2b7e4f08e1_1_1": "origin",
			},
		},
	}

	filesJSON, err := json.Marshal(files)
	require.NoError(t, err)

	sourceBranch := "conflict-resolvable"
	targetBranch := "conflict-start"
	ourCommitOID := "1450cd639e0bc6721eb02800169e464f212cde06"   // part of branch conflict-resolvable
	theirCommitOID := "824be604a34828eb682305f0d963056cfac87b2d" // part of branch conflict-start
	ancestorCommitOID := "6907208d755b60ebeacb2e9dfea74c92c3449a1f"

	// introduce a conflict that exists on both branches, but not the
	// ancestor
	commitConflict := func(parentCommitID, branch, blob string) string {
		blobID, err := repo.WriteBlob(ctx, "", strings.NewReader(blob))
		require.NoError(t, err)
		testhelper.MustRunCommand(t, nil, "git", "-C", repoPath, "read-tree", branch)
		testhelper.MustRunCommand(t, nil,
			"git", "-C", repoPath,
			"update-index", "--add", "--cacheinfo", "100644", blobID.String(), missingAncestorPath,
		)
		treeID := bytes.TrimSpace(
			testhelper.MustRunCommand(t, nil,
				"git", "-C", repoPath, "write-tree",
			),
		)
		commitID := bytes.TrimSpace(
			testhelper.MustRunCommand(t, nil,
				"git", "-C", repoPath,
				"commit-tree", string(treeID), "-p", parentCommitID,
			),
		)
		testhelper.MustRunCommand(t, nil,
			"git", "-C", repoPath, "update-ref", "refs/heads/"+branch, string(commitID))
		return string(commitID)
	}

	// sanity check: make sure the conflict file does not exist on the
	// common ancestor
	cmd := exec.CommandContext(ctx, "git", "cat-file", "-e", ancestorCommitOID+":"+missingAncestorPath)
	require.Error(t, cmd.Run())

	ourCommitOID = commitConflict(ourCommitOID, sourceBranch, "content-1")
	theirCommitOID = commitConflict(theirCommitOID, targetBranch, "content-2")

	headerRequest := &gitalypb.ResolveConflictsRequest{
		ResolveConflictsRequestPayload: &gitalypb.ResolveConflictsRequest_Header{
			Header: &gitalypb.ResolveConflictsRequestHeader{
				Repository:       repoProto,
				TargetRepository: repoProto,
				CommitMessage:    []byte(conflictResolutionCommitMessage),
				OurCommitOid:     ourCommitOID,
				TheirCommitOid:   theirCommitOID,
				SourceBranch:     []byte(sourceBranch),
				TargetBranch:     []byte(targetBranch),
				User:             user,
			},
		},
	}
	filesRequest1 := &gitalypb.ResolveConflictsRequest{
		ResolveConflictsRequestPayload: &gitalypb.ResolveConflictsRequest_FilesJson{
			FilesJson: filesJSON[:50],
		},
	}
	filesRequest2 := &gitalypb.ResolveConflictsRequest{
		ResolveConflictsRequestPayload: &gitalypb.ResolveConflictsRequest_FilesJson{
			FilesJson: filesJSON[50:],
		},
	}

	stream, err := client.ResolveConflicts(ctx)
	require.NoError(t, err)
	require.NoError(t, stream.Send(headerRequest))
	require.NoError(t, stream.Send(filesRequest1))
	require.NoError(t, stream.Send(filesRequest2))

	r, err := stream.CloseAndRecv()
	require.NoError(t, err)
	require.Empty(t, r.GetResolutionError())

	headCommit, err := repo.ReadCommit(ctx, git.Revision(sourceBranch))
	require.NoError(t, err)
	require.Contains(t, headCommit.ParentIds, ourCommitOID)
	require.Contains(t, headCommit.ParentIds, theirCommitOID)
	require.Equal(t, string(headCommit.Author.Email), "johndoe@gitlab.com")
	require.Equal(t, string(headCommit.Committer.Email), "johndoe@gitlab.com")
	require.Equal(t, string(headCommit.Subject), conflictResolutionCommitMessage)
}

func testResolveConflictsStableID(t *testing.T, cfg config.Cfg, rubySrv *rubyserver.Server) {
	testhelper.ConfigureGitalyHooksBin(t, cfg)

	testhelper.NewFeatureSets([]featureflag.FeatureFlag{
		featureflag.GoResolveConflicts,
	}).Run(t, func(t *testing.T, ctx context.Context) {
		testResolveConflictsStableIDFeatured(t, ctx, cfg, rubySrv)
	})
}

func testResolveConflictsStableIDFeatured(t *testing.T, ctx context.Context, cfg config.Cfg, rubySrv *rubyserver.Server) {
	cfg, repoProto, _, client := conflicts.SetupConflictsServiceWithRuby(t, cfg, rubySrv, true)

	repo := localrepo.New(git.NewExecCommandFactory(cfg), repoProto, cfg)

	md := testhelper.GitalyServersMetadata(t, cfg.SocketPath)
	ctx = testhelper.MergeOutgoingMetadata(ctx, md)

	stream, err := client.ResolveConflicts(ctx)
	require.NoError(t, err)

	require.NoError(t, stream.Send(&gitalypb.ResolveConflictsRequest{
		ResolveConflictsRequestPayload: &gitalypb.ResolveConflictsRequest_Header{
			Header: &gitalypb.ResolveConflictsRequestHeader{
				Repository:       repoProto,
				TargetRepository: repoProto,
				CommitMessage:    []byte(conflictResolutionCommitMessage),
				OurCommitOid:     "1450cd639e0bc6721eb02800169e464f212cde06",
				TheirCommitOid:   "824be604a34828eb682305f0d963056cfac87b2d",
				SourceBranch:     []byte("conflict-resolvable"),
				TargetBranch:     []byte("conflict-start"),
				User:             user,
				Timestamp:        &timestamp.Timestamp{Seconds: 12345},
			},
		},
	}))

	filesJSON, err := json.Marshal(files)
	require.NoError(t, err)
	require.NoError(t, stream.Send(&gitalypb.ResolveConflictsRequest{
		ResolveConflictsRequestPayload: &gitalypb.ResolveConflictsRequest_FilesJson{
			FilesJson: filesJSON,
		},
	}))

	response, err := stream.CloseAndRecv()
	require.NoError(t, err)
	require.Empty(t, response.GetResolutionError())

	resolvedCommit, err := repo.ReadCommit(ctx, git.Revision("conflict-resolvable"))
	require.NoError(t, err)
	require.Equal(t, &gitalypb.GitCommit{
		Id:     "a5ad028fd739d7a054b07c293e77c5b7aecc2435",
		TreeId: "febd97e4a09e71355a513d7e0b0b3808e2dabd28",
		ParentIds: []string{
			"1450cd639e0bc6721eb02800169e464f212cde06",
			"824be604a34828eb682305f0d963056cfac87b2d",
		},
		Subject:  []byte(conflictResolutionCommitMessage),
		Body:     []byte(conflictResolutionCommitMessage),
		BodySize: 15,
		Author: &gitalypb.CommitAuthor{
			Name:     user.Name,
			Email:    user.Email,
			Date:     &timestamp.Timestamp{Seconds: 12345},
			Timezone: []byte("+0000"),
		},
		Committer: &gitalypb.CommitAuthor{
			Name:     user.Name,
			Email:    user.Email,
			Date:     &timestamp.Timestamp{Seconds: 12345},
			Timezone: []byte("+0000"),
		},
	}, resolvedCommit)
}

func testFailedResolveConflictsRequestDueToResolutionError(t *testing.T, cfg config.Cfg, rubySrv *rubyserver.Server) {
	testhelper.NewFeatureSets([]featureflag.FeatureFlag{
		featureflag.GoResolveConflicts,
	}).Run(t, func(t *testing.T, ctx context.Context) {
		testFailedResolveConflictsRequestDueToResolutionErrorFeatured(t, ctx, cfg, rubySrv)
	})
}

func testFailedResolveConflictsRequestDueToResolutionErrorFeatured(t *testing.T, ctx context.Context, cfg config.Cfg, rubySrv *rubyserver.Server) {
	cfg, repo, _, client := conflicts.SetupConflictsServiceWithRuby(t, cfg, rubySrv, true)

	mdGS := testhelper.GitalyServersMetadata(t, cfg.SocketPath)
	mdFF, _ := metadata.FromOutgoingContext(ctx)
	ctx = metadata.NewOutgoingContext(ctx, metadata.Join(mdGS, mdFF))

	files := []map[string]interface{}{
		{
			"old_path": "files/ruby/popen.rb",
			"new_path": "files/ruby/popen.rb",
			"content":  "",
		},
		{
			"old_path": "files/ruby/regex.rb",
			"new_path": "files/ruby/regex.rb",
			"sections": map[string]string{
				"6eb14e00385d2fb284765eb1cd8d420d33d63fc9_9_9": "head",
			},
		},
	}
	filesJSON, err := json.Marshal(files)
	require.NoError(t, err)

	headerRequest := &gitalypb.ResolveConflictsRequest{
		ResolveConflictsRequestPayload: &gitalypb.ResolveConflictsRequest_Header{
			Header: &gitalypb.ResolveConflictsRequestHeader{
				Repository:       repo,
				TargetRepository: repo,
				CommitMessage:    []byte(conflictResolutionCommitMessage),
				OurCommitOid:     "1450cd639e0bc6721eb02800169e464f212cde06",
				TheirCommitOid:   "824be604a34828eb682305f0d963056cfac87b2d",
				SourceBranch:     []byte("conflict-resolvable"),
				TargetBranch:     []byte("conflict-start"),
				User:             user,
			},
		},
	}
	filesRequest := &gitalypb.ResolveConflictsRequest{
		ResolveConflictsRequestPayload: &gitalypb.ResolveConflictsRequest_FilesJson{
			FilesJson: filesJSON,
		},
	}

	stream, err := client.ResolveConflicts(ctx)
	require.NoError(t, err)
	require.NoError(t, stream.Send(headerRequest))
	require.NoError(t, stream.Send(filesRequest))

	r, err := stream.CloseAndRecv()
	require.NoError(t, err)
	require.Equal(t, r.GetResolutionError(), "Missing resolution for section ID: 6eb14e00385d2fb284765eb1cd8d420d33d63fc9_21_21")
}

func testFailedResolveConflictsRequestDueToValidation(t *testing.T, cfg config.Cfg, rubySrv *rubyserver.Server) {
	testhelper.NewFeatureSets([]featureflag.FeatureFlag{
		featureflag.GoResolveConflicts,
	}).Run(t, func(t *testing.T, ctx context.Context) {
		testFailedResolveConflictsRequestDueToValidationFeatured(t, ctx, cfg, rubySrv)
	})
}

func testFailedResolveConflictsRequestDueToValidationFeatured(t *testing.T, ctx context.Context, cfg config.Cfg, rubySrv *rubyserver.Server) {
	cfg, repo, _, client := conflicts.SetupConflictsServiceWithRuby(t, cfg, rubySrv, true)

	mdGS := testhelper.GitalyServersMetadata(t, cfg.SocketPath)
	ourCommitOid := "1450cd639e0bc6721eb02800169e464f212cde06"
	theirCommitOid := "824be604a34828eb682305f0d963056cfac87b2d"
	commitMsg := []byte(conflictResolutionCommitMessage)
	sourceBranch := []byte("conflict-resolvable")
	targetBranch := []byte("conflict-start")

	testCases := []struct {
		desc         string
		header       *gitalypb.ResolveConflictsRequestHeader
		expectedCode codes.Code
		expectedErr  string
	}{
		{
			desc: "empty user",
			header: &gitalypb.ResolveConflictsRequestHeader{
				User:             nil,
				Repository:       repo,
				OurCommitOid:     ourCommitOid,
				TargetRepository: repo,
				TheirCommitOid:   theirCommitOid,
				CommitMessage:    commitMsg,
				SourceBranch:     sourceBranch,
				TargetBranch:     targetBranch,
			},
			expectedCode: codes.InvalidArgument,
			expectedErr:  "ResolveConflicts: empty User",
		},
		{
			desc: "empty repo",
			header: &gitalypb.ResolveConflictsRequestHeader{
				User:             user,
				Repository:       nil,
				OurCommitOid:     ourCommitOid,
				TargetRepository: repo,
				TheirCommitOid:   theirCommitOid,
				CommitMessage:    commitMsg,
				SourceBranch:     sourceBranch,
				TargetBranch:     targetBranch,
			},
			expectedCode: codes.InvalidArgument,
			// Praefect checks for an empty repository, too, but will raise a different
			// error message. Luckily, both Gitaly's and Praefect's error messages
			// contain "empty Repository".
			expectedErr: "empty Repository",
		},
		{
			desc: "empty target repo",
			header: &gitalypb.ResolveConflictsRequestHeader{
				User:             user,
				Repository:       repo,
				OurCommitOid:     ourCommitOid,
				TargetRepository: nil,
				TheirCommitOid:   theirCommitOid,
				CommitMessage:    commitMsg,
				SourceBranch:     sourceBranch,
				TargetBranch:     targetBranch,
			},
			expectedCode: codes.InvalidArgument,
			expectedErr:  "ResolveConflicts: empty TargetRepository",
		},
		{
			desc: "empty OurCommitId repo",
			header: &gitalypb.ResolveConflictsRequestHeader{
				User:             user,
				Repository:       repo,
				OurCommitOid:     "",
				TargetRepository: repo,
				TheirCommitOid:   theirCommitOid,
				CommitMessage:    commitMsg,
				SourceBranch:     sourceBranch,
				TargetBranch:     targetBranch,
			},
			expectedCode: codes.InvalidArgument,
			expectedErr:  "ResolveConflicts: empty OurCommitOid",
		},
		{
			desc: "empty TheirCommitId repo",
			header: &gitalypb.ResolveConflictsRequestHeader{
				User:             user,
				Repository:       repo,
				OurCommitOid:     ourCommitOid,
				TargetRepository: repo,
				TheirCommitOid:   "",
				CommitMessage:    commitMsg,
				SourceBranch:     sourceBranch,
				TargetBranch:     targetBranch,
			},
			expectedCode: codes.InvalidArgument,
			expectedErr:  "ResolveConflicts: empty TheirCommitOid",
		},
		{
			desc: "empty CommitMessage repo",
			header: &gitalypb.ResolveConflictsRequestHeader{
				User:             user,
				Repository:       repo,
				OurCommitOid:     ourCommitOid,
				TargetRepository: repo,
				TheirCommitOid:   theirCommitOid,
				CommitMessage:    nil,
				SourceBranch:     sourceBranch,
				TargetBranch:     targetBranch,
			},
			expectedCode: codes.InvalidArgument,
			expectedErr:  "ResolveConflicts: empty CommitMessage",
		},
		{
			desc: "empty SourceBranch repo",
			header: &gitalypb.ResolveConflictsRequestHeader{
				User:             user,
				Repository:       repo,
				OurCommitOid:     ourCommitOid,
				TargetRepository: repo,
				TheirCommitOid:   theirCommitOid,
				CommitMessage:    commitMsg,
				SourceBranch:     nil,
				TargetBranch:     targetBranch,
			},
			expectedCode: codes.InvalidArgument,
			expectedErr:  "ResolveConflicts: empty SourceBranch",
		},
		{
			desc: "empty TargetBranch repo",
			header: &gitalypb.ResolveConflictsRequestHeader{
				User:             user,
				Repository:       repo,
				OurCommitOid:     ourCommitOid,
				TargetRepository: repo,
				TheirCommitOid:   theirCommitOid,
				CommitMessage:    commitMsg,
				SourceBranch:     sourceBranch,
				TargetBranch:     nil,
			},
			expectedCode: codes.InvalidArgument,
			expectedErr:  "ResolveConflicts: empty TargetBranch",
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.desc, func(t *testing.T) {
			mdFF, _ := metadata.FromOutgoingContext(ctx)
			ctx = metadata.NewOutgoingContext(ctx, metadata.Join(mdGS, mdFF))

			stream, err := client.ResolveConflicts(ctx)
			require.NoError(t, err)

			headerRequest := &gitalypb.ResolveConflictsRequest{
				ResolveConflictsRequestPayload: &gitalypb.ResolveConflictsRequest_Header{
					Header: testCase.header,
				},
			}
			require.NoError(t, stream.Send(headerRequest))

			_, err = stream.CloseAndRecv()
			testhelper.RequireGrpcError(t, err, testCase.expectedCode)
			require.Contains(t, err.Error(), testCase.expectedErr)
		})
	}
}
