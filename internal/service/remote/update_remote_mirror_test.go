package remote

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly-proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"golang.org/x/net/context"
	"google.golang.org/grpc/codes"
)

func TestSuccessfulUpdateRemoteMirrorRequest(t *testing.T) {
	server, serverSocketPath := runRemoteServiceServer(t)
	defer server.Stop()

	client, conn := NewRemoteClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, testRepoPath, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	_, mirrorPath, mirrorCleanupFn := testhelper.NewTestRepo(t)
	defer mirrorCleanupFn()

	remoteName := "remote_mirror_1"

	testhelper.MustRunCommand(t, nil, "git", "-C", mirrorPath, "tag", "v0.0.1", "master") // I needed another tag for the tests

	setupCommands := [][]string{
		// Preconditions
		{"config", "user.email", "gitalytest@example.com"},
		{"remote", "add", remoteName, mirrorPath},
		{"fetch", remoteName},
		// Updates
		{"branch", "new-branch", "60ecb67744cb56576c30214ff52294f8ce2def98"},                  // Add branch
		{"branch", "ignored-branch", "60ecb67744cb56576c30214ff52294f8ce2def98"},              // Add branch not matching branch list
		{"update-ref", "refs/heads/empty-branch", "0b4bc9a49b562e85de7cc9e834518ea6828729b9"}, // Update branch
		{"branch", "-D", "not-merged-branch"},                                                 // Delete branch
		// Scoped to the project, so will be removed after
		{"tag", "new-tag", "60ecb67744cb56576c30214ff52294f8ce2def98"},                          // Add tag
		{"tag", "-fam", "Overriding tag", "v1.0.0", "0b4bc9a49b562e85de7cc9e834518ea6828729b9"}, // Update tag
		{"tag", "-d", "v0.0.1"}, // Delete tag
	}

	for _, args := range setupCommands {
		gitArgs := []string{"-C", testRepoPath}
		gitArgs = append(gitArgs, args...)
		testhelper.MustRunCommand(t, nil, "git", gitArgs...)
	}

	newTagOid := string(testhelper.MustRunCommand(t, nil, "git", "-C", testRepoPath, "rev-parse", "v1.0.0"))
	newTagOid = strings.TrimSpace(newTagOid)
	require.NotEqual(t, newTagOid, "f4e6814c3e4e7a0de82a9e7cd20c626cc963a2f8") // Sanity check that the tag did in fact change

	ctx, cancel := testhelper.Context()
	defer cancel()

	firstRequest := &gitalypb.UpdateRemoteMirrorRequest{
		Repository:           testRepo,
		RefName:              remoteName,
		OnlyBranchesMatching: nil,
	}
	matchingRequest1 := &gitalypb.UpdateRemoteMirrorRequest{
		OnlyBranchesMatching: [][]byte{[]byte("new-branch"), []byte("empty-branch")},
	}
	matchingRequest2 := &gitalypb.UpdateRemoteMirrorRequest{
		OnlyBranchesMatching: [][]byte{[]byte("not-merged-branch"), []byte("matcher-without-matches")},
	}

	stream, err := client.UpdateRemoteMirror(ctx)
	require.NoError(t, err)
	require.NoError(t, stream.Send(firstRequest))
	require.NoError(t, stream.Send(matchingRequest1))
	require.NoError(t, stream.Send(matchingRequest2))

	_, err = stream.CloseAndRecv()
	require.NoError(t, err)

	mirrorRefs := string(testhelper.MustRunCommand(t, nil, "git", "-C", mirrorPath, "for-each-ref"))

	require.Contains(t, mirrorRefs, "60ecb67744cb56576c30214ff52294f8ce2def98 commit\trefs/heads/new-branch")
	require.NotContains(t, mirrorRefs, "refs/heads/ignored-branch")
	require.Contains(t, mirrorRefs, "0b4bc9a49b562e85de7cc9e834518ea6828729b9 commit\trefs/heads/empty-branch")
	require.NotContains(t, mirrorRefs, "refs/heads/not-merged-branch")
	require.Contains(t, mirrorRefs, "60ecb67744cb56576c30214ff52294f8ce2def98 commit\trefs/tags/new-tag")
	require.Contains(t, mirrorRefs, newTagOid+" tag\trefs/tags/v1.0.0")
	require.NotContains(t, mirrorRefs, "refs/tags/v0.0.1")
}

func TestSuccessfulUpdateRemoteMirrorRequestWithWildcards(t *testing.T) {
	server, serverSocketPath := runRemoteServiceServer(t)
	defer server.Stop()

	client, conn := NewRemoteClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, testRepoPath, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	_, mirrorPath, mirrorCleanupFn := testhelper.NewTestRepo(t)
	defer mirrorCleanupFn()

	remoteName := "remote_mirror_2"

	setupCommands := [][]string{
		// Preconditions
		{"config", "user.email", "gitalytest@example.com"},
		{"remote", "add", remoteName, mirrorPath},
		{"fetch", remoteName},
		// Updates
		{"branch", "11-0-stable", "60ecb67744cb56576c30214ff52294f8ce2def98"},
		{"branch", "11-1-stable", "60ecb67744cb56576c30214ff52294f8ce2def98"},                // Add branch
		{"branch", "ignored-branch", "60ecb67744cb56576c30214ff52294f8ce2def98"},             // Add branch not matching branch list
		{"update-ref", "refs/heads/some-branch", "0b4bc9a49b562e85de7cc9e834518ea6828729b9"}, // Update branch
		{"update-ref", "refs/heads/feature", "0b4bc9a49b562e85de7cc9e834518ea6828729b9"},     // Update branch
		// Scoped to the project, so will be removed after
		{"branch", "-D", "not-merged-branch"},                                                   // Delete branch
		{"tag", "new-tag", "60ecb67744cb56576c30214ff52294f8ce2def98"},                          // Add tag
		{"tag", "-fam", "Overriding tag", "v1.0.0", "0b4bc9a49b562e85de7cc9e834518ea6828729b9"}, // Update tag
	}

	for _, args := range setupCommands {
		gitArgs := []string{"-C", testRepoPath}
		gitArgs = append(gitArgs, args...)
		testhelper.MustRunCommand(t, nil, "git", gitArgs...)
	}

	// Workaround for https://gitlab.com/gitlab-org/gitaly/issues/1439
	// Create a tag on the remote to ensure it gets deleted later
	testhelper.MustRunCommand(t, nil, "git", "-C", mirrorPath, "tag", "v1.2.0", "master")

	newTagOid := string(testhelper.MustRunCommand(t, nil, "git", "-C", testRepoPath, "rev-parse", "v1.0.0"))
	newTagOid = strings.TrimSpace(newTagOid)
	require.NotEqual(t, newTagOid, "f4e6814c3e4e7a0de82a9e7cd20c626cc963a2f8") // Sanity check that the tag did in fact change

	ctx, cancel := testhelper.Context()
	defer cancel()

	firstRequest := &gitalypb.UpdateRemoteMirrorRequest{
		Repository:           testRepo,
		RefName:              remoteName,
		OnlyBranchesMatching: [][]byte{[]byte("*-stable"), []byte("feature")},
	}

	stream, err := client.UpdateRemoteMirror(ctx)
	require.NoError(t, err)
	require.NoError(t, stream.Send(firstRequest))

	_, err = stream.CloseAndRecv()
	require.NoError(t, err)

	mirrorRefs := string(testhelper.MustRunCommand(t, nil, "git", "-C", mirrorPath, "for-each-ref"))

	require.Contains(t, mirrorRefs, "60ecb67744cb56576c30214ff52294f8ce2def98 commit\trefs/heads/11-0-stable")
	require.Contains(t, mirrorRefs, "60ecb67744cb56576c30214ff52294f8ce2def98 commit\trefs/heads/11-1-stable")
	require.Contains(t, mirrorRefs, "0b4bc9a49b562e85de7cc9e834518ea6828729b9 commit\trefs/heads/feature")
	require.NotContains(t, mirrorRefs, "refs/heads/ignored-branch")
	require.NotContains(t, mirrorRefs, "refs/heads/some-branch")
	require.Contains(t, mirrorRefs, "refs/heads/not-merged-branch")
	require.Contains(t, mirrorRefs, "60ecb67744cb56576c30214ff52294f8ce2def98 commit\trefs/tags/new-tag")
	require.Contains(t, mirrorRefs, newTagOid+" tag\trefs/tags/v1.0.0")
	require.NotContains(t, mirrorRefs, "refs/tags/v1.2.0")
}

func TestFailedUpdateRemoteMirrorRequestDueToValidation(t *testing.T) {
	server, serverSocketPath := runRemoteServiceServer(t)
	defer server.Stop()

	client, conn := NewRemoteClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, _, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	testCases := []struct {
		desc    string
		request *gitalypb.UpdateRemoteMirrorRequest
	}{
		{
			desc: "empty Repository",
			request: &gitalypb.UpdateRemoteMirrorRequest{
				Repository: nil,
				RefName:    "remote_mirror_1",
			},
		},
		{
			desc: "empty RefName",
			request: &gitalypb.UpdateRemoteMirrorRequest{
				Repository: testRepo,
				RefName:    "",
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			stream, err := client.UpdateRemoteMirror(ctx)
			require.NoError(t, err)
			require.NoError(t, stream.Send(tc.request))

			_, err = stream.CloseAndRecv()
			testhelper.RequireGrpcError(t, err, codes.InvalidArgument)
			require.Contains(t, err.Error(), tc.desc)
		})
	}
}
