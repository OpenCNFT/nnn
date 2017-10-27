package repository

import (
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"testing"

	"google.golang.org/grpc/codes"

	"github.com/stretchr/testify/assert"
	pb "gitlab.com/gitlab-org/gitaly-proto/go"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
)

func TestApplyGitattributesSuccess(t *testing.T) {
	server, serverSocketPath := runRepoServer(t)
	defer server.Stop()

	client, conn := newRepositoryClient(t, serverSocketPath)
	defer conn.Close()

	infoPath := path.Join(testhelper.GitlabTestStoragePath(),
		testRepo.GetRelativePath(), "info")
	attributesPath := path.Join(infoPath, "attributes")

	tests := []struct {
		desc     string
		revision []byte
		contents []byte
	}{
		{
			desc:     "With a .gitattributes file",
			revision: []byte("e63f41fe459e62e1228fcef60d7189127aeba95a"),
			contents: []byte("/custom-highlighting/*.gitlab-custom gitlab-language=ruby\n"),
		},
		{
			desc:     "Without a .gitattributes file",
			revision: []byte("7efb185dd22fd5c51ef044795d62b7847900c341"),
			contents: nil,
		},
	}

	for _, test := range tests {
		t.Run(test.desc, func(t *testing.T) {
			// Test when no /info folder exists
			if err := os.RemoveAll(infoPath); err != nil {
				t.Fatal(err)
			}
			assertGitattributesApplied(t, client, attributesPath, test.revision, test.contents)

			// Test when no git attributes file exists
			if err := os.Remove(attributesPath); err != nil && !os.IsNotExist(err) {
				t.Fatal(err)
			}
			assertGitattributesApplied(t, client, attributesPath, test.revision, test.contents)

			// Test when a git attributes file already exists
			ioutil.WriteFile(attributesPath, []byte("*.docx diff=word"), 0644)
			assertGitattributesApplied(t, client, attributesPath, test.revision, test.contents)
		})
	}
}

func TestApplyGitattributesFailure(t *testing.T) {
	server, serverSocketPath := runRepoServer(t)
	defer server.Stop()

	client, conn := newRepositoryClient(t, serverSocketPath)
	defer conn.Close()

	tests := []struct {
		repo     *pb.Repository
		revision []byte
		code     codes.Code
	}{
		{
			repo:     nil,
			revision: nil,
			code:     codes.InvalidArgument,
		},
		{
			repo:     &pb.Repository{StorageName: "foo"},
			revision: []byte("master"),
			code:     codes.InvalidArgument,
		},
		{
			repo:     &pb.Repository{RelativePath: "bar"},
			revision: []byte("master"),
			code:     codes.InvalidArgument,
		},
		{
			repo:     &pb.Repository{StorageName: testRepo.GetStorageName(), RelativePath: "bar"},
			revision: []byte("master"),
			code:     codes.NotFound,
		},
		{
			repo:     testRepo,
			revision: []byte(""),
			code:     codes.InvalidArgument,
		},
		{
			repo:     testRepo,
			revision: []byte("not-existing-ref"),
			code:     codes.InvalidArgument,
		},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("%+v", test), func(t *testing.T) {
			ctx, cancel := testhelper.Context()
			defer cancel()

			req := &pb.ApplyGitattributesRequest{Repository: test.repo, Revision: test.revision}
			_, err := client.ApplyGitattributes(ctx, req)
			testhelper.AssertGrpcError(t, err, test.code, "")
		})
	}
}

func assertGitattributesApplied(t *testing.T, client pb.RepositoryServiceClient, attributesPath string, revision, expectedContents []byte) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	req := &pb.ApplyGitattributesRequest{Repository: testRepo, Revision: revision}
	c, err := client.ApplyGitattributes(ctx, req)

	assert.NoError(t, err)
	assert.NotNil(t, c)

	contents, err := ioutil.ReadFile(attributesPath)
	if expectedContents == nil {
		if !os.IsNotExist(err) {
			t.Error(err)
		}
	} else {
		if err != nil {
			t.Error(err)
		}

		assert.Equal(t, expectedContents, contents)
	}
}
