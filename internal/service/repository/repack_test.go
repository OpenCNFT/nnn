package repository

import (
	"context"
	"path"
	"path/filepath"
	"testing"
	"time"

	"google.golang.org/grpc/codes"

	"github.com/stretchr/testify/assert"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"

	pb "gitlab.com/gitlab-org/gitaly-proto/go"
)

func TestRepackIncrementalSuccess(t *testing.T) {
	server := runRepoServer(t)
	defer server.Stop()

	client := newRepositoryClient(t)

	packPath := path.Join(testhelper.GitlabTestStoragePath(), testRepo.GetRelativePath(), "objects", "pack")

	// Reset mtime to a long while ago since some filesystems don't have sub-second
	// precision on `mtime`.
	// Stamp taken from https://golang.org/pkg/time/#pkg-constants
	testhelper.MustRunCommand(t, nil, "touch", "-t", testTimeString, path.Join(packPath, "*"))
	testTime := time.Date(2006, 01, 02, 15, 04, 05, 0, time.UTC)
	c, err := client.RepackIncremental(context.Background(), &pb.RepackIncrementalRequest{Repository: testRepo})
	assert.NoError(t, err)
	assert.NotNil(t, c)

	// Entire `path`-folder gets updated so this is fine :D
	assertModTimeAfter(t, testTime, packPath)
}

func TestRepackIncrementalFailure(t *testing.T) {
	server := runRepoServer(t)
	defer server.Stop()

	client := newRepositoryClient(t)

	tests := []struct {
		repo *pb.Repository
		code codes.Code
		desc string
	}{
		{desc: "nil repo", repo: nil, code: codes.InvalidArgument},
		{desc: "invalid storage name", repo: &pb.Repository{StorageName: "foo"}, code: codes.InvalidArgument},
		{desc: "no storage name", repo: &pb.Repository{RelativePath: "bar"}, code: codes.InvalidArgument},
		{desc: "non-existing repo", repo: &pb.Repository{StorageName: testRepo.GetStorageName(), RelativePath: "bar"}, code: codes.NotFound},
	}

	for _, test := range tests {
		t.Logf("running test: %q", test.desc)
		_, err := client.RepackIncremental(context.Background(), &pb.RepackIncrementalRequest{Repository: test.repo})
		testhelper.AssertGrpcError(t, err, test.code, "")
	}

}

func TestRepackFullSuccess(t *testing.T) {
	server := runRepoServer(t)
	defer server.Stop()

	client := newRepositoryClient(t)

	tests := []struct {
		req  *pb.RepackFullRequest
		desc string
	}{
		{req: &pb.RepackFullRequest{Repository: testRepo, CreateBitmap: true}, desc: "with bitmap"},
		{req: &pb.RepackFullRequest{Repository: testRepo, CreateBitmap: false}, desc: "without bitmap"},
	}

	packPath := path.Join(testhelper.GitlabTestStoragePath(), testRepo.GetRelativePath(), "objects", "pack")

	for _, test := range tests {
		// Reset mtime to a long while ago since some filesystems don't have sub-second
		// precision on `mtime`.
		testhelper.MustRunCommand(t, nil, "touch", "-t", testTimeString, packPath)
		t.Logf("running test: %q", test.desc)
		testTime := time.Date(2006, 01, 02, 15, 04, 05, 0, time.UTC)
		c, err := client.RepackFull(context.Background(), test.req)
		assert.NoError(t, err)
		assert.NotNil(t, c)

		// Entire `path`-folder gets updated so this is fine :D
		assertModTimeAfter(t, testTime, packPath)

		bmPath, err := filepath.Glob(path.Join(packPath, "pack-*.bitmap"))
		if err != nil {
			t.Fatalf("Error globbing bitmaps: %v", err)
		}
		if test.req.GetCreateBitmap() {
			if len(bmPath) == 0 {
				t.Errorf("No bitmaps found")
			}
		} else {
			if len(bmPath) != 0 {
				t.Errorf("Bitmap found: %v", bmPath)
			}
		}
	}
}

func TestRepackFullFailure(t *testing.T) {
	server := runRepoServer(t)
	defer server.Stop()

	client := newRepositoryClient(t)

	tests := []struct {
		repo *pb.Repository
		code codes.Code
		desc string
	}{
		{desc: "nil repo", repo: nil, code: codes.InvalidArgument},
		{desc: "invalid storage name", repo: &pb.Repository{StorageName: "foo"}, code: codes.InvalidArgument},
		{desc: "no storage name", repo: &pb.Repository{RelativePath: "bar"}, code: codes.InvalidArgument},
		{desc: "non-existing repo", repo: &pb.Repository{StorageName: testRepo.GetStorageName(), RelativePath: "bar"}, code: codes.NotFound},
	}

	for _, test := range tests {
		t.Logf("running test: %q", test.desc)
		_, err := client.RepackFull(context.Background(), &pb.RepackFullRequest{Repository: test.repo})
		testhelper.AssertGrpcError(t, err, test.code, "")
	}

}
