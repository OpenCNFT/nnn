package wiki

import (
	"io"
	"testing"

	"google.golang.org/grpc/codes"

	"github.com/stretchr/testify/require"

	pb "gitlab.com/gitlab-org/gitaly-proto/go"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
)

func TestSuccessfulWikiGetAllPagesRequest(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	server, serverSocketPath := runWikiServiceServer(t)
	defer server.Stop()

	client, conn := newWikiClient(t, serverSocketPath)
	defer conn.Close()

	wikiRepo, _, cleanupFunc := setupWikiRepo(t)
	defer cleanupFunc()

	page1Name := "Page 1"
	page2Name := "Page 2"
	createTestWikiPage(t, client, wikiRepo, createWikiPageOpts{title: page1Name})
	page2Commit := createTestWikiPage(t, client, wikiRepo, createWikiPageOpts{title: page2Name})
	expectedPage1 := &pb.WikiPage{
		Version:    &pb.WikiPageVersion{Commit: page2Commit, Format: "markdown"},
		Title:      []byte(page1Name),
		Format:     "markdown",
		UrlPath:    "Page-1",
		Path:       []byte("Page-1.md"),
		Name:       []byte(page1Name),
		RawData:    mockPageContent,
		Historical: false,
	}
	expectedPage2 := &pb.WikiPage{
		Version:    &pb.WikiPageVersion{Commit: page2Commit, Format: "markdown"},
		Title:      []byte(page2Name),
		Format:     "markdown",
		UrlPath:    "Page-2",
		Path:       []byte("Page-2.md"),
		Name:       []byte(page2Name),
		RawData:    mockPageContent,
		Historical: false,
	}

	testcases := []struct {
		desc          string
		limit         uint32
		expectedCount int
	}{
		{
			desc:          "No limit",
			limit:         0,
			expectedCount: 2,
		},
		{
			desc:          "Limit of 1",
			limit:         1,
			expectedCount: 1,
		},
		{
			desc:          "Limit of 2",
			limit:         1,
			expectedCount: 1,
		},
	}

	expectedPages := []*pb.WikiPage{expectedPage1, expectedPage2}

	for _, tc := range testcases {
		t.Run(tc.desc, func(t *testing.T) {
			rpcRequest := pb.WikiGetAllPagesRequest{Repository: wikiRepo, Limit: tc.limit}

			c, err := client.WikiGetAllPages(ctx, &rpcRequest)
			require.NoError(t, err)

			receivedPages := readWikiPagesFromWikiGetAllPagesClient(t, c)

			require.Len(t, receivedPages, tc.expectedCount)

			for i := 0; i < tc.expectedCount; i++ {
				requireWikiPagesEqual(t, expectedPages[i], receivedPages[i])
			}
		})
	}
}

func TestFailedWikiGetAllPagesDueToValidation(t *testing.T) {
	server, serverSocketPath := runWikiServiceServer(t)
	defer server.Stop()

	client, conn := newWikiClient(t, serverSocketPath)
	defer conn.Close()

	rpcRequests := []pb.WikiGetAllPagesRequest{
		{Repository: &pb.Repository{StorageName: "fake", RelativePath: "path"}}, // Repository doesn't exist
		{Repository: nil}, // Repository is nil
	}

	for _, rpcRequest := range rpcRequests {
		ctx, cancel := testhelper.Context()
		defer cancel()

		c, err := client.WikiGetAllPages(ctx, &rpcRequest)
		require.NoError(t, err)

		err = drainWikiGetAllPagesResponse(c)
		testhelper.RequireGrpcError(t, err, codes.InvalidArgument)
	}
}

func readWikiPagesFromWikiGetAllPagesClient(t *testing.T, c pb.WikiService_WikiGetAllPagesClient) []*pb.WikiPage {
	var wikiPage *pb.WikiPage
	var wikiPages []*pb.WikiPage

	for {
		resp, err := c.Recv()
		if err == io.EOF {
			break
		} else if err != nil {
			t.Fatal(err)
		}

		if resp.EndOfPage {
			wikiPages = append(wikiPages, wikiPage)
			wikiPage = nil
		} else if wikiPage == nil {
			wikiPage = resp.GetPage()
		} else {
			wikiPage.RawData = append(wikiPage.RawData, resp.GetPage().GetRawData()...)
		}
	}

	return wikiPages
}

func drainWikiGetAllPagesResponse(c pb.WikiService_WikiGetAllPagesClient) error {
	for {
		_, err := c.Recv()
		if err == io.EOF {
			return nil
		} else if err != nil {
			return err
		}
	}
}
