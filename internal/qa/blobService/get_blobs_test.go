package blobService

import (
	"context"
	"fmt"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"gitlab.com/gitlab-org/gitaly/v15/proto/go/gitalypb"
	"google.golang.org/grpc"
	"io"
)

var _ = Describe("BlobService", func() {
	var (
		client gitalypb.BlobServiceClient
		conn   *grpc.ClientConn
		err    error
	)

	BeforeEach(func() {
		//socketPath := filepath.Join(cfg.Storages[0].Path, "/Users/john/dev/gdk", "gitaly.socket")
		conn, err = grpc.Dial("gdk.test:9999", grpc.WithInsecure())
		Expect(err).NotTo(HaveOccurred())

		client = gitalypb.NewBlobServiceClient(conn)
	})

	AfterEach(func() {
		conn.Close()
	})

	It("should get a blobs from a repository", func() {
		repo := &gitalypb.Repository{
			StorageName:  "default",
			RelativePath: "@hashed/2f/ca/2fca346db656187102ce806ac732e06a62df0dbb2829e511a770556d398e1a6e.git",
		}
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		request := &gitalypb.GetBlobsRequest{
			Repository: repo,
			RevisionPaths: []*gitalypb.GetBlobsRequest_RevisionPath{
				{
					Revision: "d9343d5a866b214dbb23759df549efad6b0c066a",
					Path:     []byte("ginkgo-testing-2"),
				},
			},
			Limit: 0,
		}
		stream, err := client.GetBlobs(ctx, request)
		Expect(err).NotTo(HaveOccurred())

		// read blob data
		var data []byte
		for {
			response, err := stream.Recv()
			fmt.Println("response: ", response)
			if err != nil {
				Expect(err).To(Equal(io.EOF))
				break
			}
			data = append(data, response.Data...)
		}

		fmt.Print("data is: ***", string(data), "***")
		Expect(string(data)).To(ContainElement("ginkgo-testing-2"))
	})
})
