package commit

import (
	"io"
	"testing"

	"google.golang.org/grpc/codes"

	"github.com/stretchr/testify/require"

	pb "gitlab.com/gitlab-org/gitaly-proto/go"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
)

func TestFilterShasWithSignaturesSuccessful(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	server, serverSocketPath := startTestServices(t)
	defer server.Stop()

	client, conn := newCommitServiceClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, _, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	testCases := []struct {
		desc string
		in   [][]byte
		out  [][]byte
	}{
		{
			desc: "3 shas, none signed",
			in:   [][]byte{[]byte("6907208d755b60ebeacb2e9dfea74c92c3449a1f"), []byte("c347ca2e140aa667b968e51ed0ffe055501fe4f4"), []byte("d59c60028b053793cecfb4022de34602e1a9218e")},
			out:  nil,
		},
		{
			desc: "3 shas, all signed",
			in:   [][]byte{[]byte("5937ac0a7beb003549fc5fd26fc247adbce4a52e"), []byte("570e7b2abdd848b95f2f578043fc23bd6f6fd24d"), []byte("6f6d7e7ed97bb5f0054f2b1df789b39ca89b6ff9")},
			out:  [][]byte{[]byte("5937ac0a7beb003549fc5fd26fc247adbce4a52e"), []byte("570e7b2abdd848b95f2f578043fc23bd6f6fd24d"), []byte("6f6d7e7ed97bb5f0054f2b1df789b39ca89b6ff9")},
		},
		{
			desc: "3 shas, middle unsigned",
			in:   [][]byte{[]byte("5937ac0a7beb003549fc5fd26fc247adbce4a52e"), []byte("66eceea0db202bb39c4e445e8ca28689645366c5"), []byte("6f6d7e7ed97bb5f0054f2b1df789b39ca89b6ff9")},
			out:  [][]byte{[]byte("5937ac0a7beb003549fc5fd26fc247adbce4a52e"), []byte("6f6d7e7ed97bb5f0054f2b1df789b39ca89b6ff9")},
		},
		{
			desc: "3 shas, middle non-existant",
			in:   [][]byte{[]byte("5937ac0a7beb003549fc5fd26fc247adbce4a52e"), []byte("deadf00d00000000000000000000000000000000"), []byte("6f6d7e7ed97bb5f0054f2b1df789b39ca89b6ff9")},
			out:  [][]byte{[]byte("5937ac0a7beb003549fc5fd26fc247adbce4a52e"), []byte("6f6d7e7ed97bb5f0054f2b1df789b39ca89b6ff9")},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.desc, func(t *testing.T) {
			stream, err := client.FilterShasWithSignatures(ctx)
			require.NoError(t, err)
			require.NoError(t, stream.Send(&pb.FilterShasWithSignaturesRequest{Repository: testRepo, Shas: testCase.in}))
			require.NoError(t, stream.CloseSend())
			recvOut, err := recvFSWS(stream)
			require.NoError(t, err)
			require.Equal(t, testCase.out, recvOut)
		})
	}
}

func TestFilterShasWithSignaturesValidationError(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	server, serverSocketPath := startTestServices(t)
	defer server.Stop()

	client, conn := newCommitServiceClient(t, serverSocketPath)
	defer conn.Close()

	stream, err := client.FilterShasWithSignatures(ctx)
	require.NoError(t, err)

	require.NoError(t, stream.Send(&pb.FilterShasWithSignaturesRequest{}))
	require.NoError(t, stream.CloseSend())

	_, err = recvFSWS(stream)
	testhelper.RequireGrpcError(t, err, codes.InvalidArgument)
	require.Contains(t, err.Error(), "no repository given")
}

func recvFSWS(stream pb.CommitService_FilterShasWithSignaturesClient) ([][]byte, error) {
	var ret [][]byte
	resp, err := stream.Recv()
	for ; err == nil; resp, err = stream.Recv() {
		ret = append(ret, resp.GetShas()...)
	}
	if err != io.EOF {
		return nil, err
	}
	return ret, nil
}
