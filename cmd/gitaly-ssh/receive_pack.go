package main

import (
	"context"
	"fmt"
	"os"

	"github.com/golang/protobuf/jsonpb"
	"gitlab.com/gitlab-org/gitaly-proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/client"
	"google.golang.org/grpc"
)

func receivePack(conn *grpc.ClientConn, req string) (int32, error) {
	var request gitalypb.SSHReceivePackRequest
	if err := jsonpb.UnmarshalString(req, &request); err != nil {
		return 0, fmt.Errorf("json unmarshal: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	return client.ReceivePack(ctx, conn, os.Stdin, os.Stdout, os.Stderr, &request)
}
