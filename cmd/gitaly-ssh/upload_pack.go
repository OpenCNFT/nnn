package main

import (
	"context"
	"fmt"
	"os"

	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/gitalyclient"
	"gitlab.com/gitlab-org/gitaly/v16/internal/grpc/sidechannel"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/encoding/protojson"
)

const (
	// GitConfigShowAllRefs is a git-config option.
	// We have to use a negative transfer.hideRefs since this is the only way
	// to undo an already set parameter: https://www.spinics.net/lists/git/msg256772.html
	GitConfigShowAllRefs = "transfer.hideRefs=!refs"
)

func uploadPackConfig(config []string) []string {
	return append([]string{GitConfigShowAllRefs}, config...)
}

func uploadPack(ctx context.Context, conn *grpc.ClientConn, registry *sidechannel.Registry, req string) (int32, error) {
	var request gitalypb.SSHUploadPackRequest
	if err := protojson.Unmarshal([]byte(req), &request); err != nil {
		return 0, fmt.Errorf("json unmarshal: %w", err)
	}

	request.GitConfigOptions = uploadPackConfig(request.GetGitConfigOptions())

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	return gitalyclient.UploadPack(ctx, conn, os.Stdin, os.Stdout, os.Stderr, &request)
}

func uploadPackWithSidechannel(ctx context.Context, conn *grpc.ClientConn, registry *sidechannel.Registry, req string) (int32, error) {
	var request gitalypb.SSHUploadPackWithSidechannelRequest
	if err := protojson.Unmarshal([]byte(req), &request); err != nil {
		return 0, fmt.Errorf("json unmarshal: %w", err)
	}

	request.GitConfigOptions = uploadPackConfig(request.GetGitConfigOptions())

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	result, err := gitalyclient.UploadPackWithSidechannel(ctx, conn, registry, os.Stdin, os.Stdout, os.Stderr, &request)
	if err != nil {
		return 0, err
	}

	return result.ExitCode, nil
}
