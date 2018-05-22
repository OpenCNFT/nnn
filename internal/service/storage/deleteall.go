package storage

import (
	"io"
	"os"
	"path"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus"
	log "github.com/sirupsen/logrus"
	pb "gitlab.com/gitlab-org/gitaly-proto/go"
	"gitlab.com/gitlab-org/gitaly/internal/helper"
	"gitlab.com/gitlab-org/gitaly/internal/tempdir"
	"golang.org/x/net/context"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *server) DeleteAllRepositories(ctx context.Context, req *pb.DeleteAllRepositoriesRequest) (*pb.DeleteAllRepositoriesResponse, error) {
	storageDir, err := helper.GetStorageByName(req.StorageName)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "storage lookup failed: %v", err)
	}

	trashDir, err := tempdir.ForDeleteAllRepositories(req.StorageName)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "create trash dir: %v", err)
	}

	dir, err := os.Open(storageDir)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "open storage dir: %v", err)
	}
	defer dir.Close()

	grpc_logrus.Extract(ctx).WithFields(log.Fields{
		"trashDir": trashDir,
		"storage":  req.StorageName,
	}).Warn("moving all repositories in storage to trash")

	count := 0
	for done := false; !done; {
		dirents, err := dir.Readdir(100)
		if err == io.EOF {
			done = true
		} else if err != nil {
			return nil, status.Errorf(codes.Internal, "read storage dir: %v", err)
		}

		for _, d := range dirents {
			if d.Name() == tempdir.GitalyDataPrefix {
				continue
			}

			count++

			if err := os.Rename(path.Join(storageDir, d.Name()), path.Join(trashDir, d.Name())); err != nil {
				return nil, status.Errorf(codes.Internal, "move dir: %v", err)
			}
		}
	}

	grpc_logrus.Extract(ctx).WithFields(log.Fields{
		"trashDir":       trashDir,
		"storage":        req.StorageName,
		"numDirectories": count,
	}).Warn("directories moved to trash")

	return &pb.DeleteAllRepositoriesResponse{}, nil
}
