package server

import (
	"context"
	"fmt"
	"os"

	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/mode/permission"
	"gitlab.com/gitlab-org/gitaly/v16/internal/helper/fstype"
	"gitlab.com/gitlab-org/gitaly/v16/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v16/internal/version"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
)

func (s *server) ServerInfo(ctx context.Context, in *gitalypb.ServerInfoRequest) (*gitalypb.ServerInfoResponse, error) {
	gitVersion, err := s.gitCmdFactory.GitVersion(ctx)
	if err != nil {
		return nil, structerr.NewInternal("%w", err)
	}

	var storageStatuses []*gitalypb.ServerInfoResponse_StorageStatus
	for _, shard := range s.storages {
		readable, writeable, err := shardCheck(shard.Path)
		if err != nil {
			return nil, fmt.Errorf("shard check: %w", err)
		}
		fsType := fstype.FileSystem(shard.Path)

		gitalyMetadata, err := storage.ReadMetadataFile(shard.Path)
		if err != nil {
			s.logger.WithField("storage", shard).WithError(err).ErrorContext(ctx, "reading gitaly metadata file")
		}

		storageStatuses = append(storageStatuses, &gitalypb.ServerInfoResponse_StorageStatus{
			StorageName:       shard.Name,
			ReplicationFactor: 1, // gitaly is always treated as a single replica
			Readable:          readable,
			Writeable:         writeable,
			FsType:            fsType,
			FilesystemId:      gitalyMetadata.GitalyFilesystemID,
		})
	}

	return &gitalypb.ServerInfoResponse{
		ServerVersion:   version.GetVersion(),
		GitVersion:      gitVersion.String(),
		StorageStatuses: storageStatuses,
	}, nil
}

func shardCheck(shardPath string) (bool, bool, error) {
	info, err := os.Stat(shardPath)
	if err != nil {
		return false, false, fmt.Errorf("stat: %w", err)
	}

	readable := info.Mode()&(permission.OwnerRead|permission.OwnerExecute) == permission.OwnerRead|permission.OwnerExecute
	writable := info.Mode()&permission.OwnerWrite == permission.OwnerWrite

	return readable, writable, nil
}
