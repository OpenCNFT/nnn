package service

import (
	pb "gitlab.com/gitlab-org/gitaly-proto/go"
	"gitlab.com/gitlab-org/gitaly/internal/rubyserver"
	"gitlab.com/gitlab-org/gitaly/internal/service/blob"
	"gitlab.com/gitlab-org/gitaly/internal/service/commit"
	"gitlab.com/gitlab-org/gitaly/internal/service/conflicts"
	"gitlab.com/gitlab-org/gitaly/internal/service/diff"
	"gitlab.com/gitlab-org/gitaly/internal/service/namespace"
	"gitlab.com/gitlab-org/gitaly/internal/service/notifications"
	"gitlab.com/gitlab-org/gitaly/internal/service/operations"
	"gitlab.com/gitlab-org/gitaly/internal/service/ref"
	"gitlab.com/gitlab-org/gitaly/internal/service/remote"
	"gitlab.com/gitlab-org/gitaly/internal/service/repository"
	"gitlab.com/gitlab-org/gitaly/internal/service/server"
	"gitlab.com/gitlab-org/gitaly/internal/service/smarthttp"
	"gitlab.com/gitlab-org/gitaly/internal/service/ssh"
	"gitlab.com/gitlab-org/gitaly/internal/service/storage"
	"gitlab.com/gitlab-org/gitaly/internal/service/wiki"

	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

// RegisterAll will register all the known grpc services with
// the specified grpc service instance
func RegisterAll(grpcServer *grpc.Server, rubyServer *rubyserver.Server) {
	pb.RegisterBlobServiceServer(grpcServer, blob.NewServer(rubyServer))
	pb.RegisterCommitServiceServer(grpcServer, commit.NewServer(rubyServer))
	pb.RegisterDiffServiceServer(grpcServer, diff.NewServer(rubyServer))
	pb.RegisterNamespaceServiceServer(grpcServer, namespace.NewServer())
	pb.RegisterNotificationServiceServer(grpcServer, notifications.NewServer())
	pb.RegisterOperationServiceServer(grpcServer, operations.NewServer(rubyServer))
	pb.RegisterRefServiceServer(grpcServer, ref.NewServer(rubyServer))
	pb.RegisterRepositoryServiceServer(grpcServer, repository.NewServer(rubyServer))
	pb.RegisterSSHServiceServer(grpcServer, ssh.NewServer())
	pb.RegisterSmartHTTPServiceServer(grpcServer, smarthttp.NewServer())
	pb.RegisterWikiServiceServer(grpcServer, wiki.NewServer(rubyServer))
	pb.RegisterConflictsServiceServer(grpcServer, conflicts.NewServer(rubyServer))
	pb.RegisterRemoteServiceServer(grpcServer, remote.NewServer(rubyServer))
	pb.RegisterServerServiceServer(grpcServer, server.NewServer())
	pb.RegisterStorageServiceServer(grpcServer, storage.NewServer())

	healthpb.RegisterHealthServer(grpcServer, health.NewServer())
}
