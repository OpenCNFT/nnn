package setup

import (
	grpcprometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/service"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/service/blob"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/service/cleanup"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/service/commit"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/service/conflicts"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/service/diff"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/service/hook"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/service/internalgitaly"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/service/namespace"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/service/objectpool"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/service/operations"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/service/ref"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/service/remote"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/service/repository"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/service/server"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/service/smarthttp"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/service/ssh"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"
)

var (
	smarthttpPackfileNegotiationMetrics = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "gitaly",
			Subsystem: "smarthttp",
			Name:      "packfile_negotiation_requests_total",
			Help:      "Total number of features used for packfile negotiations",
		},
		[]string{"git_negotiation_feature"},
	)

	sshPackfileNegotiationMetrics = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "gitaly",
			Subsystem: "ssh",
			Name:      "packfile_negotiation_requests_total",
			Help:      "Total number of features used for packfile negotiations",
		},
		[]string{"git_negotiation_feature"},
	)
)

// RegisterAll will register all the known gRPC services on  the provided gRPC service instance.
func RegisterAll(srv *grpc.Server, deps *service.Dependencies) {
	gitalypb.RegisterBlobServiceServer(srv, blob.NewServer(deps))
	gitalypb.RegisterCleanupServiceServer(srv, cleanup.NewServer(deps))
	gitalypb.RegisterCommitServiceServer(srv, commit.NewServer(deps))
	gitalypb.RegisterDiffServiceServer(srv, diff.NewServer(deps))
	gitalypb.RegisterNamespaceServiceServer(srv, namespace.NewServer(deps))
	gitalypb.RegisterOperationServiceServer(srv, operations.NewServer(deps))
	gitalypb.RegisterRefServiceServer(srv, ref.NewServer(deps))
	gitalypb.RegisterRepositoryServiceServer(srv, repository.NewServer(deps))
	gitalypb.RegisterSSHServiceServer(srv, ssh.NewServer(deps,
		ssh.WithPackfileNegotiationMetrics(sshPackfileNegotiationMetrics),
	))
	gitalypb.RegisterSmartHTTPServiceServer(srv, smarthttp.NewServer(deps,
		smarthttp.WithPackfileNegotiationMetrics(smarthttpPackfileNegotiationMetrics),
	))
	gitalypb.RegisterConflictsServiceServer(srv, conflicts.NewServer(deps))
	gitalypb.RegisterRemoteServiceServer(srv, remote.NewServer(
		deps.GetLocator(),
		deps.GetGitCmdFactory(),
		deps.GetCatfileCache(),
		deps.GetTxManager(),
		deps.GetConnsPool(),
	))
	gitalypb.RegisterServerServiceServer(srv, server.NewServer(deps.GetGitCmdFactory(), deps.GetCfg().Storages))
	gitalypb.RegisterObjectPoolServiceServer(srv, objectpool.NewServer(
		deps.GetLocator(),
		deps.GetGitCmdFactory(),
		deps.GetCatfileCache(),
		deps.GetTxManager(),
		deps.GetHousekeepingManager(),
		deps.GetRepositoryCounter(),
	))
	gitalypb.RegisterHookServiceServer(srv, hook.NewServer(
		deps.GetHookManager(),
		deps.GetLocator(),
		deps.GetGitCmdFactory(),
		deps.GetPackObjectsCache(),
		deps.GetPackObjectsLimiter(),
	))
	gitalypb.RegisterInternalGitalyServer(srv, internalgitaly.NewServer(
		deps.GetCfg().Storages,
		deps.GetLocator(),
	))

	healthpb.RegisterHealthServer(srv, health.NewServer())
	reflection.Register(srv)
	grpcprometheus.Register(srv)
}
