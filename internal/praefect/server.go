/*
Package praefect is a Gitaly reverse proxy for transparently routing gRPC
calls to a set of Gitaly services.
*/
package praefect

import (
	"time"

	grpcmwlogrus "github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus"
	grpcmwtags "github.com/grpc-ecosystem/go-grpc-middleware/tags"
	grpcprometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/sirupsen/logrus"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/server/auth"
	"gitlab.com/gitlab-org/gitaly/v16/internal/grpc/backchannel"
	"gitlab.com/gitlab-org/gitaly/v16/internal/grpc/grpcstats"
	"gitlab.com/gitlab-org/gitaly/v16/internal/grpc/listenmux"
	"gitlab.com/gitlab-org/gitaly/v16/internal/grpc/middleware/metadatahandler"
	"gitlab.com/gitlab-org/gitaly/v16/internal/grpc/middleware/panichandler"
	"gitlab.com/gitlab-org/gitaly/v16/internal/grpc/middleware/sentryhandler"
	"gitlab.com/gitlab-org/gitaly/v16/internal/grpc/middleware/statushandler"
	"gitlab.com/gitlab-org/gitaly/v16/internal/grpc/proxy"
	"gitlab.com/gitlab-org/gitaly/v16/internal/grpc/sidechannel"
	"gitlab.com/gitlab-org/gitaly/v16/internal/helper/fieldextractors"
	"gitlab.com/gitlab-org/gitaly/v16/internal/log"
	"gitlab.com/gitlab-org/gitaly/v16/internal/praefect/config"
	"gitlab.com/gitlab-org/gitaly/v16/internal/praefect/datastore"
	"gitlab.com/gitlab-org/gitaly/v16/internal/praefect/middleware"
	"gitlab.com/gitlab-org/gitaly/v16/internal/praefect/protoregistry"
	"gitlab.com/gitlab-org/gitaly/v16/internal/praefect/service"
	"gitlab.com/gitlab-org/gitaly/v16/internal/praefect/service/info"
	"gitlab.com/gitlab-org/gitaly/v16/internal/praefect/service/server"
	"gitlab.com/gitlab-org/gitaly/v16/internal/praefect/service/transaction"
	"gitlab.com/gitlab-org/gitaly/v16/internal/praefect/transactions"
	"gitlab.com/gitlab-org/gitaly/v16/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
	grpccorrelation "gitlab.com/gitlab-org/labkit/correlation/grpc"
	grpctracing "gitlab.com/gitlab-org/labkit/tracing/grpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/keepalive"
)

// NewBackchannelServerFactory returns a ServerFactory that serves the RefTransactionServer on the backchannel
// connection.
func NewBackchannelServerFactory(logger *logrus.Entry, refSvc gitalypb.RefTransactionServer, registry *sidechannel.Registry) backchannel.ServerFactory {
	logMsgProducer := log.MessageProducer(
		log.PropagationMessageProducer(grpcmwlogrus.DefaultMessageProducer),
		structerr.FieldsProducer,
	)

	return func() backchannel.Server {
		lm := listenmux.New(insecure.NewCredentials())
		lm.Register(sidechannel.NewServerHandshaker(registry))
		srv := grpc.NewServer(
			grpc.ChainUnaryInterceptor(
				commonUnaryServerInterceptors(logger, logMsgProducer)...,
			),
			grpc.Creds(lm),
		)
		gitalypb.RegisterRefTransactionServer(srv, refSvc)
		grpcprometheus.Register(srv)
		return srv
	}
}

func commonUnaryServerInterceptors(logger *logrus.Entry, messageProducer grpcmwlogrus.MessageProducer) []grpc.UnaryServerInterceptor {
	return []grpc.UnaryServerInterceptor{
		grpcmwtags.UnaryServerInterceptor(ctxtagsInterceptorOption()),
		grpccorrelation.UnaryServerCorrelationInterceptor(), // Must be above the metadata handler
		metadatahandler.UnaryInterceptor,
		grpcprometheus.UnaryServerInterceptor,
		grpcmwlogrus.UnaryServerInterceptor(logger,
			grpcmwlogrus.WithTimestampFormat(log.LogTimestampFormat),
			grpcmwlogrus.WithMessageProducer(messageProducer),
			log.DeciderOption(),
		),
		sentryhandler.UnaryLogHandler,
		statushandler.Unary, // Should be below LogHandler
		grpctracing.UnaryServerTracingInterceptor(),
		// Panic handler should remain last so that application panics will be
		// converted to errors and logged
		panichandler.UnaryPanicHandler,
	}
}

func ctxtagsInterceptorOption() grpcmwtags.Option {
	return grpcmwtags.WithFieldExtractorForInitialReq(fieldextractors.FieldExtractor)
}

// ServerOption is an option that can be passed to `NewGRPCServer()`.
type ServerOption func(cfg *serverConfig)

type serverConfig struct {
	unaryInterceptors  []grpc.UnaryServerInterceptor
	streamInterceptors []grpc.StreamServerInterceptor
}

// WithUnaryInterceptor adds another interceptor that shall be executed for unary RPC calls.
func WithUnaryInterceptor(interceptor grpc.UnaryServerInterceptor) ServerOption {
	return func(cfg *serverConfig) {
		cfg.unaryInterceptors = append(cfg.unaryInterceptors, interceptor)
	}
}

// WithStreamInterceptor adds another interceptor that shall be executed for streaming RPC calls.
func WithStreamInterceptor(interceptor grpc.StreamServerInterceptor) ServerOption {
	return func(cfg *serverConfig) {
		cfg.streamInterceptors = append(cfg.streamInterceptors, interceptor)
	}
}

// NewGRPCServer returns gRPC server wuth registered proxy-handler and actual services praefect serves on its own.
// It includes a set of unary and stream interceptors required to add logging, authentication, etc.
func NewGRPCServer(
	conf config.Config,
	logger *logrus.Entry,
	registry *protoregistry.Registry,
	director proxy.StreamDirector,
	txMgr *transactions.Manager,
	rs datastore.RepositoryStore,
	assignmentStore AssignmentStore,
	router Router,
	conns Connections,
	primaryGetter PrimaryGetter,
	creds credentials.TransportCredentials,
	checks []service.CheckFunc,
	opts ...ServerOption,
) *grpc.Server {
	var serverCfg serverConfig
	for _, opt := range opts {
		opt(&serverCfg)
	}

	logMsgProducer := log.MessageProducer(
		log.PropagationMessageProducer(grpcmwlogrus.DefaultMessageProducer),
		structerr.FieldsProducer,
	)

	unaryInterceptors := append(
		commonUnaryServerInterceptors(logger, logMsgProducer),
		middleware.MethodTypeUnaryInterceptor(registry),
		auth.UnaryServerInterceptor(conf.Auth),
	)
	unaryInterceptors = append(unaryInterceptors, serverCfg.unaryInterceptors...)

	streamInterceptors := []grpc.StreamServerInterceptor{
		grpcmwtags.StreamServerInterceptor(ctxtagsInterceptorOption()),
		grpccorrelation.StreamServerCorrelationInterceptor(), // Must be above the metadata handler
		middleware.MethodTypeStreamInterceptor(registry),
		metadatahandler.StreamInterceptor,
		grpcprometheus.StreamServerInterceptor,
		grpcmwlogrus.StreamServerInterceptor(logger,
			grpcmwlogrus.WithTimestampFormat(log.LogTimestampFormat),
			grpcmwlogrus.WithMessageProducer(logMsgProducer),
			log.DeciderOption(),
		),
		sentryhandler.StreamLogHandler,
		statushandler.Stream, // Should be below LogHandler
		grpctracing.StreamServerTracingInterceptor(),
		auth.StreamServerInterceptor(conf.Auth),
		// Panic handler should remain last so that application panics will be
		// converted to errors and logged
		panichandler.StreamPanicHandler,
	}
	streamInterceptors = append(streamInterceptors, serverCfg.streamInterceptors...)

	grpcOpts := proxyRequiredOpts(director)
	grpcOpts = append(grpcOpts, []grpc.ServerOption{
		grpc.StatsHandler(log.PerRPCLogHandler{
			Underlying:     &grpcstats.PayloadBytes{},
			FieldProducers: []log.FieldsProducer{grpcstats.FieldsProducer},
		}),
		grpc.ChainStreamInterceptor(streamInterceptors...),
		grpc.ChainUnaryInterceptor(unaryInterceptors...),
		// We deliberately set the server MinTime to significantly less than the client interval of 20
		// seconds to allow for network jitter. We can afford to be forgiving as the maximum number of
		// concurrent clients for a Gitaly server is typically in the hundreds and this volume of
		// keepalives won't add significant load.
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime:             10 * time.Second,
			PermitWithoutStream: true,
		}),
		grpc.KeepaliveParams(keepalive.ServerParameters{
			Time: 5 * time.Minute,
		}),
	}...)

	// Accept backchannel connections so that we can proxy sidechannels
	// from clients (e.g. Workhorse) to a backend Gitaly server.
	if creds == nil {
		creds = insecure.NewCredentials()
	}
	lm := listenmux.New(creds)
	lm.Register(backchannel.NewServerHandshaker(logger, backchannel.NewRegistry(), nil))
	grpcOpts = append(grpcOpts, grpc.Creds(lm))

	warnDupeAddrs(logger, conf)

	srv := grpc.NewServer(grpcOpts...)
	registerServices(srv, txMgr, conf, rs, assignmentStore, service.Connections(conns), primaryGetter, checks)

	if conf.Failover.ElectionStrategy == config.ElectionStrategyPerRepository {
		proxy.RegisterStreamHandlers(srv, "gitaly.RepositoryService", map[string]grpc.StreamHandler{
			"RemoveAll":        RemoveAllHandler(rs, conns),
			"RemoveRepository": RemoveRepositoryHandler(rs, conns),
			"RenameRepository": RenameRepositoryHandler(conf.VirtualStorageNames(), rs),
			"RepositoryExists": RepositoryExistsHandler(rs),
		})
		proxy.RegisterStreamHandlers(srv, "gitaly.ObjectPoolService", map[string]grpc.StreamHandler{
			"DeleteObjectPool": DeleteObjectPoolHandler(rs, conns),
		})
	}

	return srv
}

func proxyRequiredOpts(director proxy.StreamDirector) []grpc.ServerOption {
	return []grpc.ServerOption{
		grpc.ForceServerCodec(proxy.NewCodec()),
		grpc.UnknownServiceHandler(proxy.TransparentHandler(director)),
	}
}

// registerServices registers services praefect needs to handle RPCs on its own.
func registerServices(
	srv *grpc.Server,
	tm *transactions.Manager,
	conf config.Config,
	rs datastore.RepositoryStore,
	assignmentStore AssignmentStore,
	conns service.Connections,
	primaryGetter info.PrimaryGetter,
	checks []service.CheckFunc,
) {
	// ServerServiceServer is necessary for the ServerInfo RPC
	gitalypb.RegisterServerServiceServer(srv, server.NewServer(conf, conns, checks))
	gitalypb.RegisterPraefectInfoServiceServer(srv, info.NewServer(conf, rs, assignmentStore, conns, primaryGetter))
	gitalypb.RegisterRefTransactionServer(srv, transaction.NewServer(tm))
	healthpb.RegisterHealthServer(srv, health.NewServer())

	grpcprometheus.Register(srv)
}

func warnDupeAddrs(logger logrus.FieldLogger, conf config.Config) {
	var fishy bool

	for _, virtualStorage := range conf.VirtualStorages {
		addrSet := map[string]struct{}{}
		for _, n := range virtualStorage.Nodes {
			_, ok := addrSet[n.Address]
			if ok {
				logger.Warnf("more than one backend node is hosted at %s", n.Address)
				fishy = true
				continue
			}
			addrSet[n.Address] = struct{}{}
		}
		if fishy {
			logger.Warnf("your Praefect configuration may not offer actual redundancy")
		}
	}
}
