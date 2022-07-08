//go:build !gitaly_test_sha256

package server

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"net"
	"os"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v15/client"
	"gitlab.com/gitlab-org/gitaly/v15/internal/backchannel"
	"gitlab.com/gitlab-org/gitaly/v15/internal/bootstrap/starter"
	"gitlab.com/gitlab-org/gitaly/v15/internal/cache"
	"gitlab.com/gitlab-org/gitaly/v15/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v15/internal/middleware/limithandler"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper/testcfg"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
)

func TestGitalyServerFactory(t *testing.T) {
	ctx := testhelper.Context(t)

	checkHealth := func(t *testing.T, sf *GitalyServerFactory, schema, addr string) healthpb.HealthClient {
		t.Helper()

		var cc *grpc.ClientConn
		if schema == starter.TLS {
			listener, err := net.Listen(starter.TCP, addr)
			require.NoError(t, err)
			t.Cleanup(func() { listener.Close() })

			srv, err := sf.CreateExternal(true)
			require.NoError(t, err)
			healthpb.RegisterHealthServer(srv, health.NewServer())
			go srv.Serve(listener)

			certPool, err := x509.SystemCertPool()
			require.NoError(t, err)

			pem := testhelper.MustReadFile(t, sf.cfg.TLS.CertPath)
			require.True(t, certPool.AppendCertsFromPEM(pem))

			creds := credentials.NewTLS(&tls.Config{
				RootCAs:    certPool,
				MinVersion: tls.VersionTLS12,
			})

			cc, err = grpc.DialContext(ctx, listener.Addr().String(), grpc.WithTransportCredentials(creds))
			require.NoError(t, err)
		} else {
			listener, err := net.Listen(schema, addr)
			require.NoError(t, err)
			t.Cleanup(func() { listener.Close() })

			srv, err := sf.CreateExternal(false)
			require.NoError(t, err)
			healthpb.RegisterHealthServer(srv, health.NewServer())
			go srv.Serve(listener)

			endpoint, err := starter.ComposeEndpoint(schema, listener.Addr().String())
			require.NoError(t, err)

			cc, err = client.Dial(endpoint, nil)
			require.NoError(t, err)
		}

		t.Cleanup(func() { cc.Close() })

		healthClient := healthpb.NewHealthClient(cc)

		resp, err := healthClient.Check(ctx, &healthpb.HealthCheckRequest{})
		require.NoError(t, err)
		require.Equal(t, healthpb.HealthCheckResponse_SERVING, resp.Status)
		return healthClient
	}

	t.Run("insecure", func(t *testing.T) {
		cfg := testcfg.Build(t)
		sf := NewGitalyServerFactory(
			cfg,
			testhelper.NewDiscardingLogEntry(t),
			backchannel.NewRegistry(),
			cache.New(cfg, config.NewLocator(cfg)),
			[]*limithandler.LimiterMiddleware{limithandler.New(cfg, limithandler.LimitConcurrencyByRepo, limithandler.WithConcurrencyLimiters)},
		)

		checkHealth(t, sf, starter.TCP, "localhost:0")
	})

	t.Run("secure", func(t *testing.T) {
		certFile, keyFile := testhelper.GenerateCerts(t)

		cfg := testcfg.Build(t, testcfg.WithBase(config.Cfg{TLS: config.TLS{
			CertPath: certFile,
			KeyPath:  keyFile,
		}}))

		sf := NewGitalyServerFactory(
			cfg,
			testhelper.NewDiscardingLogEntry(t),
			backchannel.NewRegistry(),
			cache.New(cfg, config.NewLocator(cfg)),
			[]*limithandler.LimiterMiddleware{limithandler.New(cfg, limithandler.LimitConcurrencyByRepo, limithandler.WithConcurrencyLimiters)},
		)
		t.Cleanup(sf.Stop)

		checkHealth(t, sf, starter.TLS, "localhost:0")
	})

	t.Run("all services must be stopped", func(t *testing.T) {
		cfg := testcfg.Build(t)
		sf := NewGitalyServerFactory(
			cfg,
			testhelper.NewDiscardingLogEntry(t),
			backchannel.NewRegistry(),
			cache.New(cfg, config.NewLocator(cfg)),
			[]*limithandler.LimiterMiddleware{limithandler.New(cfg, limithandler.LimitConcurrencyByRepo, limithandler.WithConcurrencyLimiters)},
		)
		t.Cleanup(sf.Stop)

		tcpHealthClient := checkHealth(t, sf, starter.TCP, "localhost:0")

		socket := testhelper.GetTemporaryGitalySocketFileName(t)
		t.Cleanup(func() { require.NoError(t, os.RemoveAll(socket)) })

		socketHealthClient := checkHealth(t, sf, starter.Unix, socket)

		sf.GracefulStop() // stops all started servers(listeners)

		_, tcpErr := tcpHealthClient.Check(ctx, &healthpb.HealthCheckRequest{})
		require.Equal(t, codes.Unavailable, status.Code(tcpErr))

		_, socketErr := socketHealthClient.Check(ctx, &healthpb.HealthCheckRequest{})
		require.Equal(t, codes.Unavailable, status.Code(socketErr))
	})

	t.Run("logging check", func(t *testing.T) {
		t.Setenv("GITALY_LOG_REQUEST_METHOD_ALLOW_PATTERN", ".")

		cfg := testcfg.Build(t)
		logger, hook := test.NewNullLogger()
		sf := NewGitalyServerFactory(
			cfg,
			logger.WithContext(ctx),
			backchannel.NewRegistry(),
			cache.New(cfg, config.NewLocator(cfg)),
			[]*limithandler.LimiterMiddleware{limithandler.New(cfg, limithandler.LimitConcurrencyByRepo, limithandler.WithConcurrencyLimiters)},
		)

		checkHealth(t, sf, starter.TCP, "localhost:0")

		// Stop all servers to drain any postprocessing done for RPC calls.
		sf.GracefulStop()

		var entry *logrus.Entry
		for _, e := range hook.AllEntries() {
			if e.Message == "finished unary call with code OK" {
				entry = e
				break
			}
		}
		require.NotNil(t, entry)
		reqSize, found := entry.Data["grpc.request.payload_bytes"]
		assert.EqualValues(t, 0, reqSize)
		require.True(t, found)
		respSize, found := entry.Data["grpc.response.payload_bytes"]
		assert.EqualValues(t, 2, respSize)
		require.True(t, found)
	})
}

func TestGitalyServerFactory_closeOrder(t *testing.T) {
	ctx := testhelper.Context(t)

	cfg := testcfg.Build(t)
	sf := NewGitalyServerFactory(
		cfg,
		testhelper.NewDiscardingLogEntry(t),
		backchannel.NewRegistry(),
		cache.New(cfg, config.NewLocator(cfg)),
		[]*limithandler.LimiterMiddleware{limithandler.New(cfg, limithandler.LimitConcurrencyByRepo, limithandler.WithConcurrencyLimiters)},
	)
	defer sf.Stop()

	errQuickRPC := status.Error(codes.Internal, "quick RPC")
	errBlockingRPC := status.Error(codes.Internal, "blocking RPC")

	invokeQuick := func(conn *grpc.ClientConn, shouldSucceed bool) {
		err := conn.Invoke(ctx, "/Service/Quick", &healthpb.HealthCheckRequest{}, &healthpb.HealthCheckRequest{})
		if !shouldSucceed {
			testhelper.RequireGrpcCode(t, err, codes.Unavailable)
			return
		}

		testhelper.RequireGrpcError(t, errQuickRPC, err)
	}

	invokeBlocking := func(conn *grpc.ClientConn) chan struct{} {
		rpcFinished := make(chan struct{})

		go func() {
			defer close(rpcFinished)
			testhelper.RequireGrpcError(t,
				errBlockingRPC,
				conn.Invoke(ctx, "/Service/Blocking", &healthpb.HealthCheckRequest{}, &healthpb.HealthCheckRequest{}),
			)
		}()

		return rpcFinished
	}

	waitUntilFailure := func(conn *grpc.ClientConn) {
		for {
			err := conn.Invoke(ctx, "/Service/Quick", &healthpb.HealthCheckRequest{}, &healthpb.HealthCheckRequest{})
			if errors.Is(err, errQuickRPC) {
				continue
			}

			testhelper.RequireGrpcCode(t, err, codes.Unavailable)
			break
		}
	}

	var internalConn, externalConn *grpc.ClientConn
	var internalIsBlocking, externalIsBlocking chan struct{}
	var releaseInternalBlock, releaseExternalBlock chan struct{}
	for _, builder := range []struct {
		createServer func() *grpc.Server
		conn         **grpc.ClientConn
		isBlocking   *chan struct{}
		releaseBlock *chan struct{}
	}{
		{
			createServer: func() *grpc.Server {
				server, err := sf.CreateInternal()
				require.NoError(t, err)
				return server
			},
			conn:         &internalConn,
			isBlocking:   &internalIsBlocking,
			releaseBlock: &releaseInternalBlock,
		},
		{
			createServer: func() *grpc.Server {
				server, err := sf.CreateExternal(false)
				require.NoError(t, err)
				return server
			},
			conn:         &externalConn,
			isBlocking:   &externalIsBlocking,
			releaseBlock: &releaseExternalBlock,
		},
	} {
		server := builder.createServer()

		releaseBlock := make(chan struct{})
		*builder.releaseBlock = releaseBlock

		isBlocking := make(chan struct{})
		*builder.isBlocking = isBlocking

		server.RegisterService(&grpc.ServiceDesc{
			ServiceName: "Service",
			Methods: []grpc.MethodDesc{
				{
					MethodName: "Quick",
					Handler: func(interface{}, context.Context, func(interface{}) error, grpc.UnaryServerInterceptor) (interface{}, error) {
						return nil, errQuickRPC
					},
				},
				{
					MethodName: "Blocking",
					Handler: func(interface{}, context.Context, func(interface{}) error, grpc.UnaryServerInterceptor) (interface{}, error) {
						close(isBlocking)
						<-releaseBlock
						return nil, errBlockingRPC
					},
				},
			},
			HandlerType: (*interface{})(nil),
		}, server)

		ln, err := net.Listen("tcp", "localhost:0")
		require.NoError(t, err)
		defer ln.Close()

		go server.Serve(ln)

		conn, err := grpc.DialContext(ctx, ln.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
		require.NoError(t, err)
		t.Cleanup(func() { testhelper.MustClose(t, conn) })
		*builder.conn = conn
	}

	// both servers should be up and accepting RPCs
	invokeQuick(externalConn, true)
	invokeQuick(internalConn, true)

	// invoke a blocking RPC on the external server to block the graceful shutdown
	invokeBlocking(externalConn)
	<-externalIsBlocking

	shutdownCompeleted := make(chan struct{})
	go func() {
		defer close(shutdownCompeleted)
		sf.GracefulStop()
	}()

	// wait until the graceful shutdown is in progress and new RPCs are no longer accepted on the
	// external servers
	waitUntilFailure(externalConn)

	// internal sockets should still accept RPCs even if external sockets are gracefully closing.
	invokeQuick(internalConn, true)

	// block on the internal server
	internalBlockingRPCFinished := invokeBlocking(internalConn)
	<-internalIsBlocking

	// release the external server's blocking RPC so the graceful shutdown can complete and proceed to
	// shutting down the internal servers.
	close(releaseExternalBlock)

	// wait until the graceful shutdown is in progress and new RPCs are no longer accepted on the internal
	// servers
	waitUntilFailure(internalConn)

	// neither internal nor external servers should be accepting new RPCs anymore
	invokeQuick(externalConn, false)
	invokeQuick(internalConn, false)

	// wait until the blocking rpc has successfully completed
	close(releaseInternalBlock)
	<-internalBlockingRPCFinished

	// wait until the graceful shutdown completes
	<-shutdownCompeleted
}
