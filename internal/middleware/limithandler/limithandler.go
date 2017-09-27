package limithandler

import (
	"time"

	"github.com/grpc-ecosystem/go-grpc-middleware/tags"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

// LimiterMiddleware contains rate limiter state
type LimiterMiddleware struct {
	limiter ConcurrencyLimiter
}

var maxConcurrencyPerRepoPerRPC map[string]int64

func getRepoPath(ctx context.Context) string {
	tags := grpc_ctxtags.Extract(ctx)
	ctxValue := tags.Values()["grpc.request.repoPath"]
	if ctxValue == nil {
		return ""
	}

	s, ok := ctxValue.(string)
	if ok {
		return s
	}

	return ""
}

func getMaxConcurrency(fullMethod string, repoPath string) int64 {
	return maxConcurrencyPerRepoPerRPC[fullMethod]
}

// UnaryInterceptor returns a Unary Interceptor
func (c *LimiterMiddleware) UnaryInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		repoPath := getRepoPath(ctx)
		if repoPath == "" {
			return handler(ctx, req)
		}

		maxConcurrency := getMaxConcurrency(info.FullMethod, repoPath)
		if maxConcurrency <= 0 {
			// No concurrency limiting
			return handler(ctx, req)
		}
		start := time.Now()

		return c.limiter.Limit(ctx, repoPath, maxConcurrency, func() (interface{}, error) {
			emitRateLimitMetrics(ctx, "unary", info.FullMethod, start)

			return handler(ctx, req)
		})
	}
}

// StreamInterceptor returns a Stream Interceptor
func (c *LimiterMiddleware) StreamInterceptor() grpc.StreamServerInterceptor {
	return func(srv interface{}, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		ctx := stream.Context()

		repoPath := getRepoPath(ctx)
		if repoPath == "" {
			return handler(srv, stream)
		}

		maxConcurrency := getMaxConcurrency(info.FullMethod, repoPath)
		if maxConcurrency <= 0 {
			// No concurrency limiting
			return handler(srv, stream)
		}

		start := time.Now()

		_, err := c.limiter.Limit(ctx, repoPath, maxConcurrency, func() (interface{}, error) {
			emitStreamRateLimitMetrics(ctx, info, start)

			err := handler(srv, stream)
			return nil, err
		})

		return err
	}
}

// New creates a new rate limiter
func New() LimiterMiddleware {
	return LimiterMiddleware{limiter: NewLimiter()}
}

// SetMaxRepoConcurrency Configures the max concurrency per repo per RPC
func SetMaxRepoConcurrency(v map[string]int64) {
	maxConcurrencyPerRepoPerRPC = v
}
