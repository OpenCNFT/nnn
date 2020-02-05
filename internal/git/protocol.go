package git

import (
	"context"
	"fmt"
	"strings"

	grpc_ctxtags "github.com/grpc-ecosystem/go-grpc-middleware/tags"
	"github.com/prometheus/client_golang/prometheus"
	"gitlab.com/gitlab-org/gitaly/internal/metadata/featureflag"
)

const (
	// ProtocolV2 is the special value used by Git clients to request protocol v2
	ProtocolV2 = "version=2"
)

var (
	gitProtocolRequests = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "gitaly_git_protocol_requests_total",
			Help: "Counter of Git protocol requests",
		},
		[]string{"grpc_service", "grpc_method", "git_protocol"},
	)
)

func init() {
	prometheus.MustRegister(gitProtocolRequests)
}

// RequestWithGitProtocol holds requests that respond to GitProtocol
type RequestWithGitProtocol interface {
	GetGitProtocol() string
}

// AddGitProtocolEnv checks whether the request has Git protocol v2
// and sets this in the environment.
func AddGitProtocolEnv(ctx context.Context, req RequestWithGitProtocol, env []string) []string {
	reqIsV2 := req.GetGitProtocol() == ProtocolV2
	ffEnabled := featureflag.IsEnabled(ctx, featureflag.UseGitProtocolV2)
	protocol := "v0"

	switch {
	case reqIsV2 && ffEnabled:
		env = append(env, fmt.Sprintf("GIT_PROTOCOL=%s", ProtocolV2))
		protocol = "v2"
	case reqIsV2 && !ffEnabled:
		protocol = "v2-rejected"
	}

	service, method := methodFromContext(ctx)
	gitProtocolRequests.WithLabelValues(service, method, protocol).Inc()

	return env
}

func methodFromContext(ctx context.Context) (service string, method string) {
	tags := grpc_ctxtags.Extract(ctx)
	ctxValue := tags.Values()["grpc.request.fullMethod"]
	if ctxValue == nil {
		return "", ""
	}

	if s, ok := ctxValue.(string); ok {
		// Expect: "/foo.BarService/Qux"
		split := strings.Split(s, "/")
		if len(split) != 3 {
			return "", ""
		}

		return split[1], split[2]
	}

	return "", ""
}
