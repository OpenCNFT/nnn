package requestinfohandler

import (
	"context"
	"strings"

	grpcmwtags "github.com/grpc-ecosystem/go-grpc-middleware/tags"
	grpcprometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	gitalyauth "gitlab.com/gitlab-org/gitaly/v16/auth"
	"gitlab.com/gitlab-org/gitaly/v16/internal/grpc/protoregistry"
	"gitlab.com/gitlab-org/gitaly/v16/internal/structerr"
	"gitlab.com/gitlab-org/labkit/correlation"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

var requests = promauto.NewCounterVec(
	prometheus.CounterOpts{
		Name: "gitaly_service_client_requests_total",
		Help: "Counter of client requests received by client, call_site, auth version, response code and deadline_type",
	},
	[]string{
		"client_name",
		"grpc_service",
		"grpc_method",
		"call_site",
		"auth_version",
		"grpc_code",
		"deadline_type",
		"method_operation",
		"method_scope",
	},
)

type requestInfo struct {
	clientName      string
	callSite        string
	authVersion     string
	deadlineType    string
	methodOperation string
	methodScope     string
}

// CallSiteKey is the key used in ctx_tags to store the client feature
const CallSiteKey = "grpc.meta.call_site"

// ClientNameKey is the key used in ctx_tags to store the client name
const ClientNameKey = "grpc.meta.client_name"

// AuthVersionKey is the key used in ctx_tags to store the auth version
const AuthVersionKey = "grpc.meta.auth_version"

// DeadlineTypeKey is the key used in ctx_tags to store the deadline type
const DeadlineTypeKey = "grpc.meta.deadline_type"

// MethodTypeKey is one of "unary", "client_stream", "server_stream", "bidi_stream"
const MethodTypeKey = "grpc.meta.method_type"

// MethodOperationKey is one of "mutator", "accessor" or "maintenance" and corresponds to the `MethodOptions`
// extension.
const MethodOperationKey = "grpc.meta.method_operation"

// MethodScopeKey is one of "repository" or "storage" and corresponds to the `MethodOptions` extension.
const MethodScopeKey = "grpc.meta.method_scope"

// RemoteIPKey is the key used in ctx_tags to store the remote_ip
const RemoteIPKey = "remote_ip"

// UserIDKey is the key used in ctx_tags to store the user_id
const UserIDKey = "user_id"

// UsernameKey is the key used in ctx_tags to store the username
const UsernameKey = "username"

// Unknown client and feature. Matches the prometheus grpc unknown value
const unknownValue = "unknown"

func getFromMD(md metadata.MD, header string) string {
	values := md[header]
	if len(values) != 1 {
		return ""
	}

	return values[0]
}

// newRequestInfo extracts metadata from the connection headers and add it to the
// ctx_tags, if it is set. Returns values appropriate for use with prometheus labels,
// using `unknown` if a value is not set
func newRequestInfo(ctx context.Context, fullMethod, grpcMethodType string) requestInfo {
	info := requestInfo{
		clientName:      unknownValue,
		callSite:        unknownValue,
		authVersion:     unknownValue,
		deadlineType:    unknownValue,
		methodOperation: unknownValue,
		methodScope:     unknownValue,
	}

	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return info
	}

	tags := grpcmwtags.Extract(ctx)

	if methodInfo, err := protoregistry.GitalyProtoPreregistered.LookupMethod(fullMethod); err == nil {
		var operation string
		switch methodInfo.Operation {
		case protoregistry.OpAccessor:
			operation = "accessor"
		case protoregistry.OpMutator:
			operation = "mutator"
		case protoregistry.OpMaintenance:
			operation = "maintenance"
		default:
			operation = unknownValue
		}

		info.methodOperation = operation
		tags.Set(MethodOperationKey, operation)

		var scope string
		switch methodInfo.Scope {
		case protoregistry.ScopeRepository:
			scope = "repository"
		case protoregistry.ScopeStorage:
			scope = "storage"
		default:
			scope = unknownValue
		}

		info.methodScope = scope
		tags.Set(MethodScopeKey, scope)
	}

	metadata := getFromMD(md, "call_site")
	if metadata != "" {
		info.callSite = metadata
		tags.Set(CallSiteKey, metadata)
	}

	metadata = getFromMD(md, "deadline_type")
	_, deadlineSet := ctx.Deadline()
	if !deadlineSet {
		info.deadlineType = "none"
	} else if metadata != "" {
		info.deadlineType = metadata
	}

	clientName := correlation.ExtractClientNameFromContext(ctx)
	if clientName != "" {
		info.clientName = clientName
		tags.Set(ClientNameKey, clientName)
	} else {
		metadata = getFromMD(md, "client_name")
		if metadata != "" {
			info.clientName = metadata
			tags.Set(ClientNameKey, metadata)
		}
	}

	// Set the deadline and method types in the logs
	tags.Set(DeadlineTypeKey, info.deadlineType)
	tags.Set(MethodTypeKey, grpcMethodType)

	authInfo, _ := gitalyauth.ExtractAuthInfo(ctx)
	if authInfo != nil {
		info.authVersion = authInfo.Version
		tags.Set(AuthVersionKey, authInfo.Version)
	}

	metadata = getFromMD(md, "remote_ip")
	if metadata != "" {
		tags.Set(RemoteIPKey, metadata)
	}

	metadata = getFromMD(md, "user_id")
	if metadata != "" {
		tags.Set(UserIDKey, metadata)
	}

	metadata = getFromMD(md, "username")
	if metadata != "" {
		tags.Set(UsernameKey, metadata)
	}

	// This is a stop-gap approach to logging correlation_ids
	correlationID := correlation.ExtractFromContext(ctx)
	if correlationID != "" {
		tags.Set(correlation.FieldName, correlationID)
	}

	return info
}

func extractServiceAndMethodName(fullMethodName string) (string, string) {
	fullMethodName = strings.TrimPrefix(fullMethodName, "/") // remove leading slash
	service, method, ok := strings.Cut(fullMethodName, "/")
	if !ok {
		return unknownValue, unknownValue
	}
	return service, method
}

func streamRPCType(info *grpc.StreamServerInfo) string {
	if info.IsClientStream && !info.IsServerStream {
		return "client_stream"
	} else if !info.IsClientStream && info.IsServerStream {
		return "server_stream"
	}
	return "bidi_stream"
}

func reportWithPrometheusLabels(info requestInfo, fullMethod string, err error) {
	grpcCode := structerr.GRPCCode(err)
	serviceName, methodName := extractServiceAndMethodName(fullMethod)

	requests.WithLabelValues(
		info.clientName,   // client_name
		serviceName,       // grpc_service
		methodName,        // grpc_method
		info.callSite,     // call_site
		info.authVersion,  // auth_version
		grpcCode.String(), // grpc_code
		info.deadlineType, // deadline_type
		info.methodOperation,
		info.methodScope,
	).Inc()
	grpcprometheus.WithConstLabels(prometheus.Labels{"deadline_type": info.deadlineType})
}

// UnaryInterceptor returns a Unary Interceptor
func UnaryInterceptor(ctx context.Context, req interface{}, serverInfo *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	info := newRequestInfo(ctx, serverInfo.FullMethod, "unary")

	res, err := handler(ctx, req)

	reportWithPrometheusLabels(info, serverInfo.FullMethod, err)

	return res, err
}

// StreamInterceptor returns a Stream Interceptor
func StreamInterceptor(srv interface{}, stream grpc.ServerStream, serverInfo *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	ctx := stream.Context()
	info := newRequestInfo(ctx, serverInfo.FullMethod, streamRPCType(serverInfo))

	err := handler(srv, stream)

	reportWithPrometheusLabels(info, serverInfo.FullMethod, err)

	return err
}
