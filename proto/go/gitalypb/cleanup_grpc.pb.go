// Code generated by protoc-gen-go-grpc. DO NOT EDIT.
// versions:
// - protoc-gen-go-grpc v1.5.1
// - protoc             v4.23.1
// source: cleanup.proto

package gitalypb

import (
	context "context"
	grpc "google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
)

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
// Requires gRPC-Go v1.64.0 or later.
const _ = grpc.SupportPackageIsVersion9

const (
	CleanupService_ApplyBfgObjectMapStream_FullMethodName = "/gitaly.CleanupService/ApplyBfgObjectMapStream"
	CleanupService_RewriteHistory_FullMethodName          = "/gitaly.CleanupService/RewriteHistory"
)

// CleanupServiceClient is the client API for CleanupService service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://pkg.go.dev/google.golang.org/grpc/?tab=doc#ClientConn.NewStream.
//
// CleanupService provides RPCs to clean up a repository's contents.
type CleanupServiceClient interface {
	// ApplyBfgObjectMapStream ...
	ApplyBfgObjectMapStream(ctx context.Context, opts ...grpc.CallOption) (grpc.BidiStreamingClient[ApplyBfgObjectMapStreamRequest, ApplyBfgObjectMapStreamResponse], error)
	// RewriteHistory redacts targeted strings and deletes requested blobs in a
	// repository and updates all references to point to the rewritten commit
	// history. This is useful for removing inadvertently pushed secrets from your
	// repository and purging large blobs. This is a dangerous operation.
	//
	// The following known error conditions may happen:
	//
	// - `InvalidArgument` in the following situations:
	//   - The provided repository can't be validated.
	//   - The repository field is set on any request other than the initial one.
	//   - All of the client requests do not contain either blobs to remove or
	//     redaction patterns to redact.
	//   - A blob object ID is invalid.
	//   - A redaction pattern contains a newline character.
	//
	// - `Aborted` if the repository is mutated while this RPC is executing.
	RewriteHistory(ctx context.Context, opts ...grpc.CallOption) (grpc.ClientStreamingClient[RewriteHistoryRequest, RewriteHistoryResponse], error)
}

type cleanupServiceClient struct {
	cc grpc.ClientConnInterface
}

func NewCleanupServiceClient(cc grpc.ClientConnInterface) CleanupServiceClient {
	return &cleanupServiceClient{cc}
}

func (c *cleanupServiceClient) ApplyBfgObjectMapStream(ctx context.Context, opts ...grpc.CallOption) (grpc.BidiStreamingClient[ApplyBfgObjectMapStreamRequest, ApplyBfgObjectMapStreamResponse], error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	stream, err := c.cc.NewStream(ctx, &CleanupService_ServiceDesc.Streams[0], CleanupService_ApplyBfgObjectMapStream_FullMethodName, cOpts...)
	if err != nil {
		return nil, err
	}
	x := &grpc.GenericClientStream[ApplyBfgObjectMapStreamRequest, ApplyBfgObjectMapStreamResponse]{ClientStream: stream}
	return x, nil
}

// This type alias is provided for backwards compatibility with existing code that references the prior non-generic stream type by name.
type CleanupService_ApplyBfgObjectMapStreamClient = grpc.BidiStreamingClient[ApplyBfgObjectMapStreamRequest, ApplyBfgObjectMapStreamResponse]

func (c *cleanupServiceClient) RewriteHistory(ctx context.Context, opts ...grpc.CallOption) (grpc.ClientStreamingClient[RewriteHistoryRequest, RewriteHistoryResponse], error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	stream, err := c.cc.NewStream(ctx, &CleanupService_ServiceDesc.Streams[1], CleanupService_RewriteHistory_FullMethodName, cOpts...)
	if err != nil {
		return nil, err
	}
	x := &grpc.GenericClientStream[RewriteHistoryRequest, RewriteHistoryResponse]{ClientStream: stream}
	return x, nil
}

// This type alias is provided for backwards compatibility with existing code that references the prior non-generic stream type by name.
type CleanupService_RewriteHistoryClient = grpc.ClientStreamingClient[RewriteHistoryRequest, RewriteHistoryResponse]

// CleanupServiceServer is the server API for CleanupService service.
// All implementations must embed UnimplementedCleanupServiceServer
// for forward compatibility.
//
// CleanupService provides RPCs to clean up a repository's contents.
type CleanupServiceServer interface {
	// ApplyBfgObjectMapStream ...
	ApplyBfgObjectMapStream(grpc.BidiStreamingServer[ApplyBfgObjectMapStreamRequest, ApplyBfgObjectMapStreamResponse]) error
	// RewriteHistory redacts targeted strings and deletes requested blobs in a
	// repository and updates all references to point to the rewritten commit
	// history. This is useful for removing inadvertently pushed secrets from your
	// repository and purging large blobs. This is a dangerous operation.
	//
	// The following known error conditions may happen:
	//
	// - `InvalidArgument` in the following situations:
	//   - The provided repository can't be validated.
	//   - The repository field is set on any request other than the initial one.
	//   - All of the client requests do not contain either blobs to remove or
	//     redaction patterns to redact.
	//   - A blob object ID is invalid.
	//   - A redaction pattern contains a newline character.
	//
	// - `Aborted` if the repository is mutated while this RPC is executing.
	RewriteHistory(grpc.ClientStreamingServer[RewriteHistoryRequest, RewriteHistoryResponse]) error
	mustEmbedUnimplementedCleanupServiceServer()
}

// UnimplementedCleanupServiceServer must be embedded to have
// forward compatible implementations.
//
// NOTE: this should be embedded by value instead of pointer to avoid a nil
// pointer dereference when methods are called.
type UnimplementedCleanupServiceServer struct{}

func (UnimplementedCleanupServiceServer) ApplyBfgObjectMapStream(grpc.BidiStreamingServer[ApplyBfgObjectMapStreamRequest, ApplyBfgObjectMapStreamResponse]) error {
	return status.Errorf(codes.Unimplemented, "method ApplyBfgObjectMapStream not implemented")
}
func (UnimplementedCleanupServiceServer) RewriteHistory(grpc.ClientStreamingServer[RewriteHistoryRequest, RewriteHistoryResponse]) error {
	return status.Errorf(codes.Unimplemented, "method RewriteHistory not implemented")
}
func (UnimplementedCleanupServiceServer) mustEmbedUnimplementedCleanupServiceServer() {}
func (UnimplementedCleanupServiceServer) testEmbeddedByValue()                        {}

// UnsafeCleanupServiceServer may be embedded to opt out of forward compatibility for this service.
// Use of this interface is not recommended, as added methods to CleanupServiceServer will
// result in compilation errors.
type UnsafeCleanupServiceServer interface {
	mustEmbedUnimplementedCleanupServiceServer()
}

func RegisterCleanupServiceServer(s grpc.ServiceRegistrar, srv CleanupServiceServer) {
	// If the following call pancis, it indicates UnimplementedCleanupServiceServer was
	// embedded by pointer and is nil.  This will cause panics if an
	// unimplemented method is ever invoked, so we test this at initialization
	// time to prevent it from happening at runtime later due to I/O.
	if t, ok := srv.(interface{ testEmbeddedByValue() }); ok {
		t.testEmbeddedByValue()
	}
	s.RegisterService(&CleanupService_ServiceDesc, srv)
}

func _CleanupService_ApplyBfgObjectMapStream_Handler(srv interface{}, stream grpc.ServerStream) error {
	return srv.(CleanupServiceServer).ApplyBfgObjectMapStream(&grpc.GenericServerStream[ApplyBfgObjectMapStreamRequest, ApplyBfgObjectMapStreamResponse]{ServerStream: stream})
}

// This type alias is provided for backwards compatibility with existing code that references the prior non-generic stream type by name.
type CleanupService_ApplyBfgObjectMapStreamServer = grpc.BidiStreamingServer[ApplyBfgObjectMapStreamRequest, ApplyBfgObjectMapStreamResponse]

func _CleanupService_RewriteHistory_Handler(srv interface{}, stream grpc.ServerStream) error {
	return srv.(CleanupServiceServer).RewriteHistory(&grpc.GenericServerStream[RewriteHistoryRequest, RewriteHistoryResponse]{ServerStream: stream})
}

// This type alias is provided for backwards compatibility with existing code that references the prior non-generic stream type by name.
type CleanupService_RewriteHistoryServer = grpc.ClientStreamingServer[RewriteHistoryRequest, RewriteHistoryResponse]

// CleanupService_ServiceDesc is the grpc.ServiceDesc for CleanupService service.
// It's only intended for direct use with grpc.RegisterService,
// and not to be introspected or modified (even as a copy)
var CleanupService_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "gitaly.CleanupService",
	HandlerType: (*CleanupServiceServer)(nil),
	Methods:     []grpc.MethodDesc{},
	Streams: []grpc.StreamDesc{
		{
			StreamName:    "ApplyBfgObjectMapStream",
			Handler:       _CleanupService_ApplyBfgObjectMapStream_Handler,
			ServerStreams: true,
			ClientStreams: true,
		},
		{
			StreamName:    "RewriteHistory",
			Handler:       _CleanupService_RewriteHistory_Handler,
			ClientStreams: true,
		},
	},
	Metadata: "cleanup.proto",
}
