// Code generated by protoc-gen-go-grpc. DO NOT EDIT.
// versions:
// - protoc-gen-go-grpc v1.5.1
// - protoc             v4.23.1
// source: diff.proto

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
	DiffService_CommitDiff_FullMethodName       = "/gitaly.DiffService/CommitDiff"
	DiffService_CommitDelta_FullMethodName      = "/gitaly.DiffService/CommitDelta"
	DiffService_RawDiff_FullMethodName          = "/gitaly.DiffService/RawDiff"
	DiffService_RawPatch_FullMethodName         = "/gitaly.DiffService/RawPatch"
	DiffService_DiffStats_FullMethodName        = "/gitaly.DiffService/DiffStats"
	DiffService_FindChangedPaths_FullMethodName = "/gitaly.DiffService/FindChangedPaths"
	DiffService_GetPatchID_FullMethodName       = "/gitaly.DiffService/GetPatchID"
	DiffService_RawRangeDiff_FullMethodName     = "/gitaly.DiffService/RawRangeDiff"
	DiffService_RangeDiff_FullMethodName        = "/gitaly.DiffService/RangeDiff"
	DiffService_DiffBlobs_FullMethodName        = "/gitaly.DiffService/DiffBlobs"
)

// DiffServiceClient is the client API for DiffService service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://pkg.go.dev/google.golang.org/grpc/?tab=doc#ClientConn.NewStream.
//
// DiffService is a service which provides RPCs to inspect differences
// introduced between a set of commits.
type DiffServiceClient interface {
	// CommitDiff returns a diff between two different commits. The patch data is
	// chunked across messages and get streamed back to the client.
	CommitDiff(ctx context.Context, in *CommitDiffRequest, opts ...grpc.CallOption) (grpc.ServerStreamingClient[CommitDiffResponse], error)
	// CommitDelta returns the deltas between two different commits. A delta
	// includes everything that changed about a set of paths except for the actual
	// diff.
	CommitDelta(ctx context.Context, in *CommitDeltaRequest, opts ...grpc.CallOption) (grpc.ServerStreamingClient[CommitDeltaResponse], error)
	// RawDiff returns a diff between two commits. The output is the unmodified
	// output from git-diff(1). This is not to be confused with git-diff(1)'s
	// --raw mode.
	RawDiff(ctx context.Context, in *RawDiffRequest, opts ...grpc.CallOption) (grpc.ServerStreamingClient[RawDiffResponse], error)
	// RawPatch returns a diff between two commits in a formatted patch.The output
	// is the unmodified output from git-format-patch(1). This is not to be confused with
	// git-diff(1)'s --raw mode.
	RawPatch(ctx context.Context, in *RawPatchRequest, opts ...grpc.CallOption) (grpc.ServerStreamingClient[RawPatchResponse], error)
	// DiffStats returns the diff stats between two commits such as number of lines
	// changed, etc.
	DiffStats(ctx context.Context, in *DiffStatsRequest, opts ...grpc.CallOption) (grpc.ServerStreamingClient[DiffStatsResponse], error)
	// FindChangedPaths returns a list of files changed along with the status of each file
	FindChangedPaths(ctx context.Context, in *FindChangedPathsRequest, opts ...grpc.CallOption) (grpc.ServerStreamingClient[FindChangedPathsResponse], error)
	GetPatchID(ctx context.Context, in *GetPatchIDRequest, opts ...grpc.CallOption) (*GetPatchIDResponse, error)
	// RawRangeDiff outputs the raw range diff data for a given range specification.
	RawRangeDiff(ctx context.Context, in *RawRangeDiffRequest, opts ...grpc.CallOption) (grpc.ServerStreamingClient[RawRangeDiffResponse], error)
	// RangeDiff outputs the parsed commit pairs from range diff for a given range specification.
	RangeDiff(ctx context.Context, in *RangeDiffRequest, opts ...grpc.CallOption) (grpc.ServerStreamingClient[RangeDiffResponse], error)
	// DiffBlobs computes diffs between pairs of blobs. A batch of blob pairs is sent to the server.
	// The resulting patches are then chucked across response messages and streamed to the client.
	DiffBlobs(ctx context.Context, in *DiffBlobsRequest, opts ...grpc.CallOption) (grpc.ServerStreamingClient[DiffBlobsResponse], error)
}

type diffServiceClient struct {
	cc grpc.ClientConnInterface
}

func NewDiffServiceClient(cc grpc.ClientConnInterface) DiffServiceClient {
	return &diffServiceClient{cc}
}

func (c *diffServiceClient) CommitDiff(ctx context.Context, in *CommitDiffRequest, opts ...grpc.CallOption) (grpc.ServerStreamingClient[CommitDiffResponse], error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	stream, err := c.cc.NewStream(ctx, &DiffService_ServiceDesc.Streams[0], DiffService_CommitDiff_FullMethodName, cOpts...)
	if err != nil {
		return nil, err
	}
	x := &grpc.GenericClientStream[CommitDiffRequest, CommitDiffResponse]{ClientStream: stream}
	if err := x.ClientStream.SendMsg(in); err != nil {
		return nil, err
	}
	if err := x.ClientStream.CloseSend(); err != nil {
		return nil, err
	}
	return x, nil
}

// This type alias is provided for backwards compatibility with existing code that references the prior non-generic stream type by name.
type DiffService_CommitDiffClient = grpc.ServerStreamingClient[CommitDiffResponse]

func (c *diffServiceClient) CommitDelta(ctx context.Context, in *CommitDeltaRequest, opts ...grpc.CallOption) (grpc.ServerStreamingClient[CommitDeltaResponse], error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	stream, err := c.cc.NewStream(ctx, &DiffService_ServiceDesc.Streams[1], DiffService_CommitDelta_FullMethodName, cOpts...)
	if err != nil {
		return nil, err
	}
	x := &grpc.GenericClientStream[CommitDeltaRequest, CommitDeltaResponse]{ClientStream: stream}
	if err := x.ClientStream.SendMsg(in); err != nil {
		return nil, err
	}
	if err := x.ClientStream.CloseSend(); err != nil {
		return nil, err
	}
	return x, nil
}

// This type alias is provided for backwards compatibility with existing code that references the prior non-generic stream type by name.
type DiffService_CommitDeltaClient = grpc.ServerStreamingClient[CommitDeltaResponse]

func (c *diffServiceClient) RawDiff(ctx context.Context, in *RawDiffRequest, opts ...grpc.CallOption) (grpc.ServerStreamingClient[RawDiffResponse], error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	stream, err := c.cc.NewStream(ctx, &DiffService_ServiceDesc.Streams[2], DiffService_RawDiff_FullMethodName, cOpts...)
	if err != nil {
		return nil, err
	}
	x := &grpc.GenericClientStream[RawDiffRequest, RawDiffResponse]{ClientStream: stream}
	if err := x.ClientStream.SendMsg(in); err != nil {
		return nil, err
	}
	if err := x.ClientStream.CloseSend(); err != nil {
		return nil, err
	}
	return x, nil
}

// This type alias is provided for backwards compatibility with existing code that references the prior non-generic stream type by name.
type DiffService_RawDiffClient = grpc.ServerStreamingClient[RawDiffResponse]

func (c *diffServiceClient) RawPatch(ctx context.Context, in *RawPatchRequest, opts ...grpc.CallOption) (grpc.ServerStreamingClient[RawPatchResponse], error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	stream, err := c.cc.NewStream(ctx, &DiffService_ServiceDesc.Streams[3], DiffService_RawPatch_FullMethodName, cOpts...)
	if err != nil {
		return nil, err
	}
	x := &grpc.GenericClientStream[RawPatchRequest, RawPatchResponse]{ClientStream: stream}
	if err := x.ClientStream.SendMsg(in); err != nil {
		return nil, err
	}
	if err := x.ClientStream.CloseSend(); err != nil {
		return nil, err
	}
	return x, nil
}

// This type alias is provided for backwards compatibility with existing code that references the prior non-generic stream type by name.
type DiffService_RawPatchClient = grpc.ServerStreamingClient[RawPatchResponse]

func (c *diffServiceClient) DiffStats(ctx context.Context, in *DiffStatsRequest, opts ...grpc.CallOption) (grpc.ServerStreamingClient[DiffStatsResponse], error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	stream, err := c.cc.NewStream(ctx, &DiffService_ServiceDesc.Streams[4], DiffService_DiffStats_FullMethodName, cOpts...)
	if err != nil {
		return nil, err
	}
	x := &grpc.GenericClientStream[DiffStatsRequest, DiffStatsResponse]{ClientStream: stream}
	if err := x.ClientStream.SendMsg(in); err != nil {
		return nil, err
	}
	if err := x.ClientStream.CloseSend(); err != nil {
		return nil, err
	}
	return x, nil
}

// This type alias is provided for backwards compatibility with existing code that references the prior non-generic stream type by name.
type DiffService_DiffStatsClient = grpc.ServerStreamingClient[DiffStatsResponse]

func (c *diffServiceClient) FindChangedPaths(ctx context.Context, in *FindChangedPathsRequest, opts ...grpc.CallOption) (grpc.ServerStreamingClient[FindChangedPathsResponse], error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	stream, err := c.cc.NewStream(ctx, &DiffService_ServiceDesc.Streams[5], DiffService_FindChangedPaths_FullMethodName, cOpts...)
	if err != nil {
		return nil, err
	}
	x := &grpc.GenericClientStream[FindChangedPathsRequest, FindChangedPathsResponse]{ClientStream: stream}
	if err := x.ClientStream.SendMsg(in); err != nil {
		return nil, err
	}
	if err := x.ClientStream.CloseSend(); err != nil {
		return nil, err
	}
	return x, nil
}

// This type alias is provided for backwards compatibility with existing code that references the prior non-generic stream type by name.
type DiffService_FindChangedPathsClient = grpc.ServerStreamingClient[FindChangedPathsResponse]

func (c *diffServiceClient) GetPatchID(ctx context.Context, in *GetPatchIDRequest, opts ...grpc.CallOption) (*GetPatchIDResponse, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(GetPatchIDResponse)
	err := c.cc.Invoke(ctx, DiffService_GetPatchID_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *diffServiceClient) RawRangeDiff(ctx context.Context, in *RawRangeDiffRequest, opts ...grpc.CallOption) (grpc.ServerStreamingClient[RawRangeDiffResponse], error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	stream, err := c.cc.NewStream(ctx, &DiffService_ServiceDesc.Streams[6], DiffService_RawRangeDiff_FullMethodName, cOpts...)
	if err != nil {
		return nil, err
	}
	x := &grpc.GenericClientStream[RawRangeDiffRequest, RawRangeDiffResponse]{ClientStream: stream}
	if err := x.ClientStream.SendMsg(in); err != nil {
		return nil, err
	}
	if err := x.ClientStream.CloseSend(); err != nil {
		return nil, err
	}
	return x, nil
}

// This type alias is provided for backwards compatibility with existing code that references the prior non-generic stream type by name.
type DiffService_RawRangeDiffClient = grpc.ServerStreamingClient[RawRangeDiffResponse]

func (c *diffServiceClient) RangeDiff(ctx context.Context, in *RangeDiffRequest, opts ...grpc.CallOption) (grpc.ServerStreamingClient[RangeDiffResponse], error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	stream, err := c.cc.NewStream(ctx, &DiffService_ServiceDesc.Streams[7], DiffService_RangeDiff_FullMethodName, cOpts...)
	if err != nil {
		return nil, err
	}
	x := &grpc.GenericClientStream[RangeDiffRequest, RangeDiffResponse]{ClientStream: stream}
	if err := x.ClientStream.SendMsg(in); err != nil {
		return nil, err
	}
	if err := x.ClientStream.CloseSend(); err != nil {
		return nil, err
	}
	return x, nil
}

// This type alias is provided for backwards compatibility with existing code that references the prior non-generic stream type by name.
type DiffService_RangeDiffClient = grpc.ServerStreamingClient[RangeDiffResponse]

func (c *diffServiceClient) DiffBlobs(ctx context.Context, in *DiffBlobsRequest, opts ...grpc.CallOption) (grpc.ServerStreamingClient[DiffBlobsResponse], error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	stream, err := c.cc.NewStream(ctx, &DiffService_ServiceDesc.Streams[8], DiffService_DiffBlobs_FullMethodName, cOpts...)
	if err != nil {
		return nil, err
	}
	x := &grpc.GenericClientStream[DiffBlobsRequest, DiffBlobsResponse]{ClientStream: stream}
	if err := x.ClientStream.SendMsg(in); err != nil {
		return nil, err
	}
	if err := x.ClientStream.CloseSend(); err != nil {
		return nil, err
	}
	return x, nil
}

// This type alias is provided for backwards compatibility with existing code that references the prior non-generic stream type by name.
type DiffService_DiffBlobsClient = grpc.ServerStreamingClient[DiffBlobsResponse]

// DiffServiceServer is the server API for DiffService service.
// All implementations must embed UnimplementedDiffServiceServer
// for forward compatibility.
//
// DiffService is a service which provides RPCs to inspect differences
// introduced between a set of commits.
type DiffServiceServer interface {
	// CommitDiff returns a diff between two different commits. The patch data is
	// chunked across messages and get streamed back to the client.
	CommitDiff(*CommitDiffRequest, grpc.ServerStreamingServer[CommitDiffResponse]) error
	// CommitDelta returns the deltas between two different commits. A delta
	// includes everything that changed about a set of paths except for the actual
	// diff.
	CommitDelta(*CommitDeltaRequest, grpc.ServerStreamingServer[CommitDeltaResponse]) error
	// RawDiff returns a diff between two commits. The output is the unmodified
	// output from git-diff(1). This is not to be confused with git-diff(1)'s
	// --raw mode.
	RawDiff(*RawDiffRequest, grpc.ServerStreamingServer[RawDiffResponse]) error
	// RawPatch returns a diff between two commits in a formatted patch.The output
	// is the unmodified output from git-format-patch(1). This is not to be confused with
	// git-diff(1)'s --raw mode.
	RawPatch(*RawPatchRequest, grpc.ServerStreamingServer[RawPatchResponse]) error
	// DiffStats returns the diff stats between two commits such as number of lines
	// changed, etc.
	DiffStats(*DiffStatsRequest, grpc.ServerStreamingServer[DiffStatsResponse]) error
	// FindChangedPaths returns a list of files changed along with the status of each file
	FindChangedPaths(*FindChangedPathsRequest, grpc.ServerStreamingServer[FindChangedPathsResponse]) error
	GetPatchID(context.Context, *GetPatchIDRequest) (*GetPatchIDResponse, error)
	// RawRangeDiff outputs the raw range diff data for a given range specification.
	RawRangeDiff(*RawRangeDiffRequest, grpc.ServerStreamingServer[RawRangeDiffResponse]) error
	// RangeDiff outputs the parsed commit pairs from range diff for a given range specification.
	RangeDiff(*RangeDiffRequest, grpc.ServerStreamingServer[RangeDiffResponse]) error
	// DiffBlobs computes diffs between pairs of blobs. A batch of blob pairs is sent to the server.
	// The resulting patches are then chucked across response messages and streamed to the client.
	DiffBlobs(*DiffBlobsRequest, grpc.ServerStreamingServer[DiffBlobsResponse]) error
	mustEmbedUnimplementedDiffServiceServer()
}

// UnimplementedDiffServiceServer must be embedded to have
// forward compatible implementations.
//
// NOTE: this should be embedded by value instead of pointer to avoid a nil
// pointer dereference when methods are called.
type UnimplementedDiffServiceServer struct{}

func (UnimplementedDiffServiceServer) CommitDiff(*CommitDiffRequest, grpc.ServerStreamingServer[CommitDiffResponse]) error {
	return status.Errorf(codes.Unimplemented, "method CommitDiff not implemented")
}
func (UnimplementedDiffServiceServer) CommitDelta(*CommitDeltaRequest, grpc.ServerStreamingServer[CommitDeltaResponse]) error {
	return status.Errorf(codes.Unimplemented, "method CommitDelta not implemented")
}
func (UnimplementedDiffServiceServer) RawDiff(*RawDiffRequest, grpc.ServerStreamingServer[RawDiffResponse]) error {
	return status.Errorf(codes.Unimplemented, "method RawDiff not implemented")
}
func (UnimplementedDiffServiceServer) RawPatch(*RawPatchRequest, grpc.ServerStreamingServer[RawPatchResponse]) error {
	return status.Errorf(codes.Unimplemented, "method RawPatch not implemented")
}
func (UnimplementedDiffServiceServer) DiffStats(*DiffStatsRequest, grpc.ServerStreamingServer[DiffStatsResponse]) error {
	return status.Errorf(codes.Unimplemented, "method DiffStats not implemented")
}
func (UnimplementedDiffServiceServer) FindChangedPaths(*FindChangedPathsRequest, grpc.ServerStreamingServer[FindChangedPathsResponse]) error {
	return status.Errorf(codes.Unimplemented, "method FindChangedPaths not implemented")
}
func (UnimplementedDiffServiceServer) GetPatchID(context.Context, *GetPatchIDRequest) (*GetPatchIDResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method GetPatchID not implemented")
}
func (UnimplementedDiffServiceServer) RawRangeDiff(*RawRangeDiffRequest, grpc.ServerStreamingServer[RawRangeDiffResponse]) error {
	return status.Errorf(codes.Unimplemented, "method RawRangeDiff not implemented")
}
func (UnimplementedDiffServiceServer) RangeDiff(*RangeDiffRequest, grpc.ServerStreamingServer[RangeDiffResponse]) error {
	return status.Errorf(codes.Unimplemented, "method RangeDiff not implemented")
}
func (UnimplementedDiffServiceServer) DiffBlobs(*DiffBlobsRequest, grpc.ServerStreamingServer[DiffBlobsResponse]) error {
	return status.Errorf(codes.Unimplemented, "method DiffBlobs not implemented")
}
func (UnimplementedDiffServiceServer) mustEmbedUnimplementedDiffServiceServer() {}
func (UnimplementedDiffServiceServer) testEmbeddedByValue()                     {}

// UnsafeDiffServiceServer may be embedded to opt out of forward compatibility for this service.
// Use of this interface is not recommended, as added methods to DiffServiceServer will
// result in compilation errors.
type UnsafeDiffServiceServer interface {
	mustEmbedUnimplementedDiffServiceServer()
}

func RegisterDiffServiceServer(s grpc.ServiceRegistrar, srv DiffServiceServer) {
	// If the following call pancis, it indicates UnimplementedDiffServiceServer was
	// embedded by pointer and is nil.  This will cause panics if an
	// unimplemented method is ever invoked, so we test this at initialization
	// time to prevent it from happening at runtime later due to I/O.
	if t, ok := srv.(interface{ testEmbeddedByValue() }); ok {
		t.testEmbeddedByValue()
	}
	s.RegisterService(&DiffService_ServiceDesc, srv)
}

func _DiffService_CommitDiff_Handler(srv interface{}, stream grpc.ServerStream) error {
	m := new(CommitDiffRequest)
	if err := stream.RecvMsg(m); err != nil {
		return err
	}
	return srv.(DiffServiceServer).CommitDiff(m, &grpc.GenericServerStream[CommitDiffRequest, CommitDiffResponse]{ServerStream: stream})
}

// This type alias is provided for backwards compatibility with existing code that references the prior non-generic stream type by name.
type DiffService_CommitDiffServer = grpc.ServerStreamingServer[CommitDiffResponse]

func _DiffService_CommitDelta_Handler(srv interface{}, stream grpc.ServerStream) error {
	m := new(CommitDeltaRequest)
	if err := stream.RecvMsg(m); err != nil {
		return err
	}
	return srv.(DiffServiceServer).CommitDelta(m, &grpc.GenericServerStream[CommitDeltaRequest, CommitDeltaResponse]{ServerStream: stream})
}

// This type alias is provided for backwards compatibility with existing code that references the prior non-generic stream type by name.
type DiffService_CommitDeltaServer = grpc.ServerStreamingServer[CommitDeltaResponse]

func _DiffService_RawDiff_Handler(srv interface{}, stream grpc.ServerStream) error {
	m := new(RawDiffRequest)
	if err := stream.RecvMsg(m); err != nil {
		return err
	}
	return srv.(DiffServiceServer).RawDiff(m, &grpc.GenericServerStream[RawDiffRequest, RawDiffResponse]{ServerStream: stream})
}

// This type alias is provided for backwards compatibility with existing code that references the prior non-generic stream type by name.
type DiffService_RawDiffServer = grpc.ServerStreamingServer[RawDiffResponse]

func _DiffService_RawPatch_Handler(srv interface{}, stream grpc.ServerStream) error {
	m := new(RawPatchRequest)
	if err := stream.RecvMsg(m); err != nil {
		return err
	}
	return srv.(DiffServiceServer).RawPatch(m, &grpc.GenericServerStream[RawPatchRequest, RawPatchResponse]{ServerStream: stream})
}

// This type alias is provided for backwards compatibility with existing code that references the prior non-generic stream type by name.
type DiffService_RawPatchServer = grpc.ServerStreamingServer[RawPatchResponse]

func _DiffService_DiffStats_Handler(srv interface{}, stream grpc.ServerStream) error {
	m := new(DiffStatsRequest)
	if err := stream.RecvMsg(m); err != nil {
		return err
	}
	return srv.(DiffServiceServer).DiffStats(m, &grpc.GenericServerStream[DiffStatsRequest, DiffStatsResponse]{ServerStream: stream})
}

// This type alias is provided for backwards compatibility with existing code that references the prior non-generic stream type by name.
type DiffService_DiffStatsServer = grpc.ServerStreamingServer[DiffStatsResponse]

func _DiffService_FindChangedPaths_Handler(srv interface{}, stream grpc.ServerStream) error {
	m := new(FindChangedPathsRequest)
	if err := stream.RecvMsg(m); err != nil {
		return err
	}
	return srv.(DiffServiceServer).FindChangedPaths(m, &grpc.GenericServerStream[FindChangedPathsRequest, FindChangedPathsResponse]{ServerStream: stream})
}

// This type alias is provided for backwards compatibility with existing code that references the prior non-generic stream type by name.
type DiffService_FindChangedPathsServer = grpc.ServerStreamingServer[FindChangedPathsResponse]

func _DiffService_GetPatchID_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(GetPatchIDRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(DiffServiceServer).GetPatchID(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: DiffService_GetPatchID_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(DiffServiceServer).GetPatchID(ctx, req.(*GetPatchIDRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _DiffService_RawRangeDiff_Handler(srv interface{}, stream grpc.ServerStream) error {
	m := new(RawRangeDiffRequest)
	if err := stream.RecvMsg(m); err != nil {
		return err
	}
	return srv.(DiffServiceServer).RawRangeDiff(m, &grpc.GenericServerStream[RawRangeDiffRequest, RawRangeDiffResponse]{ServerStream: stream})
}

// This type alias is provided for backwards compatibility with existing code that references the prior non-generic stream type by name.
type DiffService_RawRangeDiffServer = grpc.ServerStreamingServer[RawRangeDiffResponse]

func _DiffService_RangeDiff_Handler(srv interface{}, stream grpc.ServerStream) error {
	m := new(RangeDiffRequest)
	if err := stream.RecvMsg(m); err != nil {
		return err
	}
	return srv.(DiffServiceServer).RangeDiff(m, &grpc.GenericServerStream[RangeDiffRequest, RangeDiffResponse]{ServerStream: stream})
}

// This type alias is provided for backwards compatibility with existing code that references the prior non-generic stream type by name.
type DiffService_RangeDiffServer = grpc.ServerStreamingServer[RangeDiffResponse]

func _DiffService_DiffBlobs_Handler(srv interface{}, stream grpc.ServerStream) error {
	m := new(DiffBlobsRequest)
	if err := stream.RecvMsg(m); err != nil {
		return err
	}
	return srv.(DiffServiceServer).DiffBlobs(m, &grpc.GenericServerStream[DiffBlobsRequest, DiffBlobsResponse]{ServerStream: stream})
}

// This type alias is provided for backwards compatibility with existing code that references the prior non-generic stream type by name.
type DiffService_DiffBlobsServer = grpc.ServerStreamingServer[DiffBlobsResponse]

// DiffService_ServiceDesc is the grpc.ServiceDesc for DiffService service.
// It's only intended for direct use with grpc.RegisterService,
// and not to be introspected or modified (even as a copy)
var DiffService_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "gitaly.DiffService",
	HandlerType: (*DiffServiceServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "GetPatchID",
			Handler:    _DiffService_GetPatchID_Handler,
		},
	},
	Streams: []grpc.StreamDesc{
		{
			StreamName:    "CommitDiff",
			Handler:       _DiffService_CommitDiff_Handler,
			ServerStreams: true,
		},
		{
			StreamName:    "CommitDelta",
			Handler:       _DiffService_CommitDelta_Handler,
			ServerStreams: true,
		},
		{
			StreamName:    "RawDiff",
			Handler:       _DiffService_RawDiff_Handler,
			ServerStreams: true,
		},
		{
			StreamName:    "RawPatch",
			Handler:       _DiffService_RawPatch_Handler,
			ServerStreams: true,
		},
		{
			StreamName:    "DiffStats",
			Handler:       _DiffService_DiffStats_Handler,
			ServerStreams: true,
		},
		{
			StreamName:    "FindChangedPaths",
			Handler:       _DiffService_FindChangedPaths_Handler,
			ServerStreams: true,
		},
		{
			StreamName:    "RawRangeDiff",
			Handler:       _DiffService_RawRangeDiff_Handler,
			ServerStreams: true,
		},
		{
			StreamName:    "RangeDiff",
			Handler:       _DiffService_RangeDiff_Handler,
			ServerStreams: true,
		},
		{
			StreamName:    "DiffBlobs",
			Handler:       _DiffService_DiffBlobs_Handler,
			ServerStreams: true,
		},
	},
	Metadata: "diff.proto",
}
