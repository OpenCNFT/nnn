// Code generated by protoc-gen-go-grpc. DO NOT EDIT.
// versions:
// - protoc-gen-go-grpc v1.3.0
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
// Requires gRPC-Go v1.32.0 or later.
const _ = grpc.SupportPackageIsVersion7

const (
	DiffService_CommitDiff_FullMethodName       = "/gitaly.DiffService/CommitDiff"
	DiffService_CommitDelta_FullMethodName      = "/gitaly.DiffService/CommitDelta"
	DiffService_RawDiff_FullMethodName          = "/gitaly.DiffService/RawDiff"
	DiffService_RawPatch_FullMethodName         = "/gitaly.DiffService/RawPatch"
	DiffService_DiffStats_FullMethodName        = "/gitaly.DiffService/DiffStats"
	DiffService_FindChangedPaths_FullMethodName = "/gitaly.DiffService/FindChangedPaths"
	DiffService_GetPatchID_FullMethodName       = "/gitaly.DiffService/GetPatchID"
)

// DiffServiceClient is the client API for DiffService service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://pkg.go.dev/google.golang.org/grpc/?tab=doc#ClientConn.NewStream.
type DiffServiceClient interface {
	// CommitDiff returns a diff between two different commits. The patch data is
	// chunked across messages and get streamed back to the client.
	CommitDiff(ctx context.Context, in *CommitDiffRequest, opts ...grpc.CallOption) (DiffService_CommitDiffClient, error)
	// CommitDelta returns the deltas between two different commits. A delta
	// includes everything that changed about a set of paths except for the actual
	// diff.
	CommitDelta(ctx context.Context, in *CommitDeltaRequest, opts ...grpc.CallOption) (DiffService_CommitDeltaClient, error)
	// RawDiff returns a diff between two commits. The output is the unmodified
	// output from git-diff(1). This is not to be confused with git-diff(1)'s
	// --raw mode.
	RawDiff(ctx context.Context, in *RawDiffRequest, opts ...grpc.CallOption) (DiffService_RawDiffClient, error)
	// RawPatch returns a diff between two commits in a formatted patch.The output
	// is the unmodified output from git-format-patch(1). This is not to be confused with
	// git-diff(1)'s --raw mode.
	RawPatch(ctx context.Context, in *RawPatchRequest, opts ...grpc.CallOption) (DiffService_RawPatchClient, error)
	// DiffStats returns the diff stats between two commits such as number of lines
	// changed, etc.
	DiffStats(ctx context.Context, in *DiffStatsRequest, opts ...grpc.CallOption) (DiffService_DiffStatsClient, error)
	// FindChangedPaths returns a list of files changed along with the status of each file
	FindChangedPaths(ctx context.Context, in *FindChangedPathsRequest, opts ...grpc.CallOption) (DiffService_FindChangedPathsClient, error)
	// GetPatchID computes a patch ID for a patch. Patch IDs are a unique ID computed by hashing
	// a patch with some parameters like line numbers ignored. The patch ID can thus be used to compare
	// whether diffs make the same change. Please refer to git-patch-id(1) for further information.
	// If the difference between old and new change is empty then this RPC returns an error.
	GetPatchID(ctx context.Context, in *GetPatchIDRequest, opts ...grpc.CallOption) (*GetPatchIDResponse, error)
}

type diffServiceClient struct {
	cc grpc.ClientConnInterface
}

func NewDiffServiceClient(cc grpc.ClientConnInterface) DiffServiceClient {
	return &diffServiceClient{cc}
}

func (c *diffServiceClient) CommitDiff(ctx context.Context, in *CommitDiffRequest, opts ...grpc.CallOption) (DiffService_CommitDiffClient, error) {
	stream, err := c.cc.NewStream(ctx, &DiffService_ServiceDesc.Streams[0], DiffService_CommitDiff_FullMethodName, opts...)
	if err != nil {
		return nil, err
	}
	x := &diffServiceCommitDiffClient{stream}
	if err := x.ClientStream.SendMsg(in); err != nil {
		return nil, err
	}
	if err := x.ClientStream.CloseSend(); err != nil {
		return nil, err
	}
	return x, nil
}

type DiffService_CommitDiffClient interface {
	Recv() (*CommitDiffResponse, error)
	grpc.ClientStream
}

type diffServiceCommitDiffClient struct {
	grpc.ClientStream
}

func (x *diffServiceCommitDiffClient) Recv() (*CommitDiffResponse, error) {
	m := new(CommitDiffResponse)
	if err := x.ClientStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

func (c *diffServiceClient) CommitDelta(ctx context.Context, in *CommitDeltaRequest, opts ...grpc.CallOption) (DiffService_CommitDeltaClient, error) {
	stream, err := c.cc.NewStream(ctx, &DiffService_ServiceDesc.Streams[1], DiffService_CommitDelta_FullMethodName, opts...)
	if err != nil {
		return nil, err
	}
	x := &diffServiceCommitDeltaClient{stream}
	if err := x.ClientStream.SendMsg(in); err != nil {
		return nil, err
	}
	if err := x.ClientStream.CloseSend(); err != nil {
		return nil, err
	}
	return x, nil
}

type DiffService_CommitDeltaClient interface {
	Recv() (*CommitDeltaResponse, error)
	grpc.ClientStream
}

type diffServiceCommitDeltaClient struct {
	grpc.ClientStream
}

func (x *diffServiceCommitDeltaClient) Recv() (*CommitDeltaResponse, error) {
	m := new(CommitDeltaResponse)
	if err := x.ClientStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

func (c *diffServiceClient) RawDiff(ctx context.Context, in *RawDiffRequest, opts ...grpc.CallOption) (DiffService_RawDiffClient, error) {
	stream, err := c.cc.NewStream(ctx, &DiffService_ServiceDesc.Streams[2], DiffService_RawDiff_FullMethodName, opts...)
	if err != nil {
		return nil, err
	}
	x := &diffServiceRawDiffClient{stream}
	if err := x.ClientStream.SendMsg(in); err != nil {
		return nil, err
	}
	if err := x.ClientStream.CloseSend(); err != nil {
		return nil, err
	}
	return x, nil
}

type DiffService_RawDiffClient interface {
	Recv() (*RawDiffResponse, error)
	grpc.ClientStream
}

type diffServiceRawDiffClient struct {
	grpc.ClientStream
}

func (x *diffServiceRawDiffClient) Recv() (*RawDiffResponse, error) {
	m := new(RawDiffResponse)
	if err := x.ClientStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

func (c *diffServiceClient) RawPatch(ctx context.Context, in *RawPatchRequest, opts ...grpc.CallOption) (DiffService_RawPatchClient, error) {
	stream, err := c.cc.NewStream(ctx, &DiffService_ServiceDesc.Streams[3], DiffService_RawPatch_FullMethodName, opts...)
	if err != nil {
		return nil, err
	}
	x := &diffServiceRawPatchClient{stream}
	if err := x.ClientStream.SendMsg(in); err != nil {
		return nil, err
	}
	if err := x.ClientStream.CloseSend(); err != nil {
		return nil, err
	}
	return x, nil
}

type DiffService_RawPatchClient interface {
	Recv() (*RawPatchResponse, error)
	grpc.ClientStream
}

type diffServiceRawPatchClient struct {
	grpc.ClientStream
}

func (x *diffServiceRawPatchClient) Recv() (*RawPatchResponse, error) {
	m := new(RawPatchResponse)
	if err := x.ClientStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

func (c *diffServiceClient) DiffStats(ctx context.Context, in *DiffStatsRequest, opts ...grpc.CallOption) (DiffService_DiffStatsClient, error) {
	stream, err := c.cc.NewStream(ctx, &DiffService_ServiceDesc.Streams[4], DiffService_DiffStats_FullMethodName, opts...)
	if err != nil {
		return nil, err
	}
	x := &diffServiceDiffStatsClient{stream}
	if err := x.ClientStream.SendMsg(in); err != nil {
		return nil, err
	}
	if err := x.ClientStream.CloseSend(); err != nil {
		return nil, err
	}
	return x, nil
}

type DiffService_DiffStatsClient interface {
	Recv() (*DiffStatsResponse, error)
	grpc.ClientStream
}

type diffServiceDiffStatsClient struct {
	grpc.ClientStream
}

func (x *diffServiceDiffStatsClient) Recv() (*DiffStatsResponse, error) {
	m := new(DiffStatsResponse)
	if err := x.ClientStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

func (c *diffServiceClient) FindChangedPaths(ctx context.Context, in *FindChangedPathsRequest, opts ...grpc.CallOption) (DiffService_FindChangedPathsClient, error) {
	stream, err := c.cc.NewStream(ctx, &DiffService_ServiceDesc.Streams[5], DiffService_FindChangedPaths_FullMethodName, opts...)
	if err != nil {
		return nil, err
	}
	x := &diffServiceFindChangedPathsClient{stream}
	if err := x.ClientStream.SendMsg(in); err != nil {
		return nil, err
	}
	if err := x.ClientStream.CloseSend(); err != nil {
		return nil, err
	}
	return x, nil
}

type DiffService_FindChangedPathsClient interface {
	Recv() (*FindChangedPathsResponse, error)
	grpc.ClientStream
}

type diffServiceFindChangedPathsClient struct {
	grpc.ClientStream
}

func (x *diffServiceFindChangedPathsClient) Recv() (*FindChangedPathsResponse, error) {
	m := new(FindChangedPathsResponse)
	if err := x.ClientStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

func (c *diffServiceClient) GetPatchID(ctx context.Context, in *GetPatchIDRequest, opts ...grpc.CallOption) (*GetPatchIDResponse, error) {
	out := new(GetPatchIDResponse)
	err := c.cc.Invoke(ctx, DiffService_GetPatchID_FullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// DiffServiceServer is the server API for DiffService service.
// All implementations must embed UnimplementedDiffServiceServer
// for forward compatibility
type DiffServiceServer interface {
	// CommitDiff returns a diff between two different commits. The patch data is
	// chunked across messages and get streamed back to the client.
	CommitDiff(*CommitDiffRequest, DiffService_CommitDiffServer) error
	// CommitDelta returns the deltas between two different commits. A delta
	// includes everything that changed about a set of paths except for the actual
	// diff.
	CommitDelta(*CommitDeltaRequest, DiffService_CommitDeltaServer) error
	// RawDiff returns a diff between two commits. The output is the unmodified
	// output from git-diff(1). This is not to be confused with git-diff(1)'s
	// --raw mode.
	RawDiff(*RawDiffRequest, DiffService_RawDiffServer) error
	// RawPatch returns a diff between two commits in a formatted patch.The output
	// is the unmodified output from git-format-patch(1). This is not to be confused with
	// git-diff(1)'s --raw mode.
	RawPatch(*RawPatchRequest, DiffService_RawPatchServer) error
	// DiffStats returns the diff stats between two commits such as number of lines
	// changed, etc.
	DiffStats(*DiffStatsRequest, DiffService_DiffStatsServer) error
	// FindChangedPaths returns a list of files changed along with the status of each file
	FindChangedPaths(*FindChangedPathsRequest, DiffService_FindChangedPathsServer) error
	// GetPatchID computes a patch ID for a patch. Patch IDs are a unique ID computed by hashing
	// a patch with some parameters like line numbers ignored. The patch ID can thus be used to compare
	// whether diffs make the same change. Please refer to git-patch-id(1) for further information.
	// If the difference between old and new change is empty then this RPC returns an error.
	GetPatchID(context.Context, *GetPatchIDRequest) (*GetPatchIDResponse, error)
	mustEmbedUnimplementedDiffServiceServer()
}

// UnimplementedDiffServiceServer must be embedded to have forward compatible implementations.
type UnimplementedDiffServiceServer struct {
}

func (UnimplementedDiffServiceServer) CommitDiff(*CommitDiffRequest, DiffService_CommitDiffServer) error {
	return status.Errorf(codes.Unimplemented, "method CommitDiff not implemented")
}
func (UnimplementedDiffServiceServer) CommitDelta(*CommitDeltaRequest, DiffService_CommitDeltaServer) error {
	return status.Errorf(codes.Unimplemented, "method CommitDelta not implemented")
}
func (UnimplementedDiffServiceServer) RawDiff(*RawDiffRequest, DiffService_RawDiffServer) error {
	return status.Errorf(codes.Unimplemented, "method RawDiff not implemented")
}
func (UnimplementedDiffServiceServer) RawPatch(*RawPatchRequest, DiffService_RawPatchServer) error {
	return status.Errorf(codes.Unimplemented, "method RawPatch not implemented")
}
func (UnimplementedDiffServiceServer) DiffStats(*DiffStatsRequest, DiffService_DiffStatsServer) error {
	return status.Errorf(codes.Unimplemented, "method DiffStats not implemented")
}
func (UnimplementedDiffServiceServer) FindChangedPaths(*FindChangedPathsRequest, DiffService_FindChangedPathsServer) error {
	return status.Errorf(codes.Unimplemented, "method FindChangedPaths not implemented")
}
func (UnimplementedDiffServiceServer) GetPatchID(context.Context, *GetPatchIDRequest) (*GetPatchIDResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method GetPatchID not implemented")
}
func (UnimplementedDiffServiceServer) mustEmbedUnimplementedDiffServiceServer() {}

// UnsafeDiffServiceServer may be embedded to opt out of forward compatibility for this service.
// Use of this interface is not recommended, as added methods to DiffServiceServer will
// result in compilation errors.
type UnsafeDiffServiceServer interface {
	mustEmbedUnimplementedDiffServiceServer()
}

func RegisterDiffServiceServer(s grpc.ServiceRegistrar, srv DiffServiceServer) {
	s.RegisterService(&DiffService_ServiceDesc, srv)
}

func _DiffService_CommitDiff_Handler(srv interface{}, stream grpc.ServerStream) error {
	m := new(CommitDiffRequest)
	if err := stream.RecvMsg(m); err != nil {
		return err
	}
	return srv.(DiffServiceServer).CommitDiff(m, &diffServiceCommitDiffServer{stream})
}

type DiffService_CommitDiffServer interface {
	Send(*CommitDiffResponse) error
	grpc.ServerStream
}

type diffServiceCommitDiffServer struct {
	grpc.ServerStream
}

func (x *diffServiceCommitDiffServer) Send(m *CommitDiffResponse) error {
	return x.ServerStream.SendMsg(m)
}

func _DiffService_CommitDelta_Handler(srv interface{}, stream grpc.ServerStream) error {
	m := new(CommitDeltaRequest)
	if err := stream.RecvMsg(m); err != nil {
		return err
	}
	return srv.(DiffServiceServer).CommitDelta(m, &diffServiceCommitDeltaServer{stream})
}

type DiffService_CommitDeltaServer interface {
	Send(*CommitDeltaResponse) error
	grpc.ServerStream
}

type diffServiceCommitDeltaServer struct {
	grpc.ServerStream
}

func (x *diffServiceCommitDeltaServer) Send(m *CommitDeltaResponse) error {
	return x.ServerStream.SendMsg(m)
}

func _DiffService_RawDiff_Handler(srv interface{}, stream grpc.ServerStream) error {
	m := new(RawDiffRequest)
	if err := stream.RecvMsg(m); err != nil {
		return err
	}
	return srv.(DiffServiceServer).RawDiff(m, &diffServiceRawDiffServer{stream})
}

type DiffService_RawDiffServer interface {
	Send(*RawDiffResponse) error
	grpc.ServerStream
}

type diffServiceRawDiffServer struct {
	grpc.ServerStream
}

func (x *diffServiceRawDiffServer) Send(m *RawDiffResponse) error {
	return x.ServerStream.SendMsg(m)
}

func _DiffService_RawPatch_Handler(srv interface{}, stream grpc.ServerStream) error {
	m := new(RawPatchRequest)
	if err := stream.RecvMsg(m); err != nil {
		return err
	}
	return srv.(DiffServiceServer).RawPatch(m, &diffServiceRawPatchServer{stream})
}

type DiffService_RawPatchServer interface {
	Send(*RawPatchResponse) error
	grpc.ServerStream
}

type diffServiceRawPatchServer struct {
	grpc.ServerStream
}

func (x *diffServiceRawPatchServer) Send(m *RawPatchResponse) error {
	return x.ServerStream.SendMsg(m)
}

func _DiffService_DiffStats_Handler(srv interface{}, stream grpc.ServerStream) error {
	m := new(DiffStatsRequest)
	if err := stream.RecvMsg(m); err != nil {
		return err
	}
	return srv.(DiffServiceServer).DiffStats(m, &diffServiceDiffStatsServer{stream})
}

type DiffService_DiffStatsServer interface {
	Send(*DiffStatsResponse) error
	grpc.ServerStream
}

type diffServiceDiffStatsServer struct {
	grpc.ServerStream
}

func (x *diffServiceDiffStatsServer) Send(m *DiffStatsResponse) error {
	return x.ServerStream.SendMsg(m)
}

func _DiffService_FindChangedPaths_Handler(srv interface{}, stream grpc.ServerStream) error {
	m := new(FindChangedPathsRequest)
	if err := stream.RecvMsg(m); err != nil {
		return err
	}
	return srv.(DiffServiceServer).FindChangedPaths(m, &diffServiceFindChangedPathsServer{stream})
}

type DiffService_FindChangedPathsServer interface {
	Send(*FindChangedPathsResponse) error
	grpc.ServerStream
}

type diffServiceFindChangedPathsServer struct {
	grpc.ServerStream
}

func (x *diffServiceFindChangedPathsServer) Send(m *FindChangedPathsResponse) error {
	return x.ServerStream.SendMsg(m)
}

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
	},
	Metadata: "diff.proto",
}
