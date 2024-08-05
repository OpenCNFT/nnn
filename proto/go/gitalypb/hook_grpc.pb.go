// Code generated by protoc-gen-go-grpc. DO NOT EDIT.
// versions:
// - protoc-gen-go-grpc v1.5.1
// - protoc             v4.23.1
// source: hook.proto

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
	HookService_PreReceiveHook_FullMethodName                 = "/gitaly.HookService/PreReceiveHook"
	HookService_PostReceiveHook_FullMethodName                = "/gitaly.HookService/PostReceiveHook"
	HookService_UpdateHook_FullMethodName                     = "/gitaly.HookService/UpdateHook"
	HookService_ReferenceTransactionHook_FullMethodName       = "/gitaly.HookService/ReferenceTransactionHook"
	HookService_PackObjectsHookWithSidechannel_FullMethodName = "/gitaly.HookService/PackObjectsHookWithSidechannel"
	HookService_ProcReceiveHook_FullMethodName                = "/gitaly.HookService/ProcReceiveHook"
)

// HookServiceClient is the client API for HookService service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://pkg.go.dev/google.golang.org/grpc/?tab=doc#ClientConn.NewStream.
//
// HookService is a service which provides the implementation of a subset of
// Git hooks. These are typically invoked via the `gitaly-hooks` binary to
// ensure that the actual hook logic is executed in the context of the server.
type HookServiceClient interface {
	// PreReceiveHook ...
	PreReceiveHook(ctx context.Context, opts ...grpc.CallOption) (grpc.BidiStreamingClient[PreReceiveHookRequest, PreReceiveHookResponse], error)
	// PostReceiveHook ...
	PostReceiveHook(ctx context.Context, opts ...grpc.CallOption) (grpc.BidiStreamingClient[PostReceiveHookRequest, PostReceiveHookResponse], error)
	// UpdateHook ...
	UpdateHook(ctx context.Context, in *UpdateHookRequest, opts ...grpc.CallOption) (grpc.ServerStreamingClient[UpdateHookResponse], error)
	// ReferenceTransactionHook ...
	ReferenceTransactionHook(ctx context.Context, opts ...grpc.CallOption) (grpc.BidiStreamingClient[ReferenceTransactionHookRequest, ReferenceTransactionHookResponse], error)
	// PackObjectsHookWithSidechannel is an optimized version of PackObjectsHook that uses
	// a unix socket side channel.
	PackObjectsHookWithSidechannel(ctx context.Context, in *PackObjectsHookWithSidechannelRequest, opts ...grpc.CallOption) (*PackObjectsHookWithSidechannelResponse, error)
	// ProcReceiveHook is a hook invoked by git-receive-pack(1) [1]. This hook is responsible
	// for updating the relevant references and reporting the results back to receive-pack.
	//
	// [1]: https://git-scm.com/docs/githooks#proc-receive
	ProcReceiveHook(ctx context.Context, opts ...grpc.CallOption) (grpc.BidiStreamingClient[ProcReceiveHookRequest, ProcReceiveHookResponse], error)
}

type hookServiceClient struct {
	cc grpc.ClientConnInterface
}

func NewHookServiceClient(cc grpc.ClientConnInterface) HookServiceClient {
	return &hookServiceClient{cc}
}

func (c *hookServiceClient) PreReceiveHook(ctx context.Context, opts ...grpc.CallOption) (grpc.BidiStreamingClient[PreReceiveHookRequest, PreReceiveHookResponse], error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	stream, err := c.cc.NewStream(ctx, &HookService_ServiceDesc.Streams[0], HookService_PreReceiveHook_FullMethodName, cOpts...)
	if err != nil {
		return nil, err
	}
	x := &grpc.GenericClientStream[PreReceiveHookRequest, PreReceiveHookResponse]{ClientStream: stream}
	return x, nil
}

// This type alias is provided for backwards compatibility with existing code that references the prior non-generic stream type by name.
type HookService_PreReceiveHookClient = grpc.BidiStreamingClient[PreReceiveHookRequest, PreReceiveHookResponse]

func (c *hookServiceClient) PostReceiveHook(ctx context.Context, opts ...grpc.CallOption) (grpc.BidiStreamingClient[PostReceiveHookRequest, PostReceiveHookResponse], error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	stream, err := c.cc.NewStream(ctx, &HookService_ServiceDesc.Streams[1], HookService_PostReceiveHook_FullMethodName, cOpts...)
	if err != nil {
		return nil, err
	}
	x := &grpc.GenericClientStream[PostReceiveHookRequest, PostReceiveHookResponse]{ClientStream: stream}
	return x, nil
}

// This type alias is provided for backwards compatibility with existing code that references the prior non-generic stream type by name.
type HookService_PostReceiveHookClient = grpc.BidiStreamingClient[PostReceiveHookRequest, PostReceiveHookResponse]

func (c *hookServiceClient) UpdateHook(ctx context.Context, in *UpdateHookRequest, opts ...grpc.CallOption) (grpc.ServerStreamingClient[UpdateHookResponse], error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	stream, err := c.cc.NewStream(ctx, &HookService_ServiceDesc.Streams[2], HookService_UpdateHook_FullMethodName, cOpts...)
	if err != nil {
		return nil, err
	}
	x := &grpc.GenericClientStream[UpdateHookRequest, UpdateHookResponse]{ClientStream: stream}
	if err := x.ClientStream.SendMsg(in); err != nil {
		return nil, err
	}
	if err := x.ClientStream.CloseSend(); err != nil {
		return nil, err
	}
	return x, nil
}

// This type alias is provided for backwards compatibility with existing code that references the prior non-generic stream type by name.
type HookService_UpdateHookClient = grpc.ServerStreamingClient[UpdateHookResponse]

func (c *hookServiceClient) ReferenceTransactionHook(ctx context.Context, opts ...grpc.CallOption) (grpc.BidiStreamingClient[ReferenceTransactionHookRequest, ReferenceTransactionHookResponse], error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	stream, err := c.cc.NewStream(ctx, &HookService_ServiceDesc.Streams[3], HookService_ReferenceTransactionHook_FullMethodName, cOpts...)
	if err != nil {
		return nil, err
	}
	x := &grpc.GenericClientStream[ReferenceTransactionHookRequest, ReferenceTransactionHookResponse]{ClientStream: stream}
	return x, nil
}

// This type alias is provided for backwards compatibility with existing code that references the prior non-generic stream type by name.
type HookService_ReferenceTransactionHookClient = grpc.BidiStreamingClient[ReferenceTransactionHookRequest, ReferenceTransactionHookResponse]

func (c *hookServiceClient) PackObjectsHookWithSidechannel(ctx context.Context, in *PackObjectsHookWithSidechannelRequest, opts ...grpc.CallOption) (*PackObjectsHookWithSidechannelResponse, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(PackObjectsHookWithSidechannelResponse)
	err := c.cc.Invoke(ctx, HookService_PackObjectsHookWithSidechannel_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *hookServiceClient) ProcReceiveHook(ctx context.Context, opts ...grpc.CallOption) (grpc.BidiStreamingClient[ProcReceiveHookRequest, ProcReceiveHookResponse], error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	stream, err := c.cc.NewStream(ctx, &HookService_ServiceDesc.Streams[4], HookService_ProcReceiveHook_FullMethodName, cOpts...)
	if err != nil {
		return nil, err
	}
	x := &grpc.GenericClientStream[ProcReceiveHookRequest, ProcReceiveHookResponse]{ClientStream: stream}
	return x, nil
}

// This type alias is provided for backwards compatibility with existing code that references the prior non-generic stream type by name.
type HookService_ProcReceiveHookClient = grpc.BidiStreamingClient[ProcReceiveHookRequest, ProcReceiveHookResponse]

// HookServiceServer is the server API for HookService service.
// All implementations must embed UnimplementedHookServiceServer
// for forward compatibility.
//
// HookService is a service which provides the implementation of a subset of
// Git hooks. These are typically invoked via the `gitaly-hooks` binary to
// ensure that the actual hook logic is executed in the context of the server.
type HookServiceServer interface {
	// PreReceiveHook ...
	PreReceiveHook(grpc.BidiStreamingServer[PreReceiveHookRequest, PreReceiveHookResponse]) error
	// PostReceiveHook ...
	PostReceiveHook(grpc.BidiStreamingServer[PostReceiveHookRequest, PostReceiveHookResponse]) error
	// UpdateHook ...
	UpdateHook(*UpdateHookRequest, grpc.ServerStreamingServer[UpdateHookResponse]) error
	// ReferenceTransactionHook ...
	ReferenceTransactionHook(grpc.BidiStreamingServer[ReferenceTransactionHookRequest, ReferenceTransactionHookResponse]) error
	// PackObjectsHookWithSidechannel is an optimized version of PackObjectsHook that uses
	// a unix socket side channel.
	PackObjectsHookWithSidechannel(context.Context, *PackObjectsHookWithSidechannelRequest) (*PackObjectsHookWithSidechannelResponse, error)
	// ProcReceiveHook is a hook invoked by git-receive-pack(1) [1]. This hook is responsible
	// for updating the relevant references and reporting the results back to receive-pack.
	//
	// [1]: https://git-scm.com/docs/githooks#proc-receive
	ProcReceiveHook(grpc.BidiStreamingServer[ProcReceiveHookRequest, ProcReceiveHookResponse]) error
	mustEmbedUnimplementedHookServiceServer()
}

// UnimplementedHookServiceServer must be embedded to have
// forward compatible implementations.
//
// NOTE: this should be embedded by value instead of pointer to avoid a nil
// pointer dereference when methods are called.
type UnimplementedHookServiceServer struct{}

func (UnimplementedHookServiceServer) PreReceiveHook(grpc.BidiStreamingServer[PreReceiveHookRequest, PreReceiveHookResponse]) error {
	return status.Errorf(codes.Unimplemented, "method PreReceiveHook not implemented")
}
func (UnimplementedHookServiceServer) PostReceiveHook(grpc.BidiStreamingServer[PostReceiveHookRequest, PostReceiveHookResponse]) error {
	return status.Errorf(codes.Unimplemented, "method PostReceiveHook not implemented")
}
func (UnimplementedHookServiceServer) UpdateHook(*UpdateHookRequest, grpc.ServerStreamingServer[UpdateHookResponse]) error {
	return status.Errorf(codes.Unimplemented, "method UpdateHook not implemented")
}
func (UnimplementedHookServiceServer) ReferenceTransactionHook(grpc.BidiStreamingServer[ReferenceTransactionHookRequest, ReferenceTransactionHookResponse]) error {
	return status.Errorf(codes.Unimplemented, "method ReferenceTransactionHook not implemented")
}
func (UnimplementedHookServiceServer) PackObjectsHookWithSidechannel(context.Context, *PackObjectsHookWithSidechannelRequest) (*PackObjectsHookWithSidechannelResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method PackObjectsHookWithSidechannel not implemented")
}
func (UnimplementedHookServiceServer) ProcReceiveHook(grpc.BidiStreamingServer[ProcReceiveHookRequest, ProcReceiveHookResponse]) error {
	return status.Errorf(codes.Unimplemented, "method ProcReceiveHook not implemented")
}
func (UnimplementedHookServiceServer) mustEmbedUnimplementedHookServiceServer() {}
func (UnimplementedHookServiceServer) testEmbeddedByValue()                     {}

// UnsafeHookServiceServer may be embedded to opt out of forward compatibility for this service.
// Use of this interface is not recommended, as added methods to HookServiceServer will
// result in compilation errors.
type UnsafeHookServiceServer interface {
	mustEmbedUnimplementedHookServiceServer()
}

func RegisterHookServiceServer(s grpc.ServiceRegistrar, srv HookServiceServer) {
	// If the following call pancis, it indicates UnimplementedHookServiceServer was
	// embedded by pointer and is nil.  This will cause panics if an
	// unimplemented method is ever invoked, so we test this at initialization
	// time to prevent it from happening at runtime later due to I/O.
	if t, ok := srv.(interface{ testEmbeddedByValue() }); ok {
		t.testEmbeddedByValue()
	}
	s.RegisterService(&HookService_ServiceDesc, srv)
}

func _HookService_PreReceiveHook_Handler(srv interface{}, stream grpc.ServerStream) error {
	return srv.(HookServiceServer).PreReceiveHook(&grpc.GenericServerStream[PreReceiveHookRequest, PreReceiveHookResponse]{ServerStream: stream})
}

// This type alias is provided for backwards compatibility with existing code that references the prior non-generic stream type by name.
type HookService_PreReceiveHookServer = grpc.BidiStreamingServer[PreReceiveHookRequest, PreReceiveHookResponse]

func _HookService_PostReceiveHook_Handler(srv interface{}, stream grpc.ServerStream) error {
	return srv.(HookServiceServer).PostReceiveHook(&grpc.GenericServerStream[PostReceiveHookRequest, PostReceiveHookResponse]{ServerStream: stream})
}

// This type alias is provided for backwards compatibility with existing code that references the prior non-generic stream type by name.
type HookService_PostReceiveHookServer = grpc.BidiStreamingServer[PostReceiveHookRequest, PostReceiveHookResponse]

func _HookService_UpdateHook_Handler(srv interface{}, stream grpc.ServerStream) error {
	m := new(UpdateHookRequest)
	if err := stream.RecvMsg(m); err != nil {
		return err
	}
	return srv.(HookServiceServer).UpdateHook(m, &grpc.GenericServerStream[UpdateHookRequest, UpdateHookResponse]{ServerStream: stream})
}

// This type alias is provided for backwards compatibility with existing code that references the prior non-generic stream type by name.
type HookService_UpdateHookServer = grpc.ServerStreamingServer[UpdateHookResponse]

func _HookService_ReferenceTransactionHook_Handler(srv interface{}, stream grpc.ServerStream) error {
	return srv.(HookServiceServer).ReferenceTransactionHook(&grpc.GenericServerStream[ReferenceTransactionHookRequest, ReferenceTransactionHookResponse]{ServerStream: stream})
}

// This type alias is provided for backwards compatibility with existing code that references the prior non-generic stream type by name.
type HookService_ReferenceTransactionHookServer = grpc.BidiStreamingServer[ReferenceTransactionHookRequest, ReferenceTransactionHookResponse]

func _HookService_PackObjectsHookWithSidechannel_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(PackObjectsHookWithSidechannelRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(HookServiceServer).PackObjectsHookWithSidechannel(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: HookService_PackObjectsHookWithSidechannel_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(HookServiceServer).PackObjectsHookWithSidechannel(ctx, req.(*PackObjectsHookWithSidechannelRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _HookService_ProcReceiveHook_Handler(srv interface{}, stream grpc.ServerStream) error {
	return srv.(HookServiceServer).ProcReceiveHook(&grpc.GenericServerStream[ProcReceiveHookRequest, ProcReceiveHookResponse]{ServerStream: stream})
}

// This type alias is provided for backwards compatibility with existing code that references the prior non-generic stream type by name.
type HookService_ProcReceiveHookServer = grpc.BidiStreamingServer[ProcReceiveHookRequest, ProcReceiveHookResponse]

// HookService_ServiceDesc is the grpc.ServiceDesc for HookService service.
// It's only intended for direct use with grpc.RegisterService,
// and not to be introspected or modified (even as a copy)
var HookService_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "gitaly.HookService",
	HandlerType: (*HookServiceServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "PackObjectsHookWithSidechannel",
			Handler:    _HookService_PackObjectsHookWithSidechannel_Handler,
		},
	},
	Streams: []grpc.StreamDesc{
		{
			StreamName:    "PreReceiveHook",
			Handler:       _HookService_PreReceiveHook_Handler,
			ServerStreams: true,
			ClientStreams: true,
		},
		{
			StreamName:    "PostReceiveHook",
			Handler:       _HookService_PostReceiveHook_Handler,
			ServerStreams: true,
			ClientStreams: true,
		},
		{
			StreamName:    "UpdateHook",
			Handler:       _HookService_UpdateHook_Handler,
			ServerStreams: true,
		},
		{
			StreamName:    "ReferenceTransactionHook",
			Handler:       _HookService_ReferenceTransactionHook_Handler,
			ServerStreams: true,
			ClientStreams: true,
		},
		{
			StreamName:    "ProcReceiveHook",
			Handler:       _HookService_ProcReceiveHook_Handler,
			ServerStreams: true,
			ClientStreams: true,
		},
	},
	Metadata: "hook.proto",
}
