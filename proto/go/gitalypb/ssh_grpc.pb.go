// Code generated by protoc-gen-go-grpc. DO NOT EDIT.
// versions:
// - protoc-gen-go-grpc v1.5.1
// - protoc             v4.23.1
// source: ssh.proto

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
	SSHService_SSHUploadPack_FullMethodName                = "/gitaly.SSHService/SSHUploadPack"
	SSHService_SSHUploadPackWithSidechannel_FullMethodName = "/gitaly.SSHService/SSHUploadPackWithSidechannel"
	SSHService_SSHReceivePack_FullMethodName               = "/gitaly.SSHService/SSHReceivePack"
	SSHService_SSHUploadArchive_FullMethodName             = "/gitaly.SSHService/SSHUploadArchive"
)

// SSHServiceClient is the client API for SSHService service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://pkg.go.dev/google.golang.org/grpc/?tab=doc#ClientConn.NewStream.
//
// SSHService is a service that provides RPCs required for SSH-based Git clones.
type SSHServiceClient interface {
	// Deprecated: Do not use.
	// SSHUploadPack is an RPC to forward git-upload-pack(1) to Gitaly for SSH sessions. The RPC uses
	// bidirectional streaming so the client can stream stdin and the server can stream stdout and
	// stderr for git-upload-pack(1).
	SSHUploadPack(ctx context.Context, opts ...grpc.CallOption) (grpc.BidiStreamingClient[SSHUploadPackRequest, SSHUploadPackResponse], error)
	// SSHUploadPackWithSidechannel is an RPC to forward git-upload-pack(1) to Gitaly for SSH
	// sessions, via sidechannels. Sidechannel connections sidestep gRPC Protobuf message overhead and
	// allow higher throughput of bulk data transfers. The stdin, stdout, and stderr for the
	// git-upload-pack(1) are streamed through the sidechannel connection.
	SSHUploadPackWithSidechannel(ctx context.Context, in *SSHUploadPackWithSidechannelRequest, opts ...grpc.CallOption) (*SSHUploadPackWithSidechannelResponse, error)
	// SSHReceivePack is an RPC to forward git-receive-pack(1) to Gitaly for SSH sessions. The RPC uses
	// bidirectional streaming so the client can stream stdin and the server can stream stdout and
	// stderr for git-receive-pack(1).
	SSHReceivePack(ctx context.Context, opts ...grpc.CallOption) (grpc.BidiStreamingClient[SSHReceivePackRequest, SSHReceivePackResponse], error)
	// SSHUploadArchive is an RPC to forward git-upload-archive(1) to Gitaly for SSH sessions. The RPC
	// uses bidirectional streaming so the client can stream stdin and the server can stream stdout
	// and stderr for git-upload-archive(1).
	SSHUploadArchive(ctx context.Context, opts ...grpc.CallOption) (grpc.BidiStreamingClient[SSHUploadArchiveRequest, SSHUploadArchiveResponse], error)
}

type sSHServiceClient struct {
	cc grpc.ClientConnInterface
}

func NewSSHServiceClient(cc grpc.ClientConnInterface) SSHServiceClient {
	return &sSHServiceClient{cc}
}

// Deprecated: Do not use.
func (c *sSHServiceClient) SSHUploadPack(ctx context.Context, opts ...grpc.CallOption) (grpc.BidiStreamingClient[SSHUploadPackRequest, SSHUploadPackResponse], error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	stream, err := c.cc.NewStream(ctx, &SSHService_ServiceDesc.Streams[0], SSHService_SSHUploadPack_FullMethodName, cOpts...)
	if err != nil {
		return nil, err
	}
	x := &grpc.GenericClientStream[SSHUploadPackRequest, SSHUploadPackResponse]{ClientStream: stream}
	return x, nil
}

// This type alias is provided for backwards compatibility with existing code that references the prior non-generic stream type by name.
type SSHService_SSHUploadPackClient = grpc.BidiStreamingClient[SSHUploadPackRequest, SSHUploadPackResponse]

func (c *sSHServiceClient) SSHUploadPackWithSidechannel(ctx context.Context, in *SSHUploadPackWithSidechannelRequest, opts ...grpc.CallOption) (*SSHUploadPackWithSidechannelResponse, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(SSHUploadPackWithSidechannelResponse)
	err := c.cc.Invoke(ctx, SSHService_SSHUploadPackWithSidechannel_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *sSHServiceClient) SSHReceivePack(ctx context.Context, opts ...grpc.CallOption) (grpc.BidiStreamingClient[SSHReceivePackRequest, SSHReceivePackResponse], error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	stream, err := c.cc.NewStream(ctx, &SSHService_ServiceDesc.Streams[1], SSHService_SSHReceivePack_FullMethodName, cOpts...)
	if err != nil {
		return nil, err
	}
	x := &grpc.GenericClientStream[SSHReceivePackRequest, SSHReceivePackResponse]{ClientStream: stream}
	return x, nil
}

// This type alias is provided for backwards compatibility with existing code that references the prior non-generic stream type by name.
type SSHService_SSHReceivePackClient = grpc.BidiStreamingClient[SSHReceivePackRequest, SSHReceivePackResponse]

func (c *sSHServiceClient) SSHUploadArchive(ctx context.Context, opts ...grpc.CallOption) (grpc.BidiStreamingClient[SSHUploadArchiveRequest, SSHUploadArchiveResponse], error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	stream, err := c.cc.NewStream(ctx, &SSHService_ServiceDesc.Streams[2], SSHService_SSHUploadArchive_FullMethodName, cOpts...)
	if err != nil {
		return nil, err
	}
	x := &grpc.GenericClientStream[SSHUploadArchiveRequest, SSHUploadArchiveResponse]{ClientStream: stream}
	return x, nil
}

// This type alias is provided for backwards compatibility with existing code that references the prior non-generic stream type by name.
type SSHService_SSHUploadArchiveClient = grpc.BidiStreamingClient[SSHUploadArchiveRequest, SSHUploadArchiveResponse]

// SSHServiceServer is the server API for SSHService service.
// All implementations must embed UnimplementedSSHServiceServer
// for forward compatibility.
//
// SSHService is a service that provides RPCs required for SSH-based Git clones.
type SSHServiceServer interface {
	// Deprecated: Do not use.
	// SSHUploadPack is an RPC to forward git-upload-pack(1) to Gitaly for SSH sessions. The RPC uses
	// bidirectional streaming so the client can stream stdin and the server can stream stdout and
	// stderr for git-upload-pack(1).
	SSHUploadPack(grpc.BidiStreamingServer[SSHUploadPackRequest, SSHUploadPackResponse]) error
	// SSHUploadPackWithSidechannel is an RPC to forward git-upload-pack(1) to Gitaly for SSH
	// sessions, via sidechannels. Sidechannel connections sidestep gRPC Protobuf message overhead and
	// allow higher throughput of bulk data transfers. The stdin, stdout, and stderr for the
	// git-upload-pack(1) are streamed through the sidechannel connection.
	SSHUploadPackWithSidechannel(context.Context, *SSHUploadPackWithSidechannelRequest) (*SSHUploadPackWithSidechannelResponse, error)
	// SSHReceivePack is an RPC to forward git-receive-pack(1) to Gitaly for SSH sessions. The RPC uses
	// bidirectional streaming so the client can stream stdin and the server can stream stdout and
	// stderr for git-receive-pack(1).
	SSHReceivePack(grpc.BidiStreamingServer[SSHReceivePackRequest, SSHReceivePackResponse]) error
	// SSHUploadArchive is an RPC to forward git-upload-archive(1) to Gitaly for SSH sessions. The RPC
	// uses bidirectional streaming so the client can stream stdin and the server can stream stdout
	// and stderr for git-upload-archive(1).
	SSHUploadArchive(grpc.BidiStreamingServer[SSHUploadArchiveRequest, SSHUploadArchiveResponse]) error
	mustEmbedUnimplementedSSHServiceServer()
}

// UnimplementedSSHServiceServer must be embedded to have
// forward compatible implementations.
//
// NOTE: this should be embedded by value instead of pointer to avoid a nil
// pointer dereference when methods are called.
type UnimplementedSSHServiceServer struct{}

func (UnimplementedSSHServiceServer) SSHUploadPack(grpc.BidiStreamingServer[SSHUploadPackRequest, SSHUploadPackResponse]) error {
	return status.Errorf(codes.Unimplemented, "method SSHUploadPack not implemented")
}
func (UnimplementedSSHServiceServer) SSHUploadPackWithSidechannel(context.Context, *SSHUploadPackWithSidechannelRequest) (*SSHUploadPackWithSidechannelResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method SSHUploadPackWithSidechannel not implemented")
}
func (UnimplementedSSHServiceServer) SSHReceivePack(grpc.BidiStreamingServer[SSHReceivePackRequest, SSHReceivePackResponse]) error {
	return status.Errorf(codes.Unimplemented, "method SSHReceivePack not implemented")
}
func (UnimplementedSSHServiceServer) SSHUploadArchive(grpc.BidiStreamingServer[SSHUploadArchiveRequest, SSHUploadArchiveResponse]) error {
	return status.Errorf(codes.Unimplemented, "method SSHUploadArchive not implemented")
}
func (UnimplementedSSHServiceServer) mustEmbedUnimplementedSSHServiceServer() {}
func (UnimplementedSSHServiceServer) testEmbeddedByValue()                    {}

// UnsafeSSHServiceServer may be embedded to opt out of forward compatibility for this service.
// Use of this interface is not recommended, as added methods to SSHServiceServer will
// result in compilation errors.
type UnsafeSSHServiceServer interface {
	mustEmbedUnimplementedSSHServiceServer()
}

func RegisterSSHServiceServer(s grpc.ServiceRegistrar, srv SSHServiceServer) {
	// If the following call pancis, it indicates UnimplementedSSHServiceServer was
	// embedded by pointer and is nil.  This will cause panics if an
	// unimplemented method is ever invoked, so we test this at initialization
	// time to prevent it from happening at runtime later due to I/O.
	if t, ok := srv.(interface{ testEmbeddedByValue() }); ok {
		t.testEmbeddedByValue()
	}
	s.RegisterService(&SSHService_ServiceDesc, srv)
}

func _SSHService_SSHUploadPack_Handler(srv interface{}, stream grpc.ServerStream) error {
	return srv.(SSHServiceServer).SSHUploadPack(&grpc.GenericServerStream[SSHUploadPackRequest, SSHUploadPackResponse]{ServerStream: stream})
}

// This type alias is provided for backwards compatibility with existing code that references the prior non-generic stream type by name.
type SSHService_SSHUploadPackServer = grpc.BidiStreamingServer[SSHUploadPackRequest, SSHUploadPackResponse]

func _SSHService_SSHUploadPackWithSidechannel_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(SSHUploadPackWithSidechannelRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(SSHServiceServer).SSHUploadPackWithSidechannel(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: SSHService_SSHUploadPackWithSidechannel_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(SSHServiceServer).SSHUploadPackWithSidechannel(ctx, req.(*SSHUploadPackWithSidechannelRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _SSHService_SSHReceivePack_Handler(srv interface{}, stream grpc.ServerStream) error {
	return srv.(SSHServiceServer).SSHReceivePack(&grpc.GenericServerStream[SSHReceivePackRequest, SSHReceivePackResponse]{ServerStream: stream})
}

// This type alias is provided for backwards compatibility with existing code that references the prior non-generic stream type by name.
type SSHService_SSHReceivePackServer = grpc.BidiStreamingServer[SSHReceivePackRequest, SSHReceivePackResponse]

func _SSHService_SSHUploadArchive_Handler(srv interface{}, stream grpc.ServerStream) error {
	return srv.(SSHServiceServer).SSHUploadArchive(&grpc.GenericServerStream[SSHUploadArchiveRequest, SSHUploadArchiveResponse]{ServerStream: stream})
}

// This type alias is provided for backwards compatibility with existing code that references the prior non-generic stream type by name.
type SSHService_SSHUploadArchiveServer = grpc.BidiStreamingServer[SSHUploadArchiveRequest, SSHUploadArchiveResponse]

// SSHService_ServiceDesc is the grpc.ServiceDesc for SSHService service.
// It's only intended for direct use with grpc.RegisterService,
// and not to be introspected or modified (even as a copy)
var SSHService_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "gitaly.SSHService",
	HandlerType: (*SSHServiceServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "SSHUploadPackWithSidechannel",
			Handler:    _SSHService_SSHUploadPackWithSidechannel_Handler,
		},
	},
	Streams: []grpc.StreamDesc{
		{
			StreamName:    "SSHUploadPack",
			Handler:       _SSHService_SSHUploadPack_Handler,
			ServerStreams: true,
			ClientStreams: true,
		},
		{
			StreamName:    "SSHReceivePack",
			Handler:       _SSHService_SSHReceivePack_Handler,
			ServerStreams: true,
			ClientStreams: true,
		},
		{
			StreamName:    "SSHUploadArchive",
			Handler:       _SSHService_SSHUploadArchive_Handler,
			ServerStreams: true,
			ClientStreams: true,
		},
	},
	Metadata: "ssh.proto",
}
