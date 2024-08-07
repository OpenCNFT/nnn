// Code generated by protoc-gen-go-grpc. DO NOT EDIT.
// versions:
// - protoc-gen-go-grpc v1.4.0
// - protoc             v4.23.1
// source: server.proto

package gitalypb

import (
	context "context"
	grpc "google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
)

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
// Requires gRPC-Go v1.62.0 or later.
const _ = grpc.SupportPackageIsVersion8

const (
	ServerService_ServerInfo_FullMethodName      = "/gitaly.ServerService/ServerInfo"
	ServerService_DiskStatistics_FullMethodName  = "/gitaly.ServerService/DiskStatistics"
	ServerService_ClockSynced_FullMethodName     = "/gitaly.ServerService/ClockSynced"
	ServerService_ReadinessCheck_FullMethodName  = "/gitaly.ServerService/ReadinessCheck"
	ServerService_ServerSignature_FullMethodName = "/gitaly.ServerService/ServerSignature"
)

// ServerServiceClient is the client API for ServerService service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://pkg.go.dev/google.golang.org/grpc/?tab=doc#ClientConn.NewStream.
//
// ServerService is a service that provides information about a Gitaly server.
type ServerServiceClient interface {
	// ServerInfo ...
	ServerInfo(ctx context.Context, in *ServerInfoRequest, opts ...grpc.CallOption) (*ServerInfoResponse, error)
	// DiskStatistics ...
	DiskStatistics(ctx context.Context, in *DiskStatisticsRequest, opts ...grpc.CallOption) (*DiskStatisticsResponse, error)
	// ClockSynced checks if machine clock is synced
	// (the offset is less that the one passed in the request).
	ClockSynced(ctx context.Context, in *ClockSyncedRequest, opts ...grpc.CallOption) (*ClockSyncedResponse, error)
	// ReadinessCheck runs the set of the checks to make sure service is in operational state.
	ReadinessCheck(ctx context.Context, in *ReadinessCheckRequest, opts ...grpc.CallOption) (*ReadinessCheckResponse, error)
	// ServerSignature returns the contents of the public key used to sign
	// commits made through the GitLab UI or Web IDE.
	// See https://docs.gitlab.com/ee/user/project/repository/signed_commits/#verify-commits-made-in-the-web-ui
	// for more information.
	ServerSignature(ctx context.Context, in *ServerSignatureRequest, opts ...grpc.CallOption) (*ServerSignatureResponse, error)
}

type serverServiceClient struct {
	cc grpc.ClientConnInterface
}

func NewServerServiceClient(cc grpc.ClientConnInterface) ServerServiceClient {
	return &serverServiceClient{cc}
}

func (c *serverServiceClient) ServerInfo(ctx context.Context, in *ServerInfoRequest, opts ...grpc.CallOption) (*ServerInfoResponse, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(ServerInfoResponse)
	err := c.cc.Invoke(ctx, ServerService_ServerInfo_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *serverServiceClient) DiskStatistics(ctx context.Context, in *DiskStatisticsRequest, opts ...grpc.CallOption) (*DiskStatisticsResponse, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(DiskStatisticsResponse)
	err := c.cc.Invoke(ctx, ServerService_DiskStatistics_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *serverServiceClient) ClockSynced(ctx context.Context, in *ClockSyncedRequest, opts ...grpc.CallOption) (*ClockSyncedResponse, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(ClockSyncedResponse)
	err := c.cc.Invoke(ctx, ServerService_ClockSynced_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *serverServiceClient) ReadinessCheck(ctx context.Context, in *ReadinessCheckRequest, opts ...grpc.CallOption) (*ReadinessCheckResponse, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(ReadinessCheckResponse)
	err := c.cc.Invoke(ctx, ServerService_ReadinessCheck_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *serverServiceClient) ServerSignature(ctx context.Context, in *ServerSignatureRequest, opts ...grpc.CallOption) (*ServerSignatureResponse, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(ServerSignatureResponse)
	err := c.cc.Invoke(ctx, ServerService_ServerSignature_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// ServerServiceServer is the server API for ServerService service.
// All implementations must embed UnimplementedServerServiceServer
// for forward compatibility
//
// ServerService is a service that provides information about a Gitaly server.
type ServerServiceServer interface {
	// ServerInfo ...
	ServerInfo(context.Context, *ServerInfoRequest) (*ServerInfoResponse, error)
	// DiskStatistics ...
	DiskStatistics(context.Context, *DiskStatisticsRequest) (*DiskStatisticsResponse, error)
	// ClockSynced checks if machine clock is synced
	// (the offset is less that the one passed in the request).
	ClockSynced(context.Context, *ClockSyncedRequest) (*ClockSyncedResponse, error)
	// ReadinessCheck runs the set of the checks to make sure service is in operational state.
	ReadinessCheck(context.Context, *ReadinessCheckRequest) (*ReadinessCheckResponse, error)
	// ServerSignature returns the contents of the public key used to sign
	// commits made through the GitLab UI or Web IDE.
	// See https://docs.gitlab.com/ee/user/project/repository/signed_commits/#verify-commits-made-in-the-web-ui
	// for more information.
	ServerSignature(context.Context, *ServerSignatureRequest) (*ServerSignatureResponse, error)
	mustEmbedUnimplementedServerServiceServer()
}

// UnimplementedServerServiceServer must be embedded to have forward compatible implementations.
type UnimplementedServerServiceServer struct {
}

func (UnimplementedServerServiceServer) ServerInfo(context.Context, *ServerInfoRequest) (*ServerInfoResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method ServerInfo not implemented")
}
func (UnimplementedServerServiceServer) DiskStatistics(context.Context, *DiskStatisticsRequest) (*DiskStatisticsResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method DiskStatistics not implemented")
}
func (UnimplementedServerServiceServer) ClockSynced(context.Context, *ClockSyncedRequest) (*ClockSyncedResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method ClockSynced not implemented")
}
func (UnimplementedServerServiceServer) ReadinessCheck(context.Context, *ReadinessCheckRequest) (*ReadinessCheckResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method ReadinessCheck not implemented")
}
func (UnimplementedServerServiceServer) ServerSignature(context.Context, *ServerSignatureRequest) (*ServerSignatureResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method ServerSignature not implemented")
}
func (UnimplementedServerServiceServer) mustEmbedUnimplementedServerServiceServer() {}

// UnsafeServerServiceServer may be embedded to opt out of forward compatibility for this service.
// Use of this interface is not recommended, as added methods to ServerServiceServer will
// result in compilation errors.
type UnsafeServerServiceServer interface {
	mustEmbedUnimplementedServerServiceServer()
}

func RegisterServerServiceServer(s grpc.ServiceRegistrar, srv ServerServiceServer) {
	s.RegisterService(&ServerService_ServiceDesc, srv)
}

func _ServerService_ServerInfo_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(ServerInfoRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ServerServiceServer).ServerInfo(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: ServerService_ServerInfo_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ServerServiceServer).ServerInfo(ctx, req.(*ServerInfoRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _ServerService_DiskStatistics_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(DiskStatisticsRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ServerServiceServer).DiskStatistics(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: ServerService_DiskStatistics_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ServerServiceServer).DiskStatistics(ctx, req.(*DiskStatisticsRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _ServerService_ClockSynced_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(ClockSyncedRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ServerServiceServer).ClockSynced(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: ServerService_ClockSynced_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ServerServiceServer).ClockSynced(ctx, req.(*ClockSyncedRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _ServerService_ReadinessCheck_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(ReadinessCheckRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ServerServiceServer).ReadinessCheck(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: ServerService_ReadinessCheck_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ServerServiceServer).ReadinessCheck(ctx, req.(*ReadinessCheckRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _ServerService_ServerSignature_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(ServerSignatureRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ServerServiceServer).ServerSignature(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: ServerService_ServerSignature_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ServerServiceServer).ServerSignature(ctx, req.(*ServerSignatureRequest))
	}
	return interceptor(ctx, in, info, handler)
}

// ServerService_ServiceDesc is the grpc.ServiceDesc for ServerService service.
// It's only intended for direct use with grpc.RegisterService,
// and not to be introspected or modified (even as a copy)
var ServerService_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "gitaly.ServerService",
	HandlerType: (*ServerServiceServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "ServerInfo",
			Handler:    _ServerService_ServerInfo_Handler,
		},
		{
			MethodName: "DiskStatistics",
			Handler:    _ServerService_DiskStatistics_Handler,
		},
		{
			MethodName: "ClockSynced",
			Handler:    _ServerService_ClockSynced_Handler,
		},
		{
			MethodName: "ReadinessCheck",
			Handler:    _ServerService_ReadinessCheck_Handler,
		},
		{
			MethodName: "ServerSignature",
			Handler:    _ServerService_ServerSignature_Handler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "server.proto",
}
