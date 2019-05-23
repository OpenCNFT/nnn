// Code generated by protoc-gen-go. DO NOT EDIT.
// source: server.proto

package gitalypb // import "gitlab.com/gitlab-org/gitaly-proto/go/gitalypb"

import proto "github.com/golang/protobuf/proto"
import fmt "fmt"
import math "math"

import (
	context "golang.org/x/net/context"
	grpc "google.golang.org/grpc"
)

// Reference imports to suppress errors if they are not otherwise used.
var _ = proto.Marshal
var _ = fmt.Errorf
var _ = math.Inf

// This is a compile-time assertion to ensure that this generated file
// is compatible with the proto package it is being compiled against.
// A compilation error at this line likely means your copy of the
// proto package needs to be updated.
const _ = proto.ProtoPackageIsVersion2 // please upgrade the proto package

type ServerInfoRequest struct {
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *ServerInfoRequest) Reset()         { *m = ServerInfoRequest{} }
func (m *ServerInfoRequest) String() string { return proto.CompactTextString(m) }
func (*ServerInfoRequest) ProtoMessage()    {}
func (*ServerInfoRequest) Descriptor() ([]byte, []int) {
	return fileDescriptor_server_f3182e3be274f50f, []int{0}
}
func (m *ServerInfoRequest) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_ServerInfoRequest.Unmarshal(m, b)
}
func (m *ServerInfoRequest) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_ServerInfoRequest.Marshal(b, m, deterministic)
}
func (dst *ServerInfoRequest) XXX_Merge(src proto.Message) {
	xxx_messageInfo_ServerInfoRequest.Merge(dst, src)
}
func (m *ServerInfoRequest) XXX_Size() int {
	return xxx_messageInfo_ServerInfoRequest.Size(m)
}
func (m *ServerInfoRequest) XXX_DiscardUnknown() {
	xxx_messageInfo_ServerInfoRequest.DiscardUnknown(m)
}

var xxx_messageInfo_ServerInfoRequest proto.InternalMessageInfo

type ServerInfoResponse struct {
	ServerVersion        string                              `protobuf:"bytes,1,opt,name=server_version,json=serverVersion,proto3" json:"server_version,omitempty"`
	GitVersion           string                              `protobuf:"bytes,2,opt,name=git_version,json=gitVersion,proto3" json:"git_version,omitempty"`
	StorageStatuses      []*ServerInfoResponse_StorageStatus `protobuf:"bytes,3,rep,name=storage_statuses,json=storageStatuses,proto3" json:"storage_statuses,omitempty"`
	XXX_NoUnkeyedLiteral struct{}                            `json:"-"`
	XXX_unrecognized     []byte                              `json:"-"`
	XXX_sizecache        int32                               `json:"-"`
}

func (m *ServerInfoResponse) Reset()         { *m = ServerInfoResponse{} }
func (m *ServerInfoResponse) String() string { return proto.CompactTextString(m) }
func (*ServerInfoResponse) ProtoMessage()    {}
func (*ServerInfoResponse) Descriptor() ([]byte, []int) {
	return fileDescriptor_server_f3182e3be274f50f, []int{1}
}
func (m *ServerInfoResponse) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_ServerInfoResponse.Unmarshal(m, b)
}
func (m *ServerInfoResponse) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_ServerInfoResponse.Marshal(b, m, deterministic)
}
func (dst *ServerInfoResponse) XXX_Merge(src proto.Message) {
	xxx_messageInfo_ServerInfoResponse.Merge(dst, src)
}
func (m *ServerInfoResponse) XXX_Size() int {
	return xxx_messageInfo_ServerInfoResponse.Size(m)
}
func (m *ServerInfoResponse) XXX_DiscardUnknown() {
	xxx_messageInfo_ServerInfoResponse.DiscardUnknown(m)
}

var xxx_messageInfo_ServerInfoResponse proto.InternalMessageInfo

func (m *ServerInfoResponse) GetServerVersion() string {
	if m != nil {
		return m.ServerVersion
	}
	return ""
}

func (m *ServerInfoResponse) GetGitVersion() string {
	if m != nil {
		return m.GitVersion
	}
	return ""
}

func (m *ServerInfoResponse) GetStorageStatuses() []*ServerInfoResponse_StorageStatus {
	if m != nil {
		return m.StorageStatuses
	}
	return nil
}

type ServerInfoResponse_StorageStatus struct {
	StorageName          string   `protobuf:"bytes,1,opt,name=storage_name,json=storageName,proto3" json:"storage_name,omitempty"`
	Readable             bool     `protobuf:"varint,2,opt,name=readable,proto3" json:"readable,omitempty"`
	Writeable            bool     `protobuf:"varint,3,opt,name=writeable,proto3" json:"writeable,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *ServerInfoResponse_StorageStatus) Reset()         { *m = ServerInfoResponse_StorageStatus{} }
func (m *ServerInfoResponse_StorageStatus) String() string { return proto.CompactTextString(m) }
func (*ServerInfoResponse_StorageStatus) ProtoMessage()    {}
func (*ServerInfoResponse_StorageStatus) Descriptor() ([]byte, []int) {
	return fileDescriptor_server_f3182e3be274f50f, []int{1, 0}
}
func (m *ServerInfoResponse_StorageStatus) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_ServerInfoResponse_StorageStatus.Unmarshal(m, b)
}
func (m *ServerInfoResponse_StorageStatus) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_ServerInfoResponse_StorageStatus.Marshal(b, m, deterministic)
}
func (dst *ServerInfoResponse_StorageStatus) XXX_Merge(src proto.Message) {
	xxx_messageInfo_ServerInfoResponse_StorageStatus.Merge(dst, src)
}
func (m *ServerInfoResponse_StorageStatus) XXX_Size() int {
	return xxx_messageInfo_ServerInfoResponse_StorageStatus.Size(m)
}
func (m *ServerInfoResponse_StorageStatus) XXX_DiscardUnknown() {
	xxx_messageInfo_ServerInfoResponse_StorageStatus.DiscardUnknown(m)
}

var xxx_messageInfo_ServerInfoResponse_StorageStatus proto.InternalMessageInfo

func (m *ServerInfoResponse_StorageStatus) GetStorageName() string {
	if m != nil {
		return m.StorageName
	}
	return ""
}

func (m *ServerInfoResponse_StorageStatus) GetReadable() bool {
	if m != nil {
		return m.Readable
	}
	return false
}

func (m *ServerInfoResponse_StorageStatus) GetWriteable() bool {
	if m != nil {
		return m.Writeable
	}
	return false
}

type GetObjectsRequest struct {
	Repository *Repository `protobuf:"bytes,1,opt,name=repository,proto3" json:"repository,omitempty"`
	// Object IDs (SHA1) of the object we want to get
	Oids                 []string `protobuf:"bytes,2,rep,name=oids,proto3" json:"oids,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *GetObjectsRequest) Reset()         { *m = GetObjectsRequest{} }
func (m *GetObjectsRequest) String() string { return proto.CompactTextString(m) }
func (*GetObjectsRequest) ProtoMessage()    {}
func (*GetObjectsRequest) Descriptor() ([]byte, []int) {
	return fileDescriptor_server_f3182e3be274f50f, []int{2}
}
func (m *GetObjectsRequest) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_GetObjectsRequest.Unmarshal(m, b)
}
func (m *GetObjectsRequest) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_GetObjectsRequest.Marshal(b, m, deterministic)
}
func (dst *GetObjectsRequest) XXX_Merge(src proto.Message) {
	xxx_messageInfo_GetObjectsRequest.Merge(dst, src)
}
func (m *GetObjectsRequest) XXX_Size() int {
	return xxx_messageInfo_GetObjectsRequest.Size(m)
}
func (m *GetObjectsRequest) XXX_DiscardUnknown() {
	xxx_messageInfo_GetObjectsRequest.DiscardUnknown(m)
}

var xxx_messageInfo_GetObjectsRequest proto.InternalMessageInfo

func (m *GetObjectsRequest) GetRepository() *Repository {
	if m != nil {
		return m.Repository
	}
	return nil
}

func (m *GetObjectsRequest) GetOids() []string {
	if m != nil {
		return m.Oids
	}
	return nil
}

type GetObjectsResponse struct {
	// Object size; present only in first response message
	Size int64 `protobuf:"varint,1,opt,name=size,proto3" json:"size,omitempty"`
	// Chunk of Object data
	Data []byte `protobuf:"bytes,2,opt,name=data,proto3" json:"data,omitempty"`
	// Object ID of the actual object returned. Empty if no object was found.
	Oid string `protobuf:"bytes,3,opt,name=oid,proto3" json:"oid,omitempty"`
	// Object Type of the actual object returned. Empty if no object was found.
	Type                 ObjectType `protobuf:"varint,4,opt,name=type,proto3,enum=gitaly.ObjectType" json:"type,omitempty"`
	XXX_NoUnkeyedLiteral struct{}   `json:"-"`
	XXX_unrecognized     []byte     `json:"-"`
	XXX_sizecache        int32      `json:"-"`
}

func (m *GetObjectsResponse) Reset()         { *m = GetObjectsResponse{} }
func (m *GetObjectsResponse) String() string { return proto.CompactTextString(m) }
func (*GetObjectsResponse) ProtoMessage()    {}
func (*GetObjectsResponse) Descriptor() ([]byte, []int) {
	return fileDescriptor_server_f3182e3be274f50f, []int{3}
}
func (m *GetObjectsResponse) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_GetObjectsResponse.Unmarshal(m, b)
}
func (m *GetObjectsResponse) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_GetObjectsResponse.Marshal(b, m, deterministic)
}
func (dst *GetObjectsResponse) XXX_Merge(src proto.Message) {
	xxx_messageInfo_GetObjectsResponse.Merge(dst, src)
}
func (m *GetObjectsResponse) XXX_Size() int {
	return xxx_messageInfo_GetObjectsResponse.Size(m)
}
func (m *GetObjectsResponse) XXX_DiscardUnknown() {
	xxx_messageInfo_GetObjectsResponse.DiscardUnknown(m)
}

var xxx_messageInfo_GetObjectsResponse proto.InternalMessageInfo

func (m *GetObjectsResponse) GetSize() int64 {
	if m != nil {
		return m.Size
	}
	return 0
}

func (m *GetObjectsResponse) GetData() []byte {
	if m != nil {
		return m.Data
	}
	return nil
}

func (m *GetObjectsResponse) GetOid() string {
	if m != nil {
		return m.Oid
	}
	return ""
}

func (m *GetObjectsResponse) GetType() ObjectType {
	if m != nil {
		return m.Type
	}
	return ObjectType_UNKNOWN
}

type ReceivePackRequest struct {
	Repository *Repository `protobuf:"bytes,1,opt,name=repository,proto3" json:"repository,omitempty"`
	// Object IDs (SHA1) of the object we want to get
	Oids []string `protobuf:"bytes,2,rep,name=oids,proto3" json:"oids,omitempty"`
	// the depth of commit
	Depth                int32    `protobuf:"varint,3,opt,name=depth,proto3" json:"depth,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *ReceivePackRequest) Reset()         { *m = ReceivePackRequest{} }
func (m *ReceivePackRequest) String() string { return proto.CompactTextString(m) }
func (*ReceivePackRequest) ProtoMessage()    {}
func (*ReceivePackRequest) Descriptor() ([]byte, []int) {
	return fileDescriptor_server_f3182e3be274f50f, []int{4}
}
func (m *ReceivePackRequest) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_ReceivePackRequest.Unmarshal(m, b)
}
func (m *ReceivePackRequest) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_ReceivePackRequest.Marshal(b, m, deterministic)
}
func (dst *ReceivePackRequest) XXX_Merge(src proto.Message) {
	xxx_messageInfo_ReceivePackRequest.Merge(dst, src)
}
func (m *ReceivePackRequest) XXX_Size() int {
	return xxx_messageInfo_ReceivePackRequest.Size(m)
}
func (m *ReceivePackRequest) XXX_DiscardUnknown() {
	xxx_messageInfo_ReceivePackRequest.DiscardUnknown(m)
}

var xxx_messageInfo_ReceivePackRequest proto.InternalMessageInfo

func (m *ReceivePackRequest) GetRepository() *Repository {
	if m != nil {
		return m.Repository
	}
	return nil
}

func (m *ReceivePackRequest) GetOids() []string {
	if m != nil {
		return m.Oids
	}
	return nil
}

func (m *ReceivePackRequest) GetDepth() int32 {
	if m != nil {
		return m.Depth
	}
	return 0
}

type ReceivePackResponse struct {
	Data                 []byte   `protobuf:"bytes,1,opt,name=data,proto3" json:"data,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *ReceivePackResponse) Reset()         { *m = ReceivePackResponse{} }
func (m *ReceivePackResponse) String() string { return proto.CompactTextString(m) }
func (*ReceivePackResponse) ProtoMessage()    {}
func (*ReceivePackResponse) Descriptor() ([]byte, []int) {
	return fileDescriptor_server_f3182e3be274f50f, []int{5}
}
func (m *ReceivePackResponse) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_ReceivePackResponse.Unmarshal(m, b)
}
func (m *ReceivePackResponse) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_ReceivePackResponse.Marshal(b, m, deterministic)
}
func (dst *ReceivePackResponse) XXX_Merge(src proto.Message) {
	xxx_messageInfo_ReceivePackResponse.Merge(dst, src)
}
func (m *ReceivePackResponse) XXX_Size() int {
	return xxx_messageInfo_ReceivePackResponse.Size(m)
}
func (m *ReceivePackResponse) XXX_DiscardUnknown() {
	xxx_messageInfo_ReceivePackResponse.DiscardUnknown(m)
}

var xxx_messageInfo_ReceivePackResponse proto.InternalMessageInfo

func (m *ReceivePackResponse) GetData() []byte {
	if m != nil {
		return m.Data
	}
	return nil
}

func init() {
	proto.RegisterType((*ServerInfoRequest)(nil), "gitaly.ServerInfoRequest")
	proto.RegisterType((*ServerInfoResponse)(nil), "gitaly.ServerInfoResponse")
	proto.RegisterType((*ServerInfoResponse_StorageStatus)(nil), "gitaly.ServerInfoResponse.StorageStatus")
	proto.RegisterType((*GetObjectsRequest)(nil), "gitaly.GetObjectsRequest")
	proto.RegisterType((*GetObjectsResponse)(nil), "gitaly.GetObjectsResponse")
	proto.RegisterType((*ReceivePackRequest)(nil), "gitaly.ReceivePackRequest")
	proto.RegisterType((*ReceivePackResponse)(nil), "gitaly.ReceivePackResponse")
}

// Reference imports to suppress errors if they are not otherwise used.
var _ context.Context
var _ grpc.ClientConn

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
const _ = grpc.SupportPackageIsVersion4

// ServerServiceClient is the client API for ServerService service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://godoc.org/google.golang.org/grpc#ClientConn.NewStream.
type ServerServiceClient interface {
	ServerInfo(ctx context.Context, in *ServerInfoRequest, opts ...grpc.CallOption) (*ServerInfoResponse, error)
	GetObjects(ctx context.Context, in *GetObjectsRequest, opts ...grpc.CallOption) (ServerService_GetObjectsClient, error)
	ReceivePack(ctx context.Context, in *ReceivePackRequest, opts ...grpc.CallOption) (ServerService_ReceivePackClient, error)
}

type serverServiceClient struct {
	cc *grpc.ClientConn
}

func NewServerServiceClient(cc *grpc.ClientConn) ServerServiceClient {
	return &serverServiceClient{cc}
}

func (c *serverServiceClient) ServerInfo(ctx context.Context, in *ServerInfoRequest, opts ...grpc.CallOption) (*ServerInfoResponse, error) {
	out := new(ServerInfoResponse)
	err := c.cc.Invoke(ctx, "/gitaly.ServerService/ServerInfo", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *serverServiceClient) GetObjects(ctx context.Context, in *GetObjectsRequest, opts ...grpc.CallOption) (ServerService_GetObjectsClient, error) {
	stream, err := c.cc.NewStream(ctx, &_ServerService_serviceDesc.Streams[0], "/gitaly.ServerService/GetObjects", opts...)
	if err != nil {
		return nil, err
	}
	x := &serverServiceGetObjectsClient{stream}
	if err := x.ClientStream.SendMsg(in); err != nil {
		return nil, err
	}
	if err := x.ClientStream.CloseSend(); err != nil {
		return nil, err
	}
	return x, nil
}

type ServerService_GetObjectsClient interface {
	Recv() (*GetObjectsResponse, error)
	grpc.ClientStream
}

type serverServiceGetObjectsClient struct {
	grpc.ClientStream
}

func (x *serverServiceGetObjectsClient) Recv() (*GetObjectsResponse, error) {
	m := new(GetObjectsResponse)
	if err := x.ClientStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

func (c *serverServiceClient) ReceivePack(ctx context.Context, in *ReceivePackRequest, opts ...grpc.CallOption) (ServerService_ReceivePackClient, error) {
	stream, err := c.cc.NewStream(ctx, &_ServerService_serviceDesc.Streams[1], "/gitaly.ServerService/ReceivePack", opts...)
	if err != nil {
		return nil, err
	}
	x := &serverServiceReceivePackClient{stream}
	if err := x.ClientStream.SendMsg(in); err != nil {
		return nil, err
	}
	if err := x.ClientStream.CloseSend(); err != nil {
		return nil, err
	}
	return x, nil
}

type ServerService_ReceivePackClient interface {
	Recv() (*ReceivePackResponse, error)
	grpc.ClientStream
}

type serverServiceReceivePackClient struct {
	grpc.ClientStream
}

func (x *serverServiceReceivePackClient) Recv() (*ReceivePackResponse, error) {
	m := new(ReceivePackResponse)
	if err := x.ClientStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

// ServerServiceServer is the server API for ServerService service.
type ServerServiceServer interface {
	ServerInfo(context.Context, *ServerInfoRequest) (*ServerInfoResponse, error)
	GetObjects(*GetObjectsRequest, ServerService_GetObjectsServer) error
	ReceivePack(*ReceivePackRequest, ServerService_ReceivePackServer) error
}

func RegisterServerServiceServer(s *grpc.Server, srv ServerServiceServer) {
	s.RegisterService(&_ServerService_serviceDesc, srv)
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
		FullMethod: "/gitaly.ServerService/ServerInfo",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ServerServiceServer).ServerInfo(ctx, req.(*ServerInfoRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _ServerService_GetObjects_Handler(srv interface{}, stream grpc.ServerStream) error {
	m := new(GetObjectsRequest)
	if err := stream.RecvMsg(m); err != nil {
		return err
	}
	return srv.(ServerServiceServer).GetObjects(m, &serverServiceGetObjectsServer{stream})
}

type ServerService_GetObjectsServer interface {
	Send(*GetObjectsResponse) error
	grpc.ServerStream
}

type serverServiceGetObjectsServer struct {
	grpc.ServerStream
}

func (x *serverServiceGetObjectsServer) Send(m *GetObjectsResponse) error {
	return x.ServerStream.SendMsg(m)
}

func _ServerService_ReceivePack_Handler(srv interface{}, stream grpc.ServerStream) error {
	m := new(ReceivePackRequest)
	if err := stream.RecvMsg(m); err != nil {
		return err
	}
	return srv.(ServerServiceServer).ReceivePack(m, &serverServiceReceivePackServer{stream})
}

type ServerService_ReceivePackServer interface {
	Send(*ReceivePackResponse) error
	grpc.ServerStream
}

type serverServiceReceivePackServer struct {
	grpc.ServerStream
}

func (x *serverServiceReceivePackServer) Send(m *ReceivePackResponse) error {
	return x.ServerStream.SendMsg(m)
}

var _ServerService_serviceDesc = grpc.ServiceDesc{
	ServiceName: "gitaly.ServerService",
	HandlerType: (*ServerServiceServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "ServerInfo",
			Handler:    _ServerService_ServerInfo_Handler,
		},
	},
	Streams: []grpc.StreamDesc{
		{
			StreamName:    "GetObjects",
			Handler:       _ServerService_GetObjects_Handler,
			ServerStreams: true,
		},
		{
			StreamName:    "ReceivePack",
			Handler:       _ServerService_ReceivePack_Handler,
			ServerStreams: true,
		},
	},
	Metadata: "server.proto",
}

func init() { proto.RegisterFile("server.proto", fileDescriptor_server_f3182e3be274f50f) }

var fileDescriptor_server_f3182e3be274f50f = []byte{
	// 485 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0xac, 0x53, 0xc1, 0x6e, 0xd3, 0x40,
	0x10, 0x95, 0xed, 0x34, 0x4a, 0xc6, 0x49, 0x69, 0xa7, 0x1c, 0x82, 0x41, 0x22, 0x58, 0x02, 0x99,
	0x43, 0x9d, 0x2a, 0xfc, 0x01, 0x17, 0x84, 0x10, 0x50, 0x6d, 0x10, 0x07, 0x38, 0x54, 0x6b, 0x7b,
	0x70, 0x17, 0x92, 0xac, 0xd9, 0xdd, 0x06, 0x85, 0x1f, 0xe1, 0xc8, 0x7f, 0x22, 0x21, 0xa1, 0xec,
	0x3a, 0xb1, 0xdb, 0x94, 0x5b, 0x2f, 0xd1, 0xcc, 0x9b, 0xb7, 0x33, 0xef, 0x8d, 0x27, 0x30, 0xd0,
	0xa4, 0x56, 0xa4, 0xd2, 0x4a, 0x49, 0x23, 0xb1, 0x5b, 0x0a, 0xc3, 0xe7, 0xeb, 0x68, 0xa0, 0x2f,
	0xb9, 0xa2, 0xc2, 0xa1, 0xf1, 0x09, 0x1c, 0xcf, 0x2c, 0xeb, 0xf5, 0xf2, 0x8b, 0x64, 0xf4, 0xfd,
	0x8a, 0xb4, 0x89, 0x7f, 0xfb, 0x80, 0x6d, 0x54, 0x57, 0x72, 0xa9, 0x09, 0x9f, 0xc2, 0xa1, 0xeb,
	0x78, 0xb1, 0x22, 0xa5, 0x85, 0x5c, 0x8e, 0xbc, 0xb1, 0x97, 0xf4, 0xd9, 0xd0, 0xa1, 0x1f, 0x1d,
	0x88, 0x8f, 0x21, 0x2c, 0x85, 0xd9, 0x71, 0x7c, 0xcb, 0x81, 0x52, 0x98, 0x2d, 0x61, 0x06, 0x47,
	0xda, 0x48, 0xc5, 0x4b, 0xba, 0xd0, 0x86, 0x9b, 0x2b, 0x4d, 0x7a, 0x14, 0x8c, 0x83, 0x24, 0x9c,
	0x26, 0xa9, 0x13, 0x99, 0xee, 0x4f, 0x4f, 0x67, 0xee, 0xc9, 0xcc, 0xbe, 0x60, 0xf7, 0x74, 0x3b,
	0x25, 0x1d, 0xcd, 0x61, 0x78, 0x8d, 0x81, 0x4f, 0x60, 0xb0, 0x9d, 0xb2, 0xe4, 0x0b, 0xaa, 0xb5,
	0x86, 0x35, 0xf6, 0x8e, 0x2f, 0x08, 0x23, 0xe8, 0x29, 0xe2, 0x05, 0xcf, 0xe6, 0x64, 0x65, 0xf6,
	0xd8, 0x2e, 0xc7, 0x47, 0xd0, 0xff, 0xa1, 0x84, 0x21, 0x5b, 0x0c, 0x6c, 0xb1, 0x01, 0xe2, 0xcf,
	0x70, 0xfc, 0x8a, 0xcc, 0xfb, 0xec, 0x2b, 0xe5, 0x46, 0xd7, 0x6b, 0xc3, 0x29, 0x80, 0xa2, 0x4a,
	0x6a, 0x61, 0xa4, 0x5a, 0xdb, 0x79, 0xe1, 0x14, 0xb7, 0x8e, 0xd8, 0xae, 0xc2, 0x5a, 0x2c, 0x44,
	0xe8, 0x48, 0x51, 0xe8, 0x91, 0x3f, 0x0e, 0x92, 0x3e, 0xb3, 0x71, 0xbc, 0x02, 0x6c, 0x37, 0xaf,
	0xb7, 0x8f, 0xd0, 0xd1, 0xe2, 0xa7, 0xf3, 0x11, 0x30, 0x1b, 0x6f, 0xb0, 0x82, 0x1b, 0x6e, 0xc5,
	0x0f, 0x98, 0x8d, 0xf1, 0x08, 0x02, 0x29, 0x0a, 0x2b, 0xb9, 0xcf, 0x36, 0x21, 0x3e, 0x83, 0x8e,
	0x59, 0x57, 0x34, 0xea, 0x8c, 0xbd, 0xe4, 0xb0, 0x51, 0xe4, 0x06, 0x7c, 0x58, 0x57, 0xc4, 0x6c,
	0x3d, 0x56, 0x80, 0x8c, 0x72, 0x12, 0x2b, 0x3a, 0xe7, 0xf9, 0xb7, 0x3b, 0x76, 0x85, 0xf7, 0xe1,
	0xa0, 0xa0, 0xca, 0x5c, 0x5a, 0x65, 0x07, 0xcc, 0x25, 0xf1, 0x73, 0x38, 0xb9, 0x36, 0xb3, 0x31,
	0x6b, 0x8d, 0x79, 0x8d, 0xb1, 0xe9, 0x5f, 0x0f, 0x86, 0xee, 0x2e, 0x36, 0xbf, 0x22, 0x27, 0x7c,
	0x03, 0xd0, 0x1c, 0x0a, 0x3e, 0xb8, 0xed, 0x78, 0xac, 0x87, 0x28, 0xfa, 0xff, 0x5d, 0xc5, 0xdd,
	0x3f, 0xbf, 0x12, 0xbf, 0xe7, 0xe3, 0x5b, 0x80, 0x66, 0xeb, 0x4d, 0xb3, 0xbd, 0xcf, 0xdc, 0x34,
	0xdb, 0xff, 0x48, 0xdb, 0x66, 0x67, 0x1e, 0x9e, 0x43, 0xd8, 0x32, 0x86, 0x51, 0xb3, 0xb1, 0x9b,
	0x1b, 0x8e, 0x1e, 0xde, 0x5a, 0xbb, 0xd9, 0xf1, 0xe5, 0xd9, 0xa7, 0x0d, 0x6f, 0xce, 0xb3, 0x34,
	0x97, 0x8b, 0x89, 0x0b, 0x4f, 0xa5, 0x2a, 0x27, 0xee, 0xf5, 0xa9, 0xfd, 0x43, 0x4f, 0x4a, 0x59,
	0xe7, 0x55, 0x96, 0x75, 0x2d, 0xf4, 0xe2, 0x5f, 0x00, 0x00, 0x00, 0xff, 0xff, 0x37, 0x1c, 0x2d,
	0x13, 0x09, 0x04, 0x00, 0x00,
}
