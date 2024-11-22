// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.35.2
// 	protoc        v4.23.1
// source: partition.proto

package gitalypb

import (
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	reflect "reflect"
	sync "sync"
)

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

// BackupPartitionRequest is a request for the BackupPartition RPC.
type BackupPartitionRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	// storage_name is the name of the storage containing the partition to be backed up.
	StorageName string `protobuf:"bytes,1,opt,name=storage_name,json=storageName,proto3" json:"storage_name,omitempty"`
	// partition_id is the identifier of the specific partition within the storage to
	// be backed up, which may contain one or more repositories and associated data.
	PartitionId string `protobuf:"bytes,2,opt,name=partition_id,json=partitionId,proto3" json:"partition_id,omitempty"`
}

func (x *BackupPartitionRequest) Reset() {
	*x = BackupPartitionRequest{}
	mi := &file_partition_proto_msgTypes[0]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *BackupPartitionRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*BackupPartitionRequest) ProtoMessage() {}

func (x *BackupPartitionRequest) ProtoReflect() protoreflect.Message {
	mi := &file_partition_proto_msgTypes[0]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use BackupPartitionRequest.ProtoReflect.Descriptor instead.
func (*BackupPartitionRequest) Descriptor() ([]byte, []int) {
	return file_partition_proto_rawDescGZIP(), []int{0}
}

func (x *BackupPartitionRequest) GetStorageName() string {
	if x != nil {
		return x.StorageName
	}
	return ""
}

func (x *BackupPartitionRequest) GetPartitionId() string {
	if x != nil {
		return x.PartitionId
	}
	return ""
}

// BackupPartitionResponse is a response for the BackupPartition RPC.
type BackupPartitionResponse struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields
}

func (x *BackupPartitionResponse) Reset() {
	*x = BackupPartitionResponse{}
	mi := &file_partition_proto_msgTypes[1]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *BackupPartitionResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*BackupPartitionResponse) ProtoMessage() {}

func (x *BackupPartitionResponse) ProtoReflect() protoreflect.Message {
	mi := &file_partition_proto_msgTypes[1]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use BackupPartitionResponse.ProtoReflect.Descriptor instead.
func (*BackupPartitionResponse) Descriptor() ([]byte, []int) {
	return file_partition_proto_rawDescGZIP(), []int{1}
}

// ListPartitionsRequest is a request for the ListPartitions RPC.
type ListPartitionsRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	// storage_name is the name of the storage in which partitions will be searched for.
	StorageName string `protobuf:"bytes,1,opt,name=storage_name,json=storageName,proto3" json:"storage_name,omitempty"`
	// pagination_params controls paging. Refer to PaginationParameter documentation for
	// further info.
	PaginationParams *PaginationParameter `protobuf:"bytes,2,opt,name=pagination_params,json=paginationParams,proto3" json:"pagination_params,omitempty"`
}

func (x *ListPartitionsRequest) Reset() {
	*x = ListPartitionsRequest{}
	mi := &file_partition_proto_msgTypes[2]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *ListPartitionsRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*ListPartitionsRequest) ProtoMessage() {}

func (x *ListPartitionsRequest) ProtoReflect() protoreflect.Message {
	mi := &file_partition_proto_msgTypes[2]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use ListPartitionsRequest.ProtoReflect.Descriptor instead.
func (*ListPartitionsRequest) Descriptor() ([]byte, []int) {
	return file_partition_proto_rawDescGZIP(), []int{2}
}

func (x *ListPartitionsRequest) GetStorageName() string {
	if x != nil {
		return x.StorageName
	}
	return ""
}

func (x *ListPartitionsRequest) GetPaginationParams() *PaginationParameter {
	if x != nil {
		return x.PaginationParams
	}
	return nil
}

// ListPartitionsResponse is a response for the ListPartitions RPC.
type ListPartitionsResponse struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	// partitions is the list of partitions found.
	Partitions []*Partition `protobuf:"bytes,1,rep,name=partitions,proto3" json:"partitions,omitempty"`
	// pagination_cursor contains the page token to fetch the next page of results. Refer to PaginationCursor
	// documentation for further info.
	PaginationCursor *PaginationCursor `protobuf:"bytes,2,opt,name=pagination_cursor,json=paginationCursor,proto3" json:"pagination_cursor,omitempty"`
}

func (x *ListPartitionsResponse) Reset() {
	*x = ListPartitionsResponse{}
	mi := &file_partition_proto_msgTypes[3]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *ListPartitionsResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*ListPartitionsResponse) ProtoMessage() {}

func (x *ListPartitionsResponse) ProtoReflect() protoreflect.Message {
	mi := &file_partition_proto_msgTypes[3]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use ListPartitionsResponse.ProtoReflect.Descriptor instead.
func (*ListPartitionsResponse) Descriptor() ([]byte, []int) {
	return file_partition_proto_rawDescGZIP(), []int{3}
}

func (x *ListPartitionsResponse) GetPartitions() []*Partition {
	if x != nil {
		return x.Partitions
	}
	return nil
}

func (x *ListPartitionsResponse) GetPaginationCursor() *PaginationCursor {
	if x != nil {
		return x.PaginationCursor
	}
	return nil
}

var File_partition_proto protoreflect.FileDescriptor

var file_partition_proto_rawDesc = []byte{
	0x0a, 0x0f, 0x70, 0x61, 0x72, 0x74, 0x69, 0x74, 0x69, 0x6f, 0x6e, 0x2e, 0x70, 0x72, 0x6f, 0x74,
	0x6f, 0x12, 0x06, 0x67, 0x69, 0x74, 0x61, 0x6c, 0x79, 0x1a, 0x0a, 0x6c, 0x69, 0x6e, 0x74, 0x2e,
	0x70, 0x72, 0x6f, 0x74, 0x6f, 0x1a, 0x0c, 0x73, 0x68, 0x61, 0x72, 0x65, 0x64, 0x2e, 0x70, 0x72,
	0x6f, 0x74, 0x6f, 0x22, 0x6a, 0x0a, 0x16, 0x42, 0x61, 0x63, 0x6b, 0x75, 0x70, 0x50, 0x61, 0x72,
	0x74, 0x69, 0x74, 0x69, 0x6f, 0x6e, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x12, 0x27, 0x0a,
	0x0c, 0x73, 0x74, 0x6f, 0x72, 0x61, 0x67, 0x65, 0x5f, 0x6e, 0x61, 0x6d, 0x65, 0x18, 0x01, 0x20,
	0x01, 0x28, 0x09, 0x42, 0x04, 0x88, 0xc6, 0x2c, 0x01, 0x52, 0x0b, 0x73, 0x74, 0x6f, 0x72, 0x61,
	0x67, 0x65, 0x4e, 0x61, 0x6d, 0x65, 0x12, 0x27, 0x0a, 0x0c, 0x70, 0x61, 0x72, 0x74, 0x69, 0x74,
	0x69, 0x6f, 0x6e, 0x5f, 0x69, 0x64, 0x18, 0x02, 0x20, 0x01, 0x28, 0x09, 0x42, 0x04, 0xa8, 0xc6,
	0x2c, 0x01, 0x52, 0x0b, 0x70, 0x61, 0x72, 0x74, 0x69, 0x74, 0x69, 0x6f, 0x6e, 0x49, 0x64, 0x22,
	0x19, 0x0a, 0x17, 0x42, 0x61, 0x63, 0x6b, 0x75, 0x70, 0x50, 0x61, 0x72, 0x74, 0x69, 0x74, 0x69,
	0x6f, 0x6e, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x22, 0x8a, 0x01, 0x0a, 0x15, 0x4c,
	0x69, 0x73, 0x74, 0x50, 0x61, 0x72, 0x74, 0x69, 0x74, 0x69, 0x6f, 0x6e, 0x73, 0x52, 0x65, 0x71,
	0x75, 0x65, 0x73, 0x74, 0x12, 0x27, 0x0a, 0x0c, 0x73, 0x74, 0x6f, 0x72, 0x61, 0x67, 0x65, 0x5f,
	0x6e, 0x61, 0x6d, 0x65, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x42, 0x04, 0x88, 0xc6, 0x2c, 0x01,
	0x52, 0x0b, 0x73, 0x74, 0x6f, 0x72, 0x61, 0x67, 0x65, 0x4e, 0x61, 0x6d, 0x65, 0x12, 0x48, 0x0a,
	0x11, 0x70, 0x61, 0x67, 0x69, 0x6e, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x5f, 0x70, 0x61, 0x72, 0x61,
	0x6d, 0x73, 0x18, 0x02, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x1b, 0x2e, 0x67, 0x69, 0x74, 0x61, 0x6c,
	0x79, 0x2e, 0x50, 0x61, 0x67, 0x69, 0x6e, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x50, 0x61, 0x72, 0x61,
	0x6d, 0x65, 0x74, 0x65, 0x72, 0x52, 0x10, 0x70, 0x61, 0x67, 0x69, 0x6e, 0x61, 0x74, 0x69, 0x6f,
	0x6e, 0x50, 0x61, 0x72, 0x61, 0x6d, 0x73, 0x22, 0x92, 0x01, 0x0a, 0x16, 0x4c, 0x69, 0x73, 0x74,
	0x50, 0x61, 0x72, 0x74, 0x69, 0x74, 0x69, 0x6f, 0x6e, 0x73, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e,
	0x73, 0x65, 0x12, 0x31, 0x0a, 0x0a, 0x70, 0x61, 0x72, 0x74, 0x69, 0x74, 0x69, 0x6f, 0x6e, 0x73,
	0x18, 0x01, 0x20, 0x03, 0x28, 0x0b, 0x32, 0x11, 0x2e, 0x67, 0x69, 0x74, 0x61, 0x6c, 0x79, 0x2e,
	0x50, 0x61, 0x72, 0x74, 0x69, 0x74, 0x69, 0x6f, 0x6e, 0x52, 0x0a, 0x70, 0x61, 0x72, 0x74, 0x69,
	0x74, 0x69, 0x6f, 0x6e, 0x73, 0x12, 0x45, 0x0a, 0x11, 0x70, 0x61, 0x67, 0x69, 0x6e, 0x61, 0x74,
	0x69, 0x6f, 0x6e, 0x5f, 0x63, 0x75, 0x72, 0x73, 0x6f, 0x72, 0x18, 0x02, 0x20, 0x01, 0x28, 0x0b,
	0x32, 0x18, 0x2e, 0x67, 0x69, 0x74, 0x61, 0x6c, 0x79, 0x2e, 0x50, 0x61, 0x67, 0x69, 0x6e, 0x61,
	0x74, 0x69, 0x6f, 0x6e, 0x43, 0x75, 0x72, 0x73, 0x6f, 0x72, 0x52, 0x10, 0x70, 0x61, 0x67, 0x69,
	0x6e, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x43, 0x75, 0x72, 0x73, 0x6f, 0x72, 0x32, 0xcb, 0x01, 0x0a,
	0x10, 0x50, 0x61, 0x72, 0x74, 0x69, 0x74, 0x69, 0x6f, 0x6e, 0x53, 0x65, 0x72, 0x76, 0x69, 0x63,
	0x65, 0x12, 0x5c, 0x0a, 0x0f, 0x42, 0x61, 0x63, 0x6b, 0x75, 0x70, 0x50, 0x61, 0x72, 0x74, 0x69,
	0x74, 0x69, 0x6f, 0x6e, 0x12, 0x1e, 0x2e, 0x67, 0x69, 0x74, 0x61, 0x6c, 0x79, 0x2e, 0x42, 0x61,
	0x63, 0x6b, 0x75, 0x70, 0x50, 0x61, 0x72, 0x74, 0x69, 0x74, 0x69, 0x6f, 0x6e, 0x52, 0x65, 0x71,
	0x75, 0x65, 0x73, 0x74, 0x1a, 0x1f, 0x2e, 0x67, 0x69, 0x74, 0x61, 0x6c, 0x79, 0x2e, 0x42, 0x61,
	0x63, 0x6b, 0x75, 0x70, 0x50, 0x61, 0x72, 0x74, 0x69, 0x74, 0x69, 0x6f, 0x6e, 0x52, 0x65, 0x73,
	0x70, 0x6f, 0x6e, 0x73, 0x65, 0x22, 0x08, 0xfa, 0x97, 0x28, 0x04, 0x08, 0x02, 0x10, 0x03, 0x12,
	0x59, 0x0a, 0x0e, 0x4c, 0x69, 0x73, 0x74, 0x50, 0x61, 0x72, 0x74, 0x69, 0x74, 0x69, 0x6f, 0x6e,
	0x73, 0x12, 0x1d, 0x2e, 0x67, 0x69, 0x74, 0x61, 0x6c, 0x79, 0x2e, 0x4c, 0x69, 0x73, 0x74, 0x50,
	0x61, 0x72, 0x74, 0x69, 0x74, 0x69, 0x6f, 0x6e, 0x73, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74,
	0x1a, 0x1e, 0x2e, 0x67, 0x69, 0x74, 0x61, 0x6c, 0x79, 0x2e, 0x4c, 0x69, 0x73, 0x74, 0x50, 0x61,
	0x72, 0x74, 0x69, 0x74, 0x69, 0x6f, 0x6e, 0x73, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65,
	0x22, 0x08, 0xfa, 0x97, 0x28, 0x04, 0x08, 0x02, 0x10, 0x02, 0x42, 0x34, 0x5a, 0x32, 0x67, 0x69,
	0x74, 0x6c, 0x61, 0x62, 0x2e, 0x63, 0x6f, 0x6d, 0x2f, 0x67, 0x69, 0x74, 0x6c, 0x61, 0x62, 0x2d,
	0x6f, 0x72, 0x67, 0x2f, 0x67, 0x69, 0x74, 0x61, 0x6c, 0x79, 0x2f, 0x76, 0x31, 0x36, 0x2f, 0x70,
	0x72, 0x6f, 0x74, 0x6f, 0x2f, 0x67, 0x6f, 0x2f, 0x67, 0x69, 0x74, 0x61, 0x6c, 0x79, 0x70, 0x62,
	0x62, 0x06, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x33,
}

var (
	file_partition_proto_rawDescOnce sync.Once
	file_partition_proto_rawDescData = file_partition_proto_rawDesc
)

func file_partition_proto_rawDescGZIP() []byte {
	file_partition_proto_rawDescOnce.Do(func() {
		file_partition_proto_rawDescData = protoimpl.X.CompressGZIP(file_partition_proto_rawDescData)
	})
	return file_partition_proto_rawDescData
}

var file_partition_proto_msgTypes = make([]protoimpl.MessageInfo, 4)
var file_partition_proto_goTypes = []any{
	(*BackupPartitionRequest)(nil),  // 0: gitaly.BackupPartitionRequest
	(*BackupPartitionResponse)(nil), // 1: gitaly.BackupPartitionResponse
	(*ListPartitionsRequest)(nil),   // 2: gitaly.ListPartitionsRequest
	(*ListPartitionsResponse)(nil),  // 3: gitaly.ListPartitionsResponse
	(*PaginationParameter)(nil),     // 4: gitaly.PaginationParameter
	(*Partition)(nil),               // 5: gitaly.Partition
	(*PaginationCursor)(nil),        // 6: gitaly.PaginationCursor
}
var file_partition_proto_depIdxs = []int32{
	4, // 0: gitaly.ListPartitionsRequest.pagination_params:type_name -> gitaly.PaginationParameter
	5, // 1: gitaly.ListPartitionsResponse.partitions:type_name -> gitaly.Partition
	6, // 2: gitaly.ListPartitionsResponse.pagination_cursor:type_name -> gitaly.PaginationCursor
	0, // 3: gitaly.PartitionService.BackupPartition:input_type -> gitaly.BackupPartitionRequest
	2, // 4: gitaly.PartitionService.ListPartitions:input_type -> gitaly.ListPartitionsRequest
	1, // 5: gitaly.PartitionService.BackupPartition:output_type -> gitaly.BackupPartitionResponse
	3, // 6: gitaly.PartitionService.ListPartitions:output_type -> gitaly.ListPartitionsResponse
	5, // [5:7] is the sub-list for method output_type
	3, // [3:5] is the sub-list for method input_type
	3, // [3:3] is the sub-list for extension type_name
	3, // [3:3] is the sub-list for extension extendee
	0, // [0:3] is the sub-list for field type_name
}

func init() { file_partition_proto_init() }
func file_partition_proto_init() {
	if File_partition_proto != nil {
		return
	}
	file_lint_proto_init()
	file_shared_proto_init()
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: file_partition_proto_rawDesc,
			NumEnums:      0,
			NumMessages:   4,
			NumExtensions: 0,
			NumServices:   1,
		},
		GoTypes:           file_partition_proto_goTypes,
		DependencyIndexes: file_partition_proto_depIdxs,
		MessageInfos:      file_partition_proto_msgTypes,
	}.Build()
	File_partition_proto = out.File
	file_partition_proto_rawDesc = nil
	file_partition_proto_goTypes = nil
	file_partition_proto_depIdxs = nil
}
