// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.30.0
// 	protoc        v4.23.1
// source: packfile.proto

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

type Stats struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	// total size of all pktlines' data
	PayloadSize int64 `protobuf:"varint,1,opt,name=PayloadSize,proto3" json:"PayloadSize,omitempty"`
	// total number of packets
	Packets int64 `protobuf:"varint,2,opt,name=Packets,proto3" json:"Packets,omitempty"`
	// capabilities announced by the client
	Caps []string `protobuf:"bytes,3,rep,name=caps,proto3" json:"caps,omitempty"`
	// wants is the number of objects the client announced it wants
	Wants int64 `protobuf:"varint,4,opt,name=wants,proto3" json:"wants,omitempty"`
	// haves is the number of objects the client announced it has
	Haves int64 `protobuf:"varint,5,opt,name=Haves,proto3" json:"Haves,omitempty"`
	// shallows is the number of shallow boundaries announced by the client
	Shallows int64 `protobuf:"varint,6,opt,name=shallows,proto3" json:"shallows,omitempty"`
	// deepen-filter. One of "deepen <depth>", "deepen-since <timestamp>", "deepen-not <ref>".
	Deepen string `protobuf:"bytes,7,opt,name=deepen,proto3" json:"deepen,omitempty"`
	// filter-spec specified by the client.
	Filter string `protobuf:"bytes,8,opt,name=filter,proto3" json:"filter,omitempty"`
}

func (x *Stats) Reset() {
	*x = Stats{}
	if protoimpl.UnsafeEnabled {
		mi := &file_packfile_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *Stats) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Stats) ProtoMessage() {}

func (x *Stats) ProtoReflect() protoreflect.Message {
	mi := &file_packfile_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Stats.ProtoReflect.Descriptor instead.
func (*Stats) Descriptor() ([]byte, []int) {
	return file_packfile_proto_rawDescGZIP(), []int{0}
}

func (x *Stats) GetPayloadSize() int64 {
	if x != nil {
		return x.PayloadSize
	}
	return 0
}

func (x *Stats) GetPackets() int64 {
	if x != nil {
		return x.Packets
	}
	return 0
}

func (x *Stats) GetCaps() []string {
	if x != nil {
		return x.Caps
	}
	return nil
}

func (x *Stats) GetWants() int64 {
	if x != nil {
		return x.Wants
	}
	return 0
}

func (x *Stats) GetHaves() int64 {
	if x != nil {
		return x.Haves
	}
	return 0
}

func (x *Stats) GetShallows() int64 {
	if x != nil {
		return x.Shallows
	}
	return 0
}

func (x *Stats) GetDeepen() string {
	if x != nil {
		return x.Deepen
	}
	return ""
}

func (x *Stats) GetFilter() string {
	if x != nil {
		return x.Filter
	}
	return ""
}

var File_packfile_proto protoreflect.FileDescriptor

var file_packfile_proto_rawDesc = []byte{
	0x0a, 0x0e, 0x70, 0x61, 0x63, 0x6b, 0x66, 0x69, 0x6c, 0x65, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f,
	0x12, 0x06, 0x67, 0x69, 0x74, 0x61, 0x6c, 0x79, 0x22, 0xcf, 0x01, 0x0a, 0x05, 0x53, 0x74, 0x61,
	0x74, 0x73, 0x12, 0x20, 0x0a, 0x0b, 0x50, 0x61, 0x79, 0x6c, 0x6f, 0x61, 0x64, 0x53, 0x69, 0x7a,
	0x65, 0x18, 0x01, 0x20, 0x01, 0x28, 0x03, 0x52, 0x0b, 0x50, 0x61, 0x79, 0x6c, 0x6f, 0x61, 0x64,
	0x53, 0x69, 0x7a, 0x65, 0x12, 0x18, 0x0a, 0x07, 0x50, 0x61, 0x63, 0x6b, 0x65, 0x74, 0x73, 0x18,
	0x02, 0x20, 0x01, 0x28, 0x03, 0x52, 0x07, 0x50, 0x61, 0x63, 0x6b, 0x65, 0x74, 0x73, 0x12, 0x12,
	0x0a, 0x04, 0x63, 0x61, 0x70, 0x73, 0x18, 0x03, 0x20, 0x03, 0x28, 0x09, 0x52, 0x04, 0x63, 0x61,
	0x70, 0x73, 0x12, 0x14, 0x0a, 0x05, 0x77, 0x61, 0x6e, 0x74, 0x73, 0x18, 0x04, 0x20, 0x01, 0x28,
	0x03, 0x52, 0x05, 0x77, 0x61, 0x6e, 0x74, 0x73, 0x12, 0x14, 0x0a, 0x05, 0x48, 0x61, 0x76, 0x65,
	0x73, 0x18, 0x05, 0x20, 0x01, 0x28, 0x03, 0x52, 0x05, 0x48, 0x61, 0x76, 0x65, 0x73, 0x12, 0x1a,
	0x0a, 0x08, 0x73, 0x68, 0x61, 0x6c, 0x6c, 0x6f, 0x77, 0x73, 0x18, 0x06, 0x20, 0x01, 0x28, 0x03,
	0x52, 0x08, 0x73, 0x68, 0x61, 0x6c, 0x6c, 0x6f, 0x77, 0x73, 0x12, 0x16, 0x0a, 0x06, 0x64, 0x65,
	0x65, 0x70, 0x65, 0x6e, 0x18, 0x07, 0x20, 0x01, 0x28, 0x09, 0x52, 0x06, 0x64, 0x65, 0x65, 0x70,
	0x65, 0x6e, 0x12, 0x16, 0x0a, 0x06, 0x66, 0x69, 0x6c, 0x74, 0x65, 0x72, 0x18, 0x08, 0x20, 0x01,
	0x28, 0x09, 0x52, 0x06, 0x66, 0x69, 0x6c, 0x74, 0x65, 0x72, 0x42, 0x34, 0x5a, 0x32, 0x67, 0x69,
	0x74, 0x6c, 0x61, 0x62, 0x2e, 0x63, 0x6f, 0x6d, 0x2f, 0x67, 0x69, 0x74, 0x6c, 0x61, 0x62, 0x2d,
	0x6f, 0x72, 0x67, 0x2f, 0x67, 0x69, 0x74, 0x61, 0x6c, 0x79, 0x2f, 0x76, 0x31, 0x36, 0x2f, 0x70,
	0x72, 0x6f, 0x74, 0x6f, 0x2f, 0x67, 0x6f, 0x2f, 0x67, 0x69, 0x74, 0x61, 0x6c, 0x79, 0x70, 0x62,
	0x62, 0x06, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x33,
}

var (
	file_packfile_proto_rawDescOnce sync.Once
	file_packfile_proto_rawDescData = file_packfile_proto_rawDesc
)

func file_packfile_proto_rawDescGZIP() []byte {
	file_packfile_proto_rawDescOnce.Do(func() {
		file_packfile_proto_rawDescData = protoimpl.X.CompressGZIP(file_packfile_proto_rawDescData)
	})
	return file_packfile_proto_rawDescData
}

var file_packfile_proto_msgTypes = make([]protoimpl.MessageInfo, 1)
var file_packfile_proto_goTypes = []interface{}{
	(*Stats)(nil), // 0: gitaly.Stats
}
var file_packfile_proto_depIdxs = []int32{
	0, // [0:0] is the sub-list for method output_type
	0, // [0:0] is the sub-list for method input_type
	0, // [0:0] is the sub-list for extension type_name
	0, // [0:0] is the sub-list for extension extendee
	0, // [0:0] is the sub-list for field type_name
}

func init() { file_packfile_proto_init() }
func file_packfile_proto_init() {
	if File_packfile_proto != nil {
		return
	}
	if !protoimpl.UnsafeEnabled {
		file_packfile_proto_msgTypes[0].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*Stats); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: file_packfile_proto_rawDesc,
			NumEnums:      0,
			NumMessages:   1,
			NumExtensions: 0,
			NumServices:   0,
		},
		GoTypes:           file_packfile_proto_goTypes,
		DependencyIndexes: file_packfile_proto_depIdxs,
		MessageInfos:      file_packfile_proto_msgTypes,
	}.Build()
	File_packfile_proto = out.File
	file_packfile_proto_rawDesc = nil
	file_packfile_proto_goTypes = nil
	file_packfile_proto_depIdxs = nil
}
