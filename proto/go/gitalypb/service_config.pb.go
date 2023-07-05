// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.31.0
// 	protoc        v4.23.1
// source: service_config.proto

package gitalypb

import (
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	durationpb "google.golang.org/protobuf/types/known/durationpb"
	wrapperspb "google.golang.org/protobuf/types/known/wrapperspb"
	reflect "reflect"
	sync "sync"
)

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

// ServiceConfig defines the configuration that allows service owners to publish parameters to be
// automatically used by all clients of their service. gRPC doesn't export this protobuf. So, we
// built a minimized version to for Gitaly use. The completed version is defined at
// https://github.com/grpc/grpc-proto/blob/master/grpc/service_config/service_config.proto
type ServiceConfig struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	// protolint:disable:next REPEATED_FIELD_NAMES_PLURALIZED
	LoadBalancingConfig []*LoadBalancingConfig `protobuf:"bytes,1,rep,name=load_balancing_config,json=loadBalancingConfig,proto3" json:"load_balancing_config,omitempty"`
	// protolint:disable:next REPEATED_FIELD_NAMES_PLURALIZED
	MethodConfig []*MethodConfig `protobuf:"bytes,2,rep,name=method_config,json=methodConfig,proto3" json:"method_config,omitempty"`
}

func (x *ServiceConfig) Reset() {
	*x = ServiceConfig{}
	if protoimpl.UnsafeEnabled {
		mi := &file_service_config_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *ServiceConfig) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*ServiceConfig) ProtoMessage() {}

func (x *ServiceConfig) ProtoReflect() protoreflect.Message {
	mi := &file_service_config_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use ServiceConfig.ProtoReflect.Descriptor instead.
func (*ServiceConfig) Descriptor() ([]byte, []int) {
	return file_service_config_proto_rawDescGZIP(), []int{0}
}

func (x *ServiceConfig) GetLoadBalancingConfig() []*LoadBalancingConfig {
	if x != nil {
		return x.LoadBalancingConfig
	}
	return nil
}

func (x *ServiceConfig) GetMethodConfig() []*MethodConfig {
	if x != nil {
		return x.MethodConfig
	}
	return nil
}

// LoadBalancingConfig wraps around the round-robin strategies. Only one strategy can be selected.
type LoadBalancingConfig struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	// Types that are assignable to Policy:
	//
	//	*LoadBalancingConfig_PickFirst
	//	*LoadBalancingConfig_RoundRobin
	Policy isLoadBalancingConfig_Policy `protobuf_oneof:"policy"`
}

func (x *LoadBalancingConfig) Reset() {
	*x = LoadBalancingConfig{}
	if protoimpl.UnsafeEnabled {
		mi := &file_service_config_proto_msgTypes[1]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *LoadBalancingConfig) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*LoadBalancingConfig) ProtoMessage() {}

func (x *LoadBalancingConfig) ProtoReflect() protoreflect.Message {
	mi := &file_service_config_proto_msgTypes[1]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use LoadBalancingConfig.ProtoReflect.Descriptor instead.
func (*LoadBalancingConfig) Descriptor() ([]byte, []int) {
	return file_service_config_proto_rawDescGZIP(), []int{1}
}

func (m *LoadBalancingConfig) GetPolicy() isLoadBalancingConfig_Policy {
	if m != nil {
		return m.Policy
	}
	return nil
}

func (x *LoadBalancingConfig) GetPickFirst() *PickFirstConfig {
	if x, ok := x.GetPolicy().(*LoadBalancingConfig_PickFirst); ok {
		return x.PickFirst
	}
	return nil
}

func (x *LoadBalancingConfig) GetRoundRobin() *RoundRobinConfig {
	if x, ok := x.GetPolicy().(*LoadBalancingConfig_RoundRobin); ok {
		return x.RoundRobin
	}
	return nil
}

type isLoadBalancingConfig_Policy interface {
	isLoadBalancingConfig_Policy()
}

type LoadBalancingConfig_PickFirst struct {
	// PickFirst strategy
	PickFirst *PickFirstConfig `protobuf:"bytes,1,opt,name=pick_first,proto3,oneof"`
}

type LoadBalancingConfig_RoundRobin struct {
	// RoundRobin strategy
	RoundRobin *RoundRobinConfig `protobuf:"bytes,2,opt,name=round_robin,proto3,oneof"`
}

func (*LoadBalancingConfig_PickFirst) isLoadBalancingConfig_Policy() {}

func (*LoadBalancingConfig_RoundRobin) isLoadBalancingConfig_Policy() {}

// PickFirstConfig signals the pick_first load-balancing strategy. This strategy is the default
// strategy of grpc client libraries so that the connection has only one subchannel, which is the
// first address after resolution
type PickFirstConfig struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields
}

func (x *PickFirstConfig) Reset() {
	*x = PickFirstConfig{}
	if protoimpl.UnsafeEnabled {
		mi := &file_service_config_proto_msgTypes[2]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *PickFirstConfig) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*PickFirstConfig) ProtoMessage() {}

func (x *PickFirstConfig) ProtoReflect() protoreflect.Message {
	mi := &file_service_config_proto_msgTypes[2]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use PickFirstConfig.ProtoReflect.Descriptor instead.
func (*PickFirstConfig) Descriptor() ([]byte, []int) {
	return file_service_config_proto_rawDescGZIP(), []int{2}
}

// RoundRobinConfig indicates the round_robin strategy. This strategy distributes the incoming
// requests to active subchannels in a round-robin fashion.
type RoundRobinConfig struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields
}

func (x *RoundRobinConfig) Reset() {
	*x = RoundRobinConfig{}
	if protoimpl.UnsafeEnabled {
		mi := &file_service_config_proto_msgTypes[3]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *RoundRobinConfig) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*RoundRobinConfig) ProtoMessage() {}

func (x *RoundRobinConfig) ProtoReflect() protoreflect.Message {
	mi := &file_service_config_proto_msgTypes[3]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use RoundRobinConfig.ProtoReflect.Descriptor instead.
func (*RoundRobinConfig) Descriptor() ([]byte, []int) {
	return file_service_config_proto_rawDescGZIP(), []int{3}
}

// Configuration for a method.
type MethodConfig struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	// name defines the list of affected services/methods
	// The name should be  is the definition of grpc. Nothing we can do about it
	// protolint:disable:next REPEATED_FIELD_NAMES_PLURALIZED
	Name []*MethodConfig_Name `protobuf:"bytes,1,rep,name=name,proto3" json:"name,omitempty"`
	// The following fields are unused by Gitaly at the moment. Please refer to the original
	// documentation for more information
	// protolint:disable FIELDS_HAVE_COMMENT
	WaitForReady            *wrapperspb.BoolValue   `protobuf:"bytes,2,opt,name=wait_for_ready,json=waitForReady,proto3" json:"wait_for_ready,omitempty"`
	Timeout                 *durationpb.Duration    `protobuf:"bytes,3,opt,name=timeout,proto3" json:"timeout,omitempty"`
	MaxRequestMessageBytes  *wrapperspb.UInt32Value `protobuf:"bytes,4,opt,name=max_request_message_bytes,json=maxRequestMessageBytes,proto3" json:"max_request_message_bytes,omitempty"`
	MaxResponseMessageBytes *wrapperspb.UInt32Value `protobuf:"bytes,5,opt,name=max_response_message_bytes,json=maxResponseMessageBytes,proto3" json:"max_response_message_bytes,omitempty"`
	// protolint:enable FIELDS_HAVE_COMMENT
	// retry_policy defines the exponential backoff configuration for the affected services/methods
	RetryPolicy *MethodConfig_RetryPolicy `protobuf:"bytes,6,opt,name=retry_policy,json=retryPolicy,proto3" json:"retry_policy,omitempty"`
}

func (x *MethodConfig) Reset() {
	*x = MethodConfig{}
	if protoimpl.UnsafeEnabled {
		mi := &file_service_config_proto_msgTypes[4]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *MethodConfig) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*MethodConfig) ProtoMessage() {}

func (x *MethodConfig) ProtoReflect() protoreflect.Message {
	mi := &file_service_config_proto_msgTypes[4]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use MethodConfig.ProtoReflect.Descriptor instead.
func (*MethodConfig) Descriptor() ([]byte, []int) {
	return file_service_config_proto_rawDescGZIP(), []int{4}
}

func (x *MethodConfig) GetName() []*MethodConfig_Name {
	if x != nil {
		return x.Name
	}
	return nil
}

func (x *MethodConfig) GetWaitForReady() *wrapperspb.BoolValue {
	if x != nil {
		return x.WaitForReady
	}
	return nil
}

func (x *MethodConfig) GetTimeout() *durationpb.Duration {
	if x != nil {
		return x.Timeout
	}
	return nil
}

func (x *MethodConfig) GetMaxRequestMessageBytes() *wrapperspb.UInt32Value {
	if x != nil {
		return x.MaxRequestMessageBytes
	}
	return nil
}

func (x *MethodConfig) GetMaxResponseMessageBytes() *wrapperspb.UInt32Value {
	if x != nil {
		return x.MaxResponseMessageBytes
	}
	return nil
}

func (x *MethodConfig) GetRetryPolicy() *MethodConfig_RetryPolicy {
	if x != nil {
		return x.RetryPolicy
	}
	return nil
}

// Name is an object indicating which services/methods being affected by the config
type MethodConfig_Name struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	// service is name of the service,including its package. For example: gitaly.SmartHTTPService
	Service string `protobuf:"bytes,1,opt,name=service,proto3" json:"service,omitempty"`
	// method is the name of the method within the above service. Empty method name implies the
	// method policy is effective for all methods of the service
	Method string `protobuf:"bytes,2,opt,name=method,proto3" json:"method,omitempty"`
}

func (x *MethodConfig_Name) Reset() {
	*x = MethodConfig_Name{}
	if protoimpl.UnsafeEnabled {
		mi := &file_service_config_proto_msgTypes[5]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *MethodConfig_Name) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*MethodConfig_Name) ProtoMessage() {}

func (x *MethodConfig_Name) ProtoReflect() protoreflect.Message {
	mi := &file_service_config_proto_msgTypes[5]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use MethodConfig_Name.ProtoReflect.Descriptor instead.
func (*MethodConfig_Name) Descriptor() ([]byte, []int) {
	return file_service_config_proto_rawDescGZIP(), []int{4, 0}
}

func (x *MethodConfig_Name) GetService() string {
	if x != nil {
		return x.Service
	}
	return ""
}

func (x *MethodConfig_Name) GetMethod() string {
	if x != nil {
		return x.Method
	}
	return ""
}

// RetryPolicy defines the configuration for exponential backoff when a request fails
type MethodConfig_RetryPolicy struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	// max_attempts is the total retry attempts client perform before bailing out
	MaxAttempts uint32 `protobuf:"varint,1,opt,name=max_attempts,json=maxAttempts,proto3" json:"max_attempts,omitempty"`
	// initial_backoff is the minimum delay for the first retries
	InitialBackoff *durationpb.Duration `protobuf:"bytes,2,opt,name=initial_backoff,json=initialBackoff,proto3" json:"initial_backoff,omitempty"`
	// max_backoff is the minimum delay
	MaxBackoff *durationpb.Duration `protobuf:"bytes,3,opt,name=max_backoff,json=maxBackoff,proto3" json:"max_backoff,omitempty"`
	// backoff_multiplier is the factor determining "how fast" the delay increases after each retry
	BackoffMultiplier float32 `protobuf:"fixed32,4,opt,name=backoff_multiplier,json=backoffMultiplier,proto3" json:"backoff_multiplier,omitempty"`
	// retryable_status_codes defines the list of eligible status codes. The status must be in
	// capitalized snake_case form. For example, UNAVAILABLE, FAILED_PRECONDITION
	RetryableStatusCodes []string `protobuf:"bytes,5,rep,name=retryable_status_codes,json=retryableStatusCodes,proto3" json:"retryable_status_codes,omitempty"`
}

func (x *MethodConfig_RetryPolicy) Reset() {
	*x = MethodConfig_RetryPolicy{}
	if protoimpl.UnsafeEnabled {
		mi := &file_service_config_proto_msgTypes[6]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *MethodConfig_RetryPolicy) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*MethodConfig_RetryPolicy) ProtoMessage() {}

func (x *MethodConfig_RetryPolicy) ProtoReflect() protoreflect.Message {
	mi := &file_service_config_proto_msgTypes[6]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use MethodConfig_RetryPolicy.ProtoReflect.Descriptor instead.
func (*MethodConfig_RetryPolicy) Descriptor() ([]byte, []int) {
	return file_service_config_proto_rawDescGZIP(), []int{4, 1}
}

func (x *MethodConfig_RetryPolicy) GetMaxAttempts() uint32 {
	if x != nil {
		return x.MaxAttempts
	}
	return 0
}

func (x *MethodConfig_RetryPolicy) GetInitialBackoff() *durationpb.Duration {
	if x != nil {
		return x.InitialBackoff
	}
	return nil
}

func (x *MethodConfig_RetryPolicy) GetMaxBackoff() *durationpb.Duration {
	if x != nil {
		return x.MaxBackoff
	}
	return nil
}

func (x *MethodConfig_RetryPolicy) GetBackoffMultiplier() float32 {
	if x != nil {
		return x.BackoffMultiplier
	}
	return 0
}

func (x *MethodConfig_RetryPolicy) GetRetryableStatusCodes() []string {
	if x != nil {
		return x.RetryableStatusCodes
	}
	return nil
}

var File_service_config_proto protoreflect.FileDescriptor

var file_service_config_proto_rawDesc = []byte{
	0x0a, 0x14, 0x73, 0x65, 0x72, 0x76, 0x69, 0x63, 0x65, 0x5f, 0x63, 0x6f, 0x6e, 0x66, 0x69, 0x67,
	0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x12, 0x06, 0x67, 0x69, 0x74, 0x61, 0x6c, 0x79, 0x1a, 0x1e,
	0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2f, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x62, 0x75, 0x66, 0x2f,
	0x64, 0x75, 0x72, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x1a, 0x1e,
	0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2f, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x62, 0x75, 0x66, 0x2f,
	0x77, 0x72, 0x61, 0x70, 0x70, 0x65, 0x72, 0x73, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x22, 0x9b,
	0x01, 0x0a, 0x0d, 0x53, 0x65, 0x72, 0x76, 0x69, 0x63, 0x65, 0x43, 0x6f, 0x6e, 0x66, 0x69, 0x67,
	0x12, 0x4f, 0x0a, 0x15, 0x6c, 0x6f, 0x61, 0x64, 0x5f, 0x62, 0x61, 0x6c, 0x61, 0x6e, 0x63, 0x69,
	0x6e, 0x67, 0x5f, 0x63, 0x6f, 0x6e, 0x66, 0x69, 0x67, 0x18, 0x01, 0x20, 0x03, 0x28, 0x0b, 0x32,
	0x1b, 0x2e, 0x67, 0x69, 0x74, 0x61, 0x6c, 0x79, 0x2e, 0x4c, 0x6f, 0x61, 0x64, 0x42, 0x61, 0x6c,
	0x61, 0x6e, 0x63, 0x69, 0x6e, 0x67, 0x43, 0x6f, 0x6e, 0x66, 0x69, 0x67, 0x52, 0x13, 0x6c, 0x6f,
	0x61, 0x64, 0x42, 0x61, 0x6c, 0x61, 0x6e, 0x63, 0x69, 0x6e, 0x67, 0x43, 0x6f, 0x6e, 0x66, 0x69,
	0x67, 0x12, 0x39, 0x0a, 0x0d, 0x6d, 0x65, 0x74, 0x68, 0x6f, 0x64, 0x5f, 0x63, 0x6f, 0x6e, 0x66,
	0x69, 0x67, 0x18, 0x02, 0x20, 0x03, 0x28, 0x0b, 0x32, 0x14, 0x2e, 0x67, 0x69, 0x74, 0x61, 0x6c,
	0x79, 0x2e, 0x4d, 0x65, 0x74, 0x68, 0x6f, 0x64, 0x43, 0x6f, 0x6e, 0x66, 0x69, 0x67, 0x52, 0x0c,
	0x6d, 0x65, 0x74, 0x68, 0x6f, 0x64, 0x43, 0x6f, 0x6e, 0x66, 0x69, 0x67, 0x22, 0x98, 0x01, 0x0a,
	0x13, 0x4c, 0x6f, 0x61, 0x64, 0x42, 0x61, 0x6c, 0x61, 0x6e, 0x63, 0x69, 0x6e, 0x67, 0x43, 0x6f,
	0x6e, 0x66, 0x69, 0x67, 0x12, 0x39, 0x0a, 0x0a, 0x70, 0x69, 0x63, 0x6b, 0x5f, 0x66, 0x69, 0x72,
	0x73, 0x74, 0x18, 0x01, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x17, 0x2e, 0x67, 0x69, 0x74, 0x61, 0x6c,
	0x79, 0x2e, 0x50, 0x69, 0x63, 0x6b, 0x46, 0x69, 0x72, 0x73, 0x74, 0x43, 0x6f, 0x6e, 0x66, 0x69,
	0x67, 0x48, 0x00, 0x52, 0x0a, 0x70, 0x69, 0x63, 0x6b, 0x5f, 0x66, 0x69, 0x72, 0x73, 0x74, 0x12,
	0x3c, 0x0a, 0x0b, 0x72, 0x6f, 0x75, 0x6e, 0x64, 0x5f, 0x72, 0x6f, 0x62, 0x69, 0x6e, 0x18, 0x02,
	0x20, 0x01, 0x28, 0x0b, 0x32, 0x18, 0x2e, 0x67, 0x69, 0x74, 0x61, 0x6c, 0x79, 0x2e, 0x52, 0x6f,
	0x75, 0x6e, 0x64, 0x52, 0x6f, 0x62, 0x69, 0x6e, 0x43, 0x6f, 0x6e, 0x66, 0x69, 0x67, 0x48, 0x00,
	0x52, 0x0b, 0x72, 0x6f, 0x75, 0x6e, 0x64, 0x5f, 0x72, 0x6f, 0x62, 0x69, 0x6e, 0x42, 0x08, 0x0a,
	0x06, 0x70, 0x6f, 0x6c, 0x69, 0x63, 0x79, 0x22, 0x11, 0x0a, 0x0f, 0x50, 0x69, 0x63, 0x6b, 0x46,
	0x69, 0x72, 0x73, 0x74, 0x43, 0x6f, 0x6e, 0x66, 0x69, 0x67, 0x22, 0x12, 0x0a, 0x10, 0x52, 0x6f,
	0x75, 0x6e, 0x64, 0x52, 0x6f, 0x62, 0x69, 0x6e, 0x43, 0x6f, 0x6e, 0x66, 0x69, 0x67, 0x22, 0xff,
	0x05, 0x0a, 0x0c, 0x4d, 0x65, 0x74, 0x68, 0x6f, 0x64, 0x43, 0x6f, 0x6e, 0x66, 0x69, 0x67, 0x12,
	0x2d, 0x0a, 0x04, 0x6e, 0x61, 0x6d, 0x65, 0x18, 0x01, 0x20, 0x03, 0x28, 0x0b, 0x32, 0x19, 0x2e,
	0x67, 0x69, 0x74, 0x61, 0x6c, 0x79, 0x2e, 0x4d, 0x65, 0x74, 0x68, 0x6f, 0x64, 0x43, 0x6f, 0x6e,
	0x66, 0x69, 0x67, 0x2e, 0x4e, 0x61, 0x6d, 0x65, 0x52, 0x04, 0x6e, 0x61, 0x6d, 0x65, 0x12, 0x40,
	0x0a, 0x0e, 0x77, 0x61, 0x69, 0x74, 0x5f, 0x66, 0x6f, 0x72, 0x5f, 0x72, 0x65, 0x61, 0x64, 0x79,
	0x18, 0x02, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x1a, 0x2e, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2e,
	0x70, 0x72, 0x6f, 0x74, 0x6f, 0x62, 0x75, 0x66, 0x2e, 0x42, 0x6f, 0x6f, 0x6c, 0x56, 0x61, 0x6c,
	0x75, 0x65, 0x52, 0x0c, 0x77, 0x61, 0x69, 0x74, 0x46, 0x6f, 0x72, 0x52, 0x65, 0x61, 0x64, 0x79,
	0x12, 0x33, 0x0a, 0x07, 0x74, 0x69, 0x6d, 0x65, 0x6f, 0x75, 0x74, 0x18, 0x03, 0x20, 0x01, 0x28,
	0x0b, 0x32, 0x19, 0x2e, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f,
	0x62, 0x75, 0x66, 0x2e, 0x44, 0x75, 0x72, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x52, 0x07, 0x74, 0x69,
	0x6d, 0x65, 0x6f, 0x75, 0x74, 0x12, 0x57, 0x0a, 0x19, 0x6d, 0x61, 0x78, 0x5f, 0x72, 0x65, 0x71,
	0x75, 0x65, 0x73, 0x74, 0x5f, 0x6d, 0x65, 0x73, 0x73, 0x61, 0x67, 0x65, 0x5f, 0x62, 0x79, 0x74,
	0x65, 0x73, 0x18, 0x04, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x1c, 0x2e, 0x67, 0x6f, 0x6f, 0x67, 0x6c,
	0x65, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x62, 0x75, 0x66, 0x2e, 0x55, 0x49, 0x6e, 0x74, 0x33,
	0x32, 0x56, 0x61, 0x6c, 0x75, 0x65, 0x52, 0x16, 0x6d, 0x61, 0x78, 0x52, 0x65, 0x71, 0x75, 0x65,
	0x73, 0x74, 0x4d, 0x65, 0x73, 0x73, 0x61, 0x67, 0x65, 0x42, 0x79, 0x74, 0x65, 0x73, 0x12, 0x59,
	0x0a, 0x1a, 0x6d, 0x61, 0x78, 0x5f, 0x72, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x5f, 0x6d,
	0x65, 0x73, 0x73, 0x61, 0x67, 0x65, 0x5f, 0x62, 0x79, 0x74, 0x65, 0x73, 0x18, 0x05, 0x20, 0x01,
	0x28, 0x0b, 0x32, 0x1c, 0x2e, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2e, 0x70, 0x72, 0x6f, 0x74,
	0x6f, 0x62, 0x75, 0x66, 0x2e, 0x55, 0x49, 0x6e, 0x74, 0x33, 0x32, 0x56, 0x61, 0x6c, 0x75, 0x65,
	0x52, 0x17, 0x6d, 0x61, 0x78, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x4d, 0x65, 0x73,
	0x73, 0x61, 0x67, 0x65, 0x42, 0x79, 0x74, 0x65, 0x73, 0x12, 0x43, 0x0a, 0x0c, 0x72, 0x65, 0x74,
	0x72, 0x79, 0x5f, 0x70, 0x6f, 0x6c, 0x69, 0x63, 0x79, 0x18, 0x06, 0x20, 0x01, 0x28, 0x0b, 0x32,
	0x20, 0x2e, 0x67, 0x69, 0x74, 0x61, 0x6c, 0x79, 0x2e, 0x4d, 0x65, 0x74, 0x68, 0x6f, 0x64, 0x43,
	0x6f, 0x6e, 0x66, 0x69, 0x67, 0x2e, 0x52, 0x65, 0x74, 0x72, 0x79, 0x50, 0x6f, 0x6c, 0x69, 0x63,
	0x79, 0x52, 0x0b, 0x72, 0x65, 0x74, 0x72, 0x79, 0x50, 0x6f, 0x6c, 0x69, 0x63, 0x79, 0x1a, 0x38,
	0x0a, 0x04, 0x4e, 0x61, 0x6d, 0x65, 0x12, 0x18, 0x0a, 0x07, 0x73, 0x65, 0x72, 0x76, 0x69, 0x63,
	0x65, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x07, 0x73, 0x65, 0x72, 0x76, 0x69, 0x63, 0x65,
	0x12, 0x16, 0x0a, 0x06, 0x6d, 0x65, 0x74, 0x68, 0x6f, 0x64, 0x18, 0x02, 0x20, 0x01, 0x28, 0x09,
	0x52, 0x06, 0x6d, 0x65, 0x74, 0x68, 0x6f, 0x64, 0x1a, 0x95, 0x02, 0x0a, 0x0b, 0x52, 0x65, 0x74,
	0x72, 0x79, 0x50, 0x6f, 0x6c, 0x69, 0x63, 0x79, 0x12, 0x21, 0x0a, 0x0c, 0x6d, 0x61, 0x78, 0x5f,
	0x61, 0x74, 0x74, 0x65, 0x6d, 0x70, 0x74, 0x73, 0x18, 0x01, 0x20, 0x01, 0x28, 0x0d, 0x52, 0x0b,
	0x6d, 0x61, 0x78, 0x41, 0x74, 0x74, 0x65, 0x6d, 0x70, 0x74, 0x73, 0x12, 0x42, 0x0a, 0x0f, 0x69,
	0x6e, 0x69, 0x74, 0x69, 0x61, 0x6c, 0x5f, 0x62, 0x61, 0x63, 0x6b, 0x6f, 0x66, 0x66, 0x18, 0x02,
	0x20, 0x01, 0x28, 0x0b, 0x32, 0x19, 0x2e, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2e, 0x70, 0x72,
	0x6f, 0x74, 0x6f, 0x62, 0x75, 0x66, 0x2e, 0x44, 0x75, 0x72, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x52,
	0x0e, 0x69, 0x6e, 0x69, 0x74, 0x69, 0x61, 0x6c, 0x42, 0x61, 0x63, 0x6b, 0x6f, 0x66, 0x66, 0x12,
	0x3a, 0x0a, 0x0b, 0x6d, 0x61, 0x78, 0x5f, 0x62, 0x61, 0x63, 0x6b, 0x6f, 0x66, 0x66, 0x18, 0x03,
	0x20, 0x01, 0x28, 0x0b, 0x32, 0x19, 0x2e, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2e, 0x70, 0x72,
	0x6f, 0x74, 0x6f, 0x62, 0x75, 0x66, 0x2e, 0x44, 0x75, 0x72, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x52,
	0x0a, 0x6d, 0x61, 0x78, 0x42, 0x61, 0x63, 0x6b, 0x6f, 0x66, 0x66, 0x12, 0x2d, 0x0a, 0x12, 0x62,
	0x61, 0x63, 0x6b, 0x6f, 0x66, 0x66, 0x5f, 0x6d, 0x75, 0x6c, 0x74, 0x69, 0x70, 0x6c, 0x69, 0x65,
	0x72, 0x18, 0x04, 0x20, 0x01, 0x28, 0x02, 0x52, 0x11, 0x62, 0x61, 0x63, 0x6b, 0x6f, 0x66, 0x66,
	0x4d, 0x75, 0x6c, 0x74, 0x69, 0x70, 0x6c, 0x69, 0x65, 0x72, 0x12, 0x34, 0x0a, 0x16, 0x72, 0x65,
	0x74, 0x72, 0x79, 0x61, 0x62, 0x6c, 0x65, 0x5f, 0x73, 0x74, 0x61, 0x74, 0x75, 0x73, 0x5f, 0x63,
	0x6f, 0x64, 0x65, 0x73, 0x18, 0x05, 0x20, 0x03, 0x28, 0x09, 0x52, 0x14, 0x72, 0x65, 0x74, 0x72,
	0x79, 0x61, 0x62, 0x6c, 0x65, 0x53, 0x74, 0x61, 0x74, 0x75, 0x73, 0x43, 0x6f, 0x64, 0x65, 0x73,
	0x42, 0x34, 0x5a, 0x32, 0x67, 0x69, 0x74, 0x6c, 0x61, 0x62, 0x2e, 0x63, 0x6f, 0x6d, 0x2f, 0x67,
	0x69, 0x74, 0x6c, 0x61, 0x62, 0x2d, 0x6f, 0x72, 0x67, 0x2f, 0x67, 0x69, 0x74, 0x61, 0x6c, 0x79,
	0x2f, 0x76, 0x31, 0x36, 0x2f, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x2f, 0x67, 0x6f, 0x2f, 0x67, 0x69,
	0x74, 0x61, 0x6c, 0x79, 0x70, 0x62, 0x62, 0x06, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x33,
}

var (
	file_service_config_proto_rawDescOnce sync.Once
	file_service_config_proto_rawDescData = file_service_config_proto_rawDesc
)

func file_service_config_proto_rawDescGZIP() []byte {
	file_service_config_proto_rawDescOnce.Do(func() {
		file_service_config_proto_rawDescData = protoimpl.X.CompressGZIP(file_service_config_proto_rawDescData)
	})
	return file_service_config_proto_rawDescData
}

var file_service_config_proto_msgTypes = make([]protoimpl.MessageInfo, 7)
var file_service_config_proto_goTypes = []interface{}{
	(*ServiceConfig)(nil),            // 0: gitaly.ServiceConfig
	(*LoadBalancingConfig)(nil),      // 1: gitaly.LoadBalancingConfig
	(*PickFirstConfig)(nil),          // 2: gitaly.PickFirstConfig
	(*RoundRobinConfig)(nil),         // 3: gitaly.RoundRobinConfig
	(*MethodConfig)(nil),             // 4: gitaly.MethodConfig
	(*MethodConfig_Name)(nil),        // 5: gitaly.MethodConfig.Name
	(*MethodConfig_RetryPolicy)(nil), // 6: gitaly.MethodConfig.RetryPolicy
	(*wrapperspb.BoolValue)(nil),     // 7: google.protobuf.BoolValue
	(*durationpb.Duration)(nil),      // 8: google.protobuf.Duration
	(*wrapperspb.UInt32Value)(nil),   // 9: google.protobuf.UInt32Value
}
var file_service_config_proto_depIdxs = []int32{
	1,  // 0: gitaly.ServiceConfig.load_balancing_config:type_name -> gitaly.LoadBalancingConfig
	4,  // 1: gitaly.ServiceConfig.method_config:type_name -> gitaly.MethodConfig
	2,  // 2: gitaly.LoadBalancingConfig.pick_first:type_name -> gitaly.PickFirstConfig
	3,  // 3: gitaly.LoadBalancingConfig.round_robin:type_name -> gitaly.RoundRobinConfig
	5,  // 4: gitaly.MethodConfig.name:type_name -> gitaly.MethodConfig.Name
	7,  // 5: gitaly.MethodConfig.wait_for_ready:type_name -> google.protobuf.BoolValue
	8,  // 6: gitaly.MethodConfig.timeout:type_name -> google.protobuf.Duration
	9,  // 7: gitaly.MethodConfig.max_request_message_bytes:type_name -> google.protobuf.UInt32Value
	9,  // 8: gitaly.MethodConfig.max_response_message_bytes:type_name -> google.protobuf.UInt32Value
	6,  // 9: gitaly.MethodConfig.retry_policy:type_name -> gitaly.MethodConfig.RetryPolicy
	8,  // 10: gitaly.MethodConfig.RetryPolicy.initial_backoff:type_name -> google.protobuf.Duration
	8,  // 11: gitaly.MethodConfig.RetryPolicy.max_backoff:type_name -> google.protobuf.Duration
	12, // [12:12] is the sub-list for method output_type
	12, // [12:12] is the sub-list for method input_type
	12, // [12:12] is the sub-list for extension type_name
	12, // [12:12] is the sub-list for extension extendee
	0,  // [0:12] is the sub-list for field type_name
}

func init() { file_service_config_proto_init() }
func file_service_config_proto_init() {
	if File_service_config_proto != nil {
		return
	}
	if !protoimpl.UnsafeEnabled {
		file_service_config_proto_msgTypes[0].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*ServiceConfig); i {
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
		file_service_config_proto_msgTypes[1].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*LoadBalancingConfig); i {
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
		file_service_config_proto_msgTypes[2].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*PickFirstConfig); i {
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
		file_service_config_proto_msgTypes[3].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*RoundRobinConfig); i {
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
		file_service_config_proto_msgTypes[4].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*MethodConfig); i {
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
		file_service_config_proto_msgTypes[5].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*MethodConfig_Name); i {
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
		file_service_config_proto_msgTypes[6].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*MethodConfig_RetryPolicy); i {
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
	file_service_config_proto_msgTypes[1].OneofWrappers = []interface{}{
		(*LoadBalancingConfig_PickFirst)(nil),
		(*LoadBalancingConfig_RoundRobin)(nil),
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: file_service_config_proto_rawDesc,
			NumEnums:      0,
			NumMessages:   7,
			NumExtensions: 0,
			NumServices:   0,
		},
		GoTypes:           file_service_config_proto_goTypes,
		DependencyIndexes: file_service_config_proto_depIdxs,
		MessageInfos:      file_service_config_proto_msgTypes,
	}.Build()
	File_service_config_proto = out.File
	file_service_config_proto_rawDesc = nil
	file_service_config_proto_goTypes = nil
	file_service_config_proto_depIdxs = nil
}
