package protoutil

import (
	"errors"
	"fmt"

	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/runtime/protoimpl"
	"google.golang.org/protobuf/types/descriptorpb"
)

// GetOpExtension gets the OperationMsg from a method descriptor
func GetOpExtension(m *descriptorpb.MethodDescriptorProto) (*gitalypb.OperationMsg, error) {
	ext, err := getExtension(m.GetOptions(), gitalypb.E_OpType)
	if err != nil {
		return nil, err
	}

	return ext.(*gitalypb.OperationMsg), nil
}

// IsInterceptedMethod returns whether the RPC method is intercepted by Praefect.
func IsInterceptedMethod(s *descriptorpb.ServiceDescriptorProto, m *descriptorpb.MethodDescriptorProto) (bool, error) {
	isServiceIntercepted, err := getBoolExtension(s.GetOptions(), gitalypb.E_Intercepted)
	if err != nil {
		return false, fmt.Errorf("is service intercepted: %w", err)
	}

	return isServiceIntercepted, nil
}

// GetRepositoryExtension gets the repository extension from a field descriptor
func GetRepositoryExtension(m *descriptorpb.FieldDescriptorProto) (bool, error) {
	return getBoolExtension(m.GetOptions(), gitalypb.E_Repository)
}

// GetStorageExtension gets the storage extension from a field descriptor
func GetStorageExtension(m *descriptorpb.FieldDescriptorProto) (bool, error) {
	return getBoolExtension(m.GetOptions(), gitalypb.E_Storage)
}

// GetPartitionIDExtension gets the partition id extension from a field descriptor
func GetPartitionIDExtension(m *descriptorpb.FieldDescriptorProto) (bool, error) {
	return getBoolExtension(m.GetOptions(), gitalypb.E_PartitionId)
}

// GetTargetRepositoryExtension gets the target_repository extension from a field descriptor
func GetTargetRepositoryExtension(m *descriptorpb.FieldDescriptorProto) (bool, error) {
	return getBoolExtension(m.GetOptions(), gitalypb.E_TargetRepository)
}

// GetAdditionalRepositoryExtension gets the target_repository extension from a field descriptor
func GetAdditionalRepositoryExtension(m *descriptorpb.FieldDescriptorProto) (bool, error) {
	return getBoolExtension(m.GetOptions(), gitalypb.E_AdditionalRepository)
}

func getBoolExtension(options proto.Message, extension *protoimpl.ExtensionInfo) (bool, error) {
	val, err := getExtension(options, extension)
	if err != nil {
		if errors.Is(err, protoregistry.NotFound) {
			return false, nil
		}

		return false, err
	}

	return val.(bool), nil
}

func getExtension(options proto.Message, extension *protoimpl.ExtensionInfo) (interface{}, error) {
	if !proto.HasExtension(options, extension) {
		return nil, fmt.Errorf("protoutil.getExtension %q: %w", extension.TypeDescriptor().FullName(), protoregistry.NotFound)
	}

	return proto.GetExtension(options, extension), nil
}
