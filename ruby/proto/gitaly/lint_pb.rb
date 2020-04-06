# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: lint.proto

require 'google/protobuf'

Google::Protobuf::DescriptorPool.generated_pool.build do
  add_file("lint.proto", :syntax => :proto3) do
    add_message "gitaly.OperationMsg" do
      optional :op, :enum, 1, "gitaly.OperationMsg.Operation"
      optional :scope_level, :enum, 2, "gitaly.OperationMsg.Scope"
    end
    add_enum "gitaly.OperationMsg.Operation" do
      value :UNKNOWN, 0
      value :MUTATOR, 1
      value :ACCESSOR, 2
    end
    add_enum "gitaly.OperationMsg.Scope" do
      value :REPOSITORY, 0
      value :SERVER, 1
      value :STORAGE, 2
    end
  end
end

module Gitaly
  OperationMsg = ::Google::Protobuf::DescriptorPool.generated_pool.lookup("gitaly.OperationMsg").msgclass
  OperationMsg::Operation = ::Google::Protobuf::DescriptorPool.generated_pool.lookup("gitaly.OperationMsg.Operation").enummodule
  OperationMsg::Scope = ::Google::Protobuf::DescriptorPool.generated_pool.lookup("gitaly.OperationMsg.Scope").enummodule
end
