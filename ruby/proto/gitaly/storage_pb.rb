# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: storage.proto

require 'google/protobuf'

require 'shared_pb'
Google::Protobuf::DescriptorPool.generated_pool.build do
  add_message "gitaly.ListDirectoriesRequest" do
    optional :storage_name, :string, 1
    optional :depth, :uint32, 2
  end
  add_message "gitaly.ListDirectoriesResponse" do
    repeated :paths, :string, 1
  end
  add_message "gitaly.DeleteAllRepositoriesRequest" do
    optional :storage_name, :string, 1
  end
  add_message "gitaly.DeleteAllRepositoriesResponse" do
  end
end

module Gitaly
  ListDirectoriesRequest = Google::Protobuf::DescriptorPool.generated_pool.lookup("gitaly.ListDirectoriesRequest").msgclass
  ListDirectoriesResponse = Google::Protobuf::DescriptorPool.generated_pool.lookup("gitaly.ListDirectoriesResponse").msgclass
  DeleteAllRepositoriesRequest = Google::Protobuf::DescriptorPool.generated_pool.lookup("gitaly.DeleteAllRepositoriesRequest").msgclass
  DeleteAllRepositoriesResponse = Google::Protobuf::DescriptorPool.generated_pool.lookup("gitaly.DeleteAllRepositoriesResponse").msgclass
end
