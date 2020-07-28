# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: praefect.proto

require 'google/protobuf'

require 'lint_pb'
require 'shared_pb'
Google::Protobuf::DescriptorPool.generated_pool.build do
  add_message "gitaly.SetAuthoritativeStorageRequest" do
    optional :virtual_storage, :string, 1
    optional :relative_path, :string, 2
    optional :authoritative_storage, :string, 3
  end
  add_message "gitaly.SetAuthoritativeStorageResponse" do
  end
  add_message "gitaly.DatalossCheckRequest" do
    optional :virtual_storage, :string, 1
  end
  add_message "gitaly.DatalossCheckResponse" do
    optional :primary, :string, 1
    repeated :repositories, :message, 2, "gitaly.DatalossCheckResponse.Repository"
  end
  add_message "gitaly.DatalossCheckResponse.Repository" do
    optional :relative_path, :string, 1
    repeated :storages, :message, 2, "gitaly.DatalossCheckResponse.Repository.Storage"
  end
  add_message "gitaly.DatalossCheckResponse.Repository.Storage" do
    optional :name, :string, 1
    optional :behind_by, :int64, 2
  end
  add_message "gitaly.RepositoryReplicasRequest" do
    optional :repository, :message, 1, "gitaly.Repository"
  end
  add_message "gitaly.RepositoryReplicasResponse" do
    optional :primary, :message, 1, "gitaly.RepositoryReplicasResponse.RepositoryDetails"
    repeated :replicas, :message, 2, "gitaly.RepositoryReplicasResponse.RepositoryDetails"
  end
  add_message "gitaly.RepositoryReplicasResponse.RepositoryDetails" do
    optional :repository, :message, 1, "gitaly.Repository"
    optional :checksum, :string, 2
  end
  add_message "gitaly.ConsistencyCheckRequest" do
    optional :virtual_storage, :string, 1
    optional :target_storage, :string, 2
    optional :reference_storage, :string, 3
    optional :disable_reconcilliation, :bool, 4
  end
  add_message "gitaly.ConsistencyCheckResponse" do
    optional :repo_relative_path, :string, 1
    optional :target_checksum, :string, 2
    optional :reference_checksum, :string, 3
    optional :repl_job_id, :uint64, 4
    optional :reference_storage, :string, 5
  end
end

module Gitaly
  SetAuthoritativeStorageRequest = Google::Protobuf::DescriptorPool.generated_pool.lookup("gitaly.SetAuthoritativeStorageRequest").msgclass
  SetAuthoritativeStorageResponse = Google::Protobuf::DescriptorPool.generated_pool.lookup("gitaly.SetAuthoritativeStorageResponse").msgclass
  DatalossCheckRequest = Google::Protobuf::DescriptorPool.generated_pool.lookup("gitaly.DatalossCheckRequest").msgclass
  DatalossCheckResponse = Google::Protobuf::DescriptorPool.generated_pool.lookup("gitaly.DatalossCheckResponse").msgclass
  DatalossCheckResponse::Repository = Google::Protobuf::DescriptorPool.generated_pool.lookup("gitaly.DatalossCheckResponse.Repository").msgclass
  DatalossCheckResponse::Repository::Storage = Google::Protobuf::DescriptorPool.generated_pool.lookup("gitaly.DatalossCheckResponse.Repository.Storage").msgclass
  RepositoryReplicasRequest = Google::Protobuf::DescriptorPool.generated_pool.lookup("gitaly.RepositoryReplicasRequest").msgclass
  RepositoryReplicasResponse = Google::Protobuf::DescriptorPool.generated_pool.lookup("gitaly.RepositoryReplicasResponse").msgclass
  RepositoryReplicasResponse::RepositoryDetails = Google::Protobuf::DescriptorPool.generated_pool.lookup("gitaly.RepositoryReplicasResponse.RepositoryDetails").msgclass
  ConsistencyCheckRequest = Google::Protobuf::DescriptorPool.generated_pool.lookup("gitaly.ConsistencyCheckRequest").msgclass
  ConsistencyCheckResponse = Google::Protobuf::DescriptorPool.generated_pool.lookup("gitaly.ConsistencyCheckResponse").msgclass
end
