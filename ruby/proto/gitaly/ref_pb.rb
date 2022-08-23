# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: ref.proto

require 'errors_pb'
require 'google/protobuf/timestamp_pb'
require 'lint_pb'
require 'shared_pb'
require 'google/protobuf'

Google::Protobuf::DescriptorPool.generated_pool.build do
  add_file("ref.proto", :syntax => :proto3) do
    add_message "gitaly.FindDefaultBranchNameRequest" do
      optional :repository, :message, 1, "gitaly.Repository"
    end
    add_message "gitaly.FindDefaultBranchNameResponse" do
      optional :name, :bytes, 1
    end
    add_message "gitaly.FindAllBranchNamesRequest" do
      optional :repository, :message, 1, "gitaly.Repository"
    end
    add_message "gitaly.FindAllBranchNamesResponse" do
      repeated :names, :bytes, 1
    end
    add_message "gitaly.FindAllTagNamesRequest" do
      optional :repository, :message, 1, "gitaly.Repository"
    end
    add_message "gitaly.FindAllTagNamesResponse" do
      repeated :names, :bytes, 1
    end
    add_message "gitaly.FindLocalBranchesRequest" do
      optional :repository, :message, 1, "gitaly.Repository"
      optional :sort_by, :enum, 2, "gitaly.FindLocalBranchesRequest.SortBy"
      optional :pagination_params, :message, 3, "gitaly.PaginationParameter"
    end
    add_enum "gitaly.FindLocalBranchesRequest.SortBy" do
      value :NAME, 0
      value :UPDATED_ASC, 1
      value :UPDATED_DESC, 2
    end
    add_message "gitaly.FindLocalBranchesResponse" do
      repeated :branches, :message, 1, "gitaly.FindLocalBranchResponse"
      repeated :local_branches, :message, 2, "gitaly.Branch"
    end
    add_message "gitaly.FindLocalBranchResponse" do
      optional :name, :bytes, 1
      optional :commit_id, :string, 2
      optional :commit_subject, :bytes, 3
      optional :commit_author, :message, 4, "gitaly.FindLocalBranchCommitAuthor"
      optional :commit_committer, :message, 5, "gitaly.FindLocalBranchCommitAuthor"
      optional :commit, :message, 6, "gitaly.GitCommit"
    end
    add_message "gitaly.FindLocalBranchCommitAuthor" do
      optional :name, :bytes, 1
      optional :email, :bytes, 2
      optional :date, :message, 3, "google.protobuf.Timestamp"
      optional :timezone, :bytes, 4
    end
    add_message "gitaly.FindAllBranchesRequest" do
      optional :repository, :message, 1, "gitaly.Repository"
      optional :merged_only, :bool, 2
      repeated :merged_branches, :bytes, 3
    end
    add_message "gitaly.FindAllBranchesResponse" do
      repeated :branches, :message, 1, "gitaly.FindAllBranchesResponse.Branch"
    end
    add_message "gitaly.FindAllBranchesResponse.Branch" do
      optional :name, :bytes, 1
      optional :target, :message, 2, "gitaly.GitCommit"
    end
    add_message "gitaly.FindTagRequest" do
      optional :repository, :message, 1, "gitaly.Repository"
      optional :tag_name, :bytes, 2
    end
    add_message "gitaly.FindTagResponse" do
      optional :tag, :message, 1, "gitaly.Tag"
    end
    add_message "gitaly.FindTagError" do
      oneof :error do
        optional :tag_not_found, :message, 1, "gitaly.ReferenceNotFoundError"
      end
    end
    add_message "gitaly.FindAllTagsRequest" do
      optional :repository, :message, 1, "gitaly.Repository"
      optional :sort_by, :message, 2, "gitaly.FindAllTagsRequest.SortBy"
      optional :pagination_params, :message, 3, "gitaly.PaginationParameter"
    end
    add_message "gitaly.FindAllTagsRequest.SortBy" do
      optional :key, :enum, 1, "gitaly.FindAllTagsRequest.SortBy.Key"
      optional :direction, :enum, 2, "gitaly.SortDirection"
    end
    add_enum "gitaly.FindAllTagsRequest.SortBy.Key" do
      value :REFNAME, 0
      value :CREATORDATE, 1
      value :VERSION_REFNAME, 2
    end
    add_message "gitaly.FindAllTagsResponse" do
      repeated :tags, :message, 1, "gitaly.Tag"
    end
    add_message "gitaly.RefExistsRequest" do
      optional :repository, :message, 1, "gitaly.Repository"
      optional :ref, :bytes, 2
    end
    add_message "gitaly.RefExistsResponse" do
      optional :value, :bool, 1
    end
    add_message "gitaly.CreateBranchRequest" do
      optional :repository, :message, 1, "gitaly.Repository"
      optional :name, :bytes, 2
      optional :start_point, :bytes, 3
    end
    add_message "gitaly.CreateBranchResponse" do
      optional :status, :enum, 1, "gitaly.CreateBranchResponse.Status"
      optional :branch, :message, 2, "gitaly.Branch"
    end
    add_enum "gitaly.CreateBranchResponse.Status" do
      value :OK, 0
      value :ERR_EXISTS, 1
      value :ERR_INVALID, 2
      value :ERR_INVALID_START_POINT, 3
    end
    add_message "gitaly.DeleteBranchRequest" do
      optional :repository, :message, 1, "gitaly.Repository"
      optional :name, :bytes, 2
    end
    add_message "gitaly.DeleteBranchResponse" do
    end
    add_message "gitaly.FindBranchRequest" do
      optional :repository, :message, 1, "gitaly.Repository"
      optional :name, :bytes, 2
    end
    add_message "gitaly.FindBranchResponse" do
      optional :branch, :message, 1, "gitaly.Branch"
    end
    add_message "gitaly.DeleteRefsRequest" do
      optional :repository, :message, 1, "gitaly.Repository"
      repeated :except_with_prefix, :bytes, 2
      repeated :refs, :bytes, 3
    end
    add_message "gitaly.DeleteRefsResponse" do
      optional :git_error, :string, 1
    end
    add_message "gitaly.DeleteRefsError" do
      oneof :error do
        optional :invalid_format, :message, 1, "gitaly.InvalidRefFormatError"
        optional :references_locked, :message, 2, "gitaly.ReferencesLockedError"
      end
    end
    add_message "gitaly.ListBranchNamesContainingCommitRequest" do
      optional :repository, :message, 1, "gitaly.Repository"
      optional :commit_id, :string, 2
      optional :limit, :uint32, 3
    end
    add_message "gitaly.ListBranchNamesContainingCommitResponse" do
      repeated :branch_names, :bytes, 2
    end
    add_message "gitaly.ListTagNamesContainingCommitRequest" do
      optional :repository, :message, 1, "gitaly.Repository"
      optional :commit_id, :string, 2
      optional :limit, :uint32, 3
    end
    add_message "gitaly.ListTagNamesContainingCommitResponse" do
      repeated :tag_names, :bytes, 2
    end
    add_message "gitaly.GetTagSignaturesRequest" do
      optional :repository, :message, 1, "gitaly.Repository"
      repeated :tag_revisions, :string, 2
    end
    add_message "gitaly.GetTagSignaturesResponse" do
      repeated :signatures, :message, 1, "gitaly.GetTagSignaturesResponse.TagSignature"
    end
    add_message "gitaly.GetTagSignaturesResponse.TagSignature" do
      optional :tag_id, :string, 1
      optional :signature, :bytes, 2
      optional :content, :bytes, 3
    end
    add_message "gitaly.GetTagMessagesRequest" do
      optional :repository, :message, 1, "gitaly.Repository"
      repeated :tag_ids, :string, 3
    end
    add_message "gitaly.GetTagMessagesResponse" do
      optional :message, :bytes, 2
      optional :tag_id, :string, 3
    end
    add_message "gitaly.FindAllRemoteBranchesRequest" do
      optional :repository, :message, 1, "gitaly.Repository"
      optional :remote_name, :string, 2
    end
    add_message "gitaly.FindAllRemoteBranchesResponse" do
      repeated :branches, :message, 1, "gitaly.Branch"
    end
    add_message "gitaly.PackRefsRequest" do
      optional :repository, :message, 1, "gitaly.Repository"
    end
    add_message "gitaly.PackRefsResponse" do
    end
    add_message "gitaly.ListRefsRequest" do
      optional :repository, :message, 1, "gitaly.Repository"
      repeated :patterns, :bytes, 2
      optional :head, :bool, 3
      optional :sort_by, :message, 4, "gitaly.ListRefsRequest.SortBy"
    end
    add_message "gitaly.ListRefsRequest.SortBy" do
      optional :key, :enum, 1, "gitaly.ListRefsRequest.SortBy.Key"
      optional :direction, :enum, 2, "gitaly.SortDirection"
    end
    add_enum "gitaly.ListRefsRequest.SortBy.Key" do
      value :REFNAME, 0
      value :CREATORDATE, 1
      value :AUTHORDATE, 2
      value :COMMITTERDATE, 3
    end
    add_message "gitaly.ListRefsResponse" do
      repeated :references, :message, 1, "gitaly.ListRefsResponse.Reference"
    end
    add_message "gitaly.ListRefsResponse.Reference" do
      optional :name, :bytes, 1
      optional :target, :string, 2
    end
    add_message "gitaly.FindRefsByOIDRequest" do
      optional :repository, :message, 1, "gitaly.Repository"
      optional :oid, :string, 2
      repeated :ref_patterns, :string, 3
      optional :sort_field, :string, 4
      optional :limit, :uint32, 5
    end
    add_message "gitaly.FindRefsByOIDResponse" do
      repeated :refs, :string, 1
    end
  end
end

module Gitaly
  FindDefaultBranchNameRequest = ::Google::Protobuf::DescriptorPool.generated_pool.lookup("gitaly.FindDefaultBranchNameRequest").msgclass
  FindDefaultBranchNameResponse = ::Google::Protobuf::DescriptorPool.generated_pool.lookup("gitaly.FindDefaultBranchNameResponse").msgclass
  FindAllBranchNamesRequest = ::Google::Protobuf::DescriptorPool.generated_pool.lookup("gitaly.FindAllBranchNamesRequest").msgclass
  FindAllBranchNamesResponse = ::Google::Protobuf::DescriptorPool.generated_pool.lookup("gitaly.FindAllBranchNamesResponse").msgclass
  FindAllTagNamesRequest = ::Google::Protobuf::DescriptorPool.generated_pool.lookup("gitaly.FindAllTagNamesRequest").msgclass
  FindAllTagNamesResponse = ::Google::Protobuf::DescriptorPool.generated_pool.lookup("gitaly.FindAllTagNamesResponse").msgclass
  FindLocalBranchesRequest = ::Google::Protobuf::DescriptorPool.generated_pool.lookup("gitaly.FindLocalBranchesRequest").msgclass
  FindLocalBranchesRequest::SortBy = ::Google::Protobuf::DescriptorPool.generated_pool.lookup("gitaly.FindLocalBranchesRequest.SortBy").enummodule
  FindLocalBranchesResponse = ::Google::Protobuf::DescriptorPool.generated_pool.lookup("gitaly.FindLocalBranchesResponse").msgclass
  FindLocalBranchResponse = ::Google::Protobuf::DescriptorPool.generated_pool.lookup("gitaly.FindLocalBranchResponse").msgclass
  FindLocalBranchCommitAuthor = ::Google::Protobuf::DescriptorPool.generated_pool.lookup("gitaly.FindLocalBranchCommitAuthor").msgclass
  FindAllBranchesRequest = ::Google::Protobuf::DescriptorPool.generated_pool.lookup("gitaly.FindAllBranchesRequest").msgclass
  FindAllBranchesResponse = ::Google::Protobuf::DescriptorPool.generated_pool.lookup("gitaly.FindAllBranchesResponse").msgclass
  FindAllBranchesResponse::Branch = ::Google::Protobuf::DescriptorPool.generated_pool.lookup("gitaly.FindAllBranchesResponse.Branch").msgclass
  FindTagRequest = ::Google::Protobuf::DescriptorPool.generated_pool.lookup("gitaly.FindTagRequest").msgclass
  FindTagResponse = ::Google::Protobuf::DescriptorPool.generated_pool.lookup("gitaly.FindTagResponse").msgclass
  FindTagError = ::Google::Protobuf::DescriptorPool.generated_pool.lookup("gitaly.FindTagError").msgclass
  FindAllTagsRequest = ::Google::Protobuf::DescriptorPool.generated_pool.lookup("gitaly.FindAllTagsRequest").msgclass
  FindAllTagsRequest::SortBy = ::Google::Protobuf::DescriptorPool.generated_pool.lookup("gitaly.FindAllTagsRequest.SortBy").msgclass
  FindAllTagsRequest::SortBy::Key = ::Google::Protobuf::DescriptorPool.generated_pool.lookup("gitaly.FindAllTagsRequest.SortBy.Key").enummodule
  FindAllTagsResponse = ::Google::Protobuf::DescriptorPool.generated_pool.lookup("gitaly.FindAllTagsResponse").msgclass
  RefExistsRequest = ::Google::Protobuf::DescriptorPool.generated_pool.lookup("gitaly.RefExistsRequest").msgclass
  RefExistsResponse = ::Google::Protobuf::DescriptorPool.generated_pool.lookup("gitaly.RefExistsResponse").msgclass
  CreateBranchRequest = ::Google::Protobuf::DescriptorPool.generated_pool.lookup("gitaly.CreateBranchRequest").msgclass
  CreateBranchResponse = ::Google::Protobuf::DescriptorPool.generated_pool.lookup("gitaly.CreateBranchResponse").msgclass
  CreateBranchResponse::Status = ::Google::Protobuf::DescriptorPool.generated_pool.lookup("gitaly.CreateBranchResponse.Status").enummodule
  DeleteBranchRequest = ::Google::Protobuf::DescriptorPool.generated_pool.lookup("gitaly.DeleteBranchRequest").msgclass
  DeleteBranchResponse = ::Google::Protobuf::DescriptorPool.generated_pool.lookup("gitaly.DeleteBranchResponse").msgclass
  FindBranchRequest = ::Google::Protobuf::DescriptorPool.generated_pool.lookup("gitaly.FindBranchRequest").msgclass
  FindBranchResponse = ::Google::Protobuf::DescriptorPool.generated_pool.lookup("gitaly.FindBranchResponse").msgclass
  DeleteRefsRequest = ::Google::Protobuf::DescriptorPool.generated_pool.lookup("gitaly.DeleteRefsRequest").msgclass
  DeleteRefsResponse = ::Google::Protobuf::DescriptorPool.generated_pool.lookup("gitaly.DeleteRefsResponse").msgclass
  DeleteRefsError = ::Google::Protobuf::DescriptorPool.generated_pool.lookup("gitaly.DeleteRefsError").msgclass
  ListBranchNamesContainingCommitRequest = ::Google::Protobuf::DescriptorPool.generated_pool.lookup("gitaly.ListBranchNamesContainingCommitRequest").msgclass
  ListBranchNamesContainingCommitResponse = ::Google::Protobuf::DescriptorPool.generated_pool.lookup("gitaly.ListBranchNamesContainingCommitResponse").msgclass
  ListTagNamesContainingCommitRequest = ::Google::Protobuf::DescriptorPool.generated_pool.lookup("gitaly.ListTagNamesContainingCommitRequest").msgclass
  ListTagNamesContainingCommitResponse = ::Google::Protobuf::DescriptorPool.generated_pool.lookup("gitaly.ListTagNamesContainingCommitResponse").msgclass
  GetTagSignaturesRequest = ::Google::Protobuf::DescriptorPool.generated_pool.lookup("gitaly.GetTagSignaturesRequest").msgclass
  GetTagSignaturesResponse = ::Google::Protobuf::DescriptorPool.generated_pool.lookup("gitaly.GetTagSignaturesResponse").msgclass
  GetTagSignaturesResponse::TagSignature = ::Google::Protobuf::DescriptorPool.generated_pool.lookup("gitaly.GetTagSignaturesResponse.TagSignature").msgclass
  GetTagMessagesRequest = ::Google::Protobuf::DescriptorPool.generated_pool.lookup("gitaly.GetTagMessagesRequest").msgclass
  GetTagMessagesResponse = ::Google::Protobuf::DescriptorPool.generated_pool.lookup("gitaly.GetTagMessagesResponse").msgclass
  FindAllRemoteBranchesRequest = ::Google::Protobuf::DescriptorPool.generated_pool.lookup("gitaly.FindAllRemoteBranchesRequest").msgclass
  FindAllRemoteBranchesResponse = ::Google::Protobuf::DescriptorPool.generated_pool.lookup("gitaly.FindAllRemoteBranchesResponse").msgclass
  PackRefsRequest = ::Google::Protobuf::DescriptorPool.generated_pool.lookup("gitaly.PackRefsRequest").msgclass
  PackRefsResponse = ::Google::Protobuf::DescriptorPool.generated_pool.lookup("gitaly.PackRefsResponse").msgclass
  ListRefsRequest = ::Google::Protobuf::DescriptorPool.generated_pool.lookup("gitaly.ListRefsRequest").msgclass
  ListRefsRequest::SortBy = ::Google::Protobuf::DescriptorPool.generated_pool.lookup("gitaly.ListRefsRequest.SortBy").msgclass
  ListRefsRequest::SortBy::Key = ::Google::Protobuf::DescriptorPool.generated_pool.lookup("gitaly.ListRefsRequest.SortBy.Key").enummodule
  ListRefsResponse = ::Google::Protobuf::DescriptorPool.generated_pool.lookup("gitaly.ListRefsResponse").msgclass
  ListRefsResponse::Reference = ::Google::Protobuf::DescriptorPool.generated_pool.lookup("gitaly.ListRefsResponse.Reference").msgclass
  FindRefsByOIDRequest = ::Google::Protobuf::DescriptorPool.generated_pool.lookup("gitaly.FindRefsByOIDRequest").msgclass
  FindRefsByOIDResponse = ::Google::Protobuf::DescriptorPool.generated_pool.lookup("gitaly.FindRefsByOIDResponse").msgclass
end
