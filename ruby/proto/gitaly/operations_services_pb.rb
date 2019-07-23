# Generated by the protocol buffer compiler.  DO NOT EDIT!
# Source: operations.proto for package 'gitaly'

require 'grpc'
require 'operations_pb'

module Gitaly
  module OperationService
    class Service

      include GRPC::GenericService

      self.marshal_class_method = :encode
      self.unmarshal_class_method = :decode
      self.service_name = 'gitaly.OperationService'

      rpc :UserCreateBranch, UserCreateBranchRequest, UserCreateBranchResponse
      rpc :UserUpdateBranch, UserUpdateBranchRequest, UserUpdateBranchResponse
      rpc :UserDeleteBranch, UserDeleteBranchRequest, UserDeleteBranchResponse
      rpc :UserCreateTag, UserCreateTagRequest, UserCreateTagResponse
      rpc :UserDeleteTag, UserDeleteTagRequest, UserDeleteTagResponse
      rpc :UserMergeToRef, UserMergeToRefRequest, UserMergeToRefResponse
      rpc :UserMergeBranch, stream(UserMergeBranchRequest), stream(UserMergeBranchResponse)
      rpc :UserFFBranch, UserFFBranchRequest, UserFFBranchResponse
      rpc :UserCherryPick, UserCherryPickRequest, UserCherryPickResponse
      rpc :UserCommitFiles, stream(UserCommitFilesRequest), UserCommitFilesResponse
      rpc :UserRebase, UserRebaseRequest, UserRebaseResponse
      rpc :UserRebaseConfirmable, stream(UserRebaseConfirmableRequest), stream(UserRebaseConfirmableResponse)
      rpc :UserRevert, UserRevertRequest, UserRevertResponse
      rpc :UserSquash, UserSquashRequest, UserSquashResponse
      rpc :UserApplyPatch, stream(UserApplyPatchRequest), UserApplyPatchResponse
      rpc :UserUpdateSubmodule, UserUpdateSubmoduleRequest, UserUpdateSubmoduleResponse
    end

    Stub = Service.rpc_stub_class
  end
end
