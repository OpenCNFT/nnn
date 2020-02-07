$:.unshift(File.expand_path('../proto', __dir__))
require 'gitaly'

require_relative 'gitlab/git.rb'

require_relative 'gitaly_server/client.rb'
require_relative 'gitaly_server/utils.rb'
require_relative 'gitaly_server/blob_service.rb'
require_relative 'gitaly_server/ref_service.rb'
require_relative 'gitaly_server/operations_service.rb'
require_relative 'gitaly_server/repository_service.rb'
require_relative 'gitaly_server/wiki_service.rb'
require_relative 'gitaly_server/conflicts_service.rb'
require_relative 'gitaly_server/remote_service.rb'
require_relative 'gitaly_server/health_service.rb'

module GitalyServer
  STORAGE_PATH_HEADER = 'gitaly-storage-path'.freeze
  REPO_PATH_HEADER = 'gitaly-repo-path'.freeze
  GL_REPOSITORY_HEADER = 'gitaly-gl-repository'.freeze
  REPO_ALT_DIRS_HEADER = 'gitaly-repo-alt-dirs'.freeze
  GITALY_SERVERS_HEADER = 'gitaly-servers'.freeze
  RUBY_FEATURE_HEADER = 'gitaly-feature-ruby-'.freeze

  def self.storage_path(call)
    call.metadata.fetch(STORAGE_PATH_HEADER)
  end

  def self.repo_path(call)
    call.metadata.fetch(REPO_PATH_HEADER)
  end

  def self.gl_repository(call)
    call.metadata.fetch(GL_REPOSITORY_HEADER)
  end

  def self.repo_alt_dirs(call)
    call.metadata.fetch(REPO_ALT_DIRS_HEADER)
  end

  def self.feature_flags(call)
    call.metadata.select do |key, _|
      key.start_with?(RUBY_FEATURE_HEADER)
    end
  end

  def self.client(call)
    Client.new(call.metadata[GITALY_SERVERS_HEADER])
  end

  def self.register_handlers(server)
    server.handle(RefService.new)
    server.handle(OperationsService.new)
    server.handle(RepositoryService.new)
    server.handle(WikiService.new)
    server.handle(ConflictsService.new)
    server.handle(RemoteService.new)
    server.handle(BlobService.new)
    server.handle(HealthService.new)
  end
end
