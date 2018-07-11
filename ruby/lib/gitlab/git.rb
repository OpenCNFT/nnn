# External dependencies of Gitlab::Git
require 'rugged'
require 'linguist/blob_helper'
require 'securerandom'

# Ruby on Rails mix-ins that GitLab::Git code relies on
require 'active_support/core_ext/object/blank'
require 'active_support/core_ext/numeric/bytes'
require 'active_support/core_ext/numeric/time'
require 'active_support/core_ext/integer/time'
require 'active_support/core_ext/module/delegation'
require 'active_support/core_ext/hash/transform_values'
require 'active_support/core_ext/enumerable'

# We split our mock implementation of Gitlab::GitalyClient into a separate file
require_relative 'gitaly_client.rb'
require_relative 'git_logger.rb'
require_relative 'rails_logger.rb'
require_relative 'gollum.rb'
require_relative 'config.rb'

def require_dependency(_arg)
  # no-op
end

vendor_gitlab_git = '../../vendor/gitlab_git/'

# Some later requires are order-sensitive. Manually require whatever we need.
require_relative File.join(vendor_gitlab_git, 'lib/gitlab/encoding_helper.rb')
require_relative File.join(vendor_gitlab_git, 'lib/gitlab/utils/strong_memoize.rb')
require_relative File.join(vendor_gitlab_git, 'lib/gitlab/version_info.rb')
require_relative File.join(vendor_gitlab_git, 'lib/gitlab/git.rb')
require_relative File.join(vendor_gitlab_git, 'lib/gitlab/git/popen.rb')
require_relative File.join(vendor_gitlab_git, 'lib/gitlab/git/ref.rb')
require_relative File.join(vendor_gitlab_git, 'lib/gitlab/git/repository_mirroring.rb')

# Require all .rb files we can find in the vendored gitlab/git directory
dir = File.expand_path(File.join('..', vendor_gitlab_git, 'lib/gitlab/'), __FILE__)
Dir["#{dir}/git/**/*.rb"].sort.each do |ruby_file|
  next if ruby_file.include?('circuit_breaker')

  require_relative ruby_file.sub(dir, File.join(vendor_gitlab_git, 'lib/gitlab/')).sub(%r{^/*}, '')
end

# Require all .rb files we can find in the local gitlab/git directory
dir = __dir__
Dir["#{dir}/git/**/*.rb"].sort.each do |ruby_file|
  require_relative ruby_file.sub(dir, '').sub(%r{^/*}, '')
end

class String
  # Because we are not rendering HTML, this is a no-op in gitaly-ruby.
  def html_safe
    self
  end
end

class FakeCircuitBreaker
  def self.perform
    yield
  end
end

class RequestStore
  def self.active?
    false
  end
end

module Gitlab
  module Git
    class Env
      NotAvailableInGitalyRuby = Class.new(StandardError)

      def self.all
        raise NotAvailableInGitalyRuby
      end
    end

    module Version
      extend Gitlab::Git::Popen

      def self.git_version
        Gitlab::VersionInfo.parse(popen(%W(#{Gitlab.config.git.bin_path} --version), nil).first)
      end
    end
  end
end

module Gitlab
  module GlId
    def self.gl_id(user)
      user.gl_id
    end

    def self.gl_id_from_id_value(id)
      "user-#{id}"
    end
  end
end
