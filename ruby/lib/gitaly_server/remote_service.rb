module GitalyServer
  class RemoteService < Gitaly::RemoteService::Service
    include Utils

    def add_remote(request, call)
      repo = Gitlab::Git::Repository.from_gitaly(request.repository, call)

      mirror_refmap = parse_refmaps(request.mirror_refmaps)

      repo.add_remote(request.name, request.url, mirror_refmap: mirror_refmap)

      Gitaly::AddRemoteResponse.new
    end

    def remove_remote(request, call)
      repo = Gitlab::Git::Repository.from_gitaly(request.repository, call)

      result = repo.remove_remote(request.name)

      Gitaly::RemoveRemoteResponse.new(result: result)
    end

    def fetch_internal_remote(request, call)
      repo = Gitlab::Git::Repository.from_gitaly(request.repository, call)
      remote_repo = Gitlab::Git::GitalyRemoteRepository.new(request.remote_repository, call)

      result = repo.fetch_repository_as_mirror(remote_repo)

      Gitaly::FetchInternalRemoteResponse.new(result: result)
    end

    def update_remote_mirror(call)
      request_enum = call.each_remote_read
      first_request = request_enum.next
      repo = Gitlab::Git::Repository.from_gitaly(first_request.repository, call)
      only_branches_matching = first_request.only_branches_matching.to_a

      only_branches_matching += request_enum.flat_map(&:only_branches_matching)

      remote_mirror = Gitlab::Git::RemoteMirror.new(
        repo,
        first_request.ref_name,
        only_branches_matching: only_branches_matching,
        ssh_auth: Gitlab::Git::SshAuth.from_gitaly(first_request)
      )

      remote_mirror.update

      Gitaly::UpdateRemoteMirrorResponse.new
    end

    private

    def parse_refmaps(refmaps)
      return unless refmaps.present?

      parsed_refmaps = refmaps.map do |refmap|
        next unless refmap.present?

        refmap_spec = refmap.to_sym

        if Gitlab::Git::RepositoryMirroring::REFMAPS.has_key?(refmap_spec)
          refmap_spec
        else
          refmap
        end
      end

      parsed_refmaps.compact.presence
    end
  end
end
