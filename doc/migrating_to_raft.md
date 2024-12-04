In order to migrate repositories from a Praefect cluster to a Raft one, we
leverage GitLab's [bulk repository move
API](https://docs.gitlab.com/ee/administration/operations/moving_repositories.html#move-all-projects).
This documents expects that the Raft cluster has been set up and properly
configured _(TODO: Add a link to Raft setup or configuration)_.

Throughout the
rest of this document the Raft cluster will be referred to as `raft` and the
Praefect cluster as `praefect`, change their values to match how your clustered are named.

### Migration steps

1. [Configure repository storage
   weights](https://docs.gitlab.com/ee/administration/repository_storage_paths.html#configure-where-new-repositories-are-stored)
   so that `raft` receives all new projects. This stops new projects from being
   created on `praefect` while the migration is in progress.
1. Make sure that there are no project repositories are in read-only mode. In a
   [Rails
   console](https://docs.gitlab.com/ee/administration/operations/rails_console.html)
   run the following:
    ```ruby
    Project.where(repository_read_only: true).count
    ```
    * If the count is not zero, then we need to update the attribute to be `false`:
        ```ruby
        Project.where(repository_read_only: true).update_all(repository_read_only: false)
        ```
1. From an administrator account, create a new personal token, which would be
   used to issue API requests that schedule the repository moves.
1. For each type of repository GitLab manages (project, group, and snippet), we
   issue an API request to schedule a bulk move from one storage to another:
    1.
    ```bash
      curl --request POST --header "PRIVATE-TOKEN: <your_access_token>" \
     --header "Content-Type: application/json" \
     --data '{"source_storage_name":"praefect","destination_storage_name":"raft"}' \
     "https://gitlab.example.com/api/v4/project_repository_storage_moves"
    ```
    2.
    ```bash
      curl --request POST --header "PRIVATE-TOKEN: <your_access_token>" \
     --header "Content-Type: application/json" \
     --data '{"source_storage_name":"praefect","destination_storage_name":"raft"}' \
     "https://gitlab.example.com/api/v4/group_repository_storage_moves"
    ```
    3.
    ```bash
      curl --request POST --header "PRIVATE-TOKEN: <your_access_token>" \
     --header "Content-Type: application/json" \
     --data '{"source_storage_name":"praefect","destination_storage_name":"raft"}' \
     "https://gitlab.example.com/api/v4/snippet_repository_storage_moves"
    ```
1. In a Rails console,
   periodically, check the number of scheduled moves for each type of
   repository. If the numbers are zero then GitLab has finished processing all
   repositories in `praefect`:
    ```ruby
    Projects::RepositoryStorageMove.where(source_storage_name: 'praefect', destination_storage_name: 'raft', state: 2).count
    Groups::RepositoryStorageMove.where(source_storage_name: 'praefect', destination_storage_name: 'raft', state: 2).count
    Snippets::RepositoryStorageMove.where(source_storage_name: 'praefect', destination_storage_name: 'raft', state: 2).count
    ```
1. Check the number of remaining repositories in a Rails console, if all
   repositories were moved successfully then the numbers zero:
    ```ruby
    ProjectRepository.for_repository_storage('praefect').count
    GroupWikiRepository.for_repository_storage('praefect').count
    SnippetRepository.for_repository_storage('praefect').count
    ```
1. If there are no remaining projects, the `praefect` can be decommissioned
   safely _(TODO: Add a link to decommissioning Praefect if there's one)_. If
   there are remaining projects, then we should repeat steps 3 through 5;
   sometimes there could be transient failures and restarting the move can fix
   them. If there are still remaining projects, then please refer to
   Troubleshooting below.

### Troubleshooting

First we compile a list of each failed project and the error that caused the
failure. Run the following snippet in a Rails console:

```ruby
{'ProjectRepository' => :project, 'GroupWikiRepository' => :group, 'SnippetRepository' => :snippet}.each do |cls, assoc|
  puts "== #{cls}"

  cls.safe_constantize.for_repository_storage('gitaly-02-stor-gstg.c.gitlab-gitaly-gstg-380a.internal').in_batches do |batch|
    batch.each do |resource|
      container = resource.send(assoc)
      puts "#{container.full_path}: #{container.repository_storage_moves.last.error_message}"
    end
  end
end
```

Then, for each project we follow the fixing steps if the error is listed below,
otherwise please get in touch with GitLab support.

#### `Timeout waiting for project repository pushes`

This error means the repository is being actively written to during the time of
its move. Retrying the move should eventually succeed. If not, the reference
counter of the repository could be in an incorrect state, this can be checked
and fixed as follows in a Rails console:

```
project = Project.find_by_full_path('path/to/project')
project.reference_counter(type: project.repository.repo_type).value
# => 6
project.reference_counter(type: project.repository.repo_type).value
# => 6
# If the number is not going down then we can reset the counter
project.reference_counter(type: project.repository.repo_type).reset!
```

#### `replicating repository: synchronizing references: fetch internal remote`

Sometimes this error points to a repository having a reference that points to
non-existent object. The reference(s) in question could be identified by
running a `git rev-list` inside the repository directory. This can be achieved
like this:

```bash
$ cd /path/to/your/repositories/repo/path
$ /opt/gitlab/embedded/bin/gitaly git -c /var/opt/gitlab/gitaly/config.toml -- rev-list --objects --all --quiet
fatal: bad object refs/heads/dev
$ /opt/gitlab/embedded/bin/gitaly git -c /var/opt/gitlab/gitaly/config.toml -- update-ref -d refs/heads/dev
```

`rev-list` may be needed to be run multiple times until it no longer prints a
`fatal` message.

#### `git connectivity error while disconnected: exit status 128.`

This error can be caused by many causes, one of them could be the
`commit-graph` file inside the repository. To verify, we run `git rev-list`
inside the repository directory:

```bash 
$ cd /path/to/your/repositories/repo/path
$ /opt/gitlab/embedded/bin/gitaly git -c /var/opt/gitlab/gitaly/config.toml -- rev-list --objects --all --quiet
fatal: commit-graph requires overflow generation data but has none
$ rm objects/info/commit-graph
```
