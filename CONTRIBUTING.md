# Contributing to Gitaly

This document describes requirements for merge requests to Gitaly.

## Style Guide

The Gitaly style guide is [documented in it's own file](STYLE.md)

## Changelog

Any new merge request must contain either a CHANGELOG.md entry or a
justification why no changelog entry is needed. New changelog entries
should be added to the 'UNRELEASED' section of CHANGELOG.md.

If a change is specific to an RPC, start the changelog line with the
RPC name. So for a change to RPC `FooBar` you would get:

> FooBar: Add support for `fluffy_bunnies` parameter

## GitLab CE changelog

We only create GitLab CE changelog entries for two types of merge request:

- adding a feature flag for one or more Gitaly RPC's (meaning the RPC becomes available for trials)
- removing a feature flag for one or more Gitaly RPC's (meaning everybody is using a given RPC from that point on)

## Gitaly Maintainers

| Maintainer         |
|--------------------|
|@jacobvosmaer-gitlab|


## Development Process

We use long-running "~Conversation" issues (aka meta-issues) to track the progress of endpoint migrations and other work.

These issues can be tracked on the **[Migration Board](https://gitlab.com/gitlab-org/gitaly/boards/331341)**.

Conversation issues help us track migrations through the stages of the [migration process](doc/MIGRATION_PROCESS.md):
- ~"Migration:Ready-for-Development"
- ~"Migration:RPC-Design"
- ~"Migration:Server-Impl"
- ~"Migration:Client-Impl"
- ~"Migration:Ready-for-Testing"
- ~"Migration:Acceptance-Testing"
- ~"Migration:Disabled"
- ~"Migration:Opt-In"
- ~"Migration:Opt-Out"
- ~"Migration:Mandatory"

While migration may take several releases from end-to-end, and, so that we can better track progress, each stage of the migration will
spawn it's own issue. These issues should generally not stay open for more than a week or two.

## How to develop a migration

1. **Select a migration endpoint**: select a migration from the [migration board](https://gitlab.com/gitlab-org/gitaly/boards/331341). These migrations are prioritised so choose one near the top.
   - Assign the conversation issue to yourself so that others know that you are working on it.
1. **Build the RPC interface**:
   - Using the RPC design in the issue, create a merge request in the [gitaly-proto](https://gitlab.com/gitlab-org/gitaly-proto) repo
1. **Build the Gitaly Server implementation**
1. **Build the client implementation**: this will be in one or more of GitLab-Workhorse, GitLab-Shell, GitLab-CE and GitLab-EE.
1. **Note**: you may find it more productive to work on the RPC interface, server and client implementations simultaneously
   - Sometimes while building the server and client you may discover problems with the RPC interface
   - For this reason, it can be easier to vendor your branch of `gitaly-proto` into your server and client branches during development
   - In GitLab-CE and GitLab-Shell, custom versions of Gitaly can be specified in the `GITALY_SERVER_VERSION` file by using the syntax
     `=my-branch-name`
   - Remember to release a new version of `gitaly-proto` and `gitaly` and change the `GITALY_SERVER_VERSIONS` before review.
1. **Await a new release of GitLab**:
   - Assign the conversation to the corresponding ["Post GitLab <Version> Deployment"](https://gitlab.com/gitlab-org/gitaly/milestones/)
     milestone.
1. **Perform Acceptance Testing**
   - We use feature toggles to gradually enable migration endpoints while monitoring their performance and status
   - If a critical bug is discovered, it's disable the feature toggle and move the conversation into the ~"Migration:Disabled" columm
     on the [migration board](https://gitlab.com/gitlab-org/gitaly/boards/331341) until a fix has been released.

### Workflow

1. The owner of a migration is responsible for moving conversation issues across the
   [migration board](https://gitlab.com/gitlab-org/gitaly/boards/331341).
1. The Gitaly team uses a weekly Wednesday-Wednesday iteration with retrospectives every second week.
1. Work for the cycle should be assigned to the [appropriate Infrastructure Deliverable](https://gitlab.com/gitlab-org/gitaly/milestones/) milestone and tagged with the ~"Infrastructure Deliverable" label.
   - Please **do not assign ~Conversation issues** to the weekly infrastructure deliverable milestone or the  ~"Infrastructure Deliverable" label.
   - ~"Conversation"s are ongoing work-streams while ~"Infrastructure Deliverable" issues should be closed by the upcoming milestone.
1. To keep track of slipping issues, items which we have been unable to complete by the infrastructure deliverable milestone should be moved over to the next milestone and marked with ~"Moved:x1",  ~"Moved:x2",  ~"Moved:x3" etc

## Reviews and Approvals

Merge requests need to **approval by at least two [Gitaly team members](https://gitlab.com/groups/gl-gitaly/group_members)**.

If a merge request will affect code that cannot be conditionally disabled via [feature flags](https://gitlab.com/gitlab-org/gitlab-ce/merge_requests/11747) then at least one of the approvers should be a **Gitaly maintainer**.

Examples of code that **requires maintainer approval** include:
* Configuration management
* Process startup or shutdown
* Shared utility classes such as stream utilities, etc

Examples of code that **does not require maintainer approval** include:
* Build script changes (does not run in production)
* Testing changes (does not run in production)
* Migration endpoints (can be disabled via a feature flag)

Additionally, if you feel that the change you are making is sufficiently complicated or just for your own confidence,
please feel free to assign a maintainer.

### Review Process

The Gitaly team uses the following process:

- When you merge request is ready for review, select two approvers from the Merge Request edit view.
- Assign the first reviewer
- When the first reviewer is done, they assign the second reviewer
- When the second reviewer is done
  - If there are no discussions, they are free to merge
  - Otherwise assign back to the author for next round of review.

**Note**: the author-reviewer 1-reviewer 2-author cycle works best with small changes. With larger changes feel free to use
the traditional author-reviewer 1-author-reviewer 1-reviewer 2-author-reviewer 2-cycle.

## Gitaly Developer Quick-start Guide

- Check the [README.md](README.md) file to ensure that you have the correct version of Go installed.
- To **lint**, **check formatting**, etc: `make verify`
- To **build**: `make build`
- To run **tests**: `make test`
- To get **coverage**: `make cover`
- **Clean**: `make clean`
- All-in-one: `make verify build test`
