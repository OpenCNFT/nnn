package gitcmd

import (
	"context"
	"fmt"
	"math"
	"runtime"
	"strings"

	"gitlab.com/gitlab-org/gitaly/v16/internal/git"
	"gitlab.com/gitlab-org/gitaly/v16/internal/structerr"
)

const (
	// scNoRefUpdates denotes a command which will never update refs
	scNoRefUpdates = 1 << iota
	// scNoEndOfOptions denotes a command which doesn't know --end-of-options
	scNoEndOfOptions
)

type commandDescription struct {
	flags                  uint
	opts                   func(context.Context) []GlobalOption
	validatePositionalArgs func([]string) error
}

// commandDescriptions is a curated list of Git command descriptions for special
// git.ExecCommandFactory validation logic
var commandDescriptions = map[string]commandDescription{
	"am": {},
	"apply": {
		flags: scNoRefUpdates,
	},
	"archive": {
		// git-archive(1) does not support disambiguating options from paths from revisions.
		flags: scNoRefUpdates | scNoEndOfOptions,
	},
	"blame": {
		// git-blame(1) does not support disambiguating options from paths from revisions.
		flags: scNoRefUpdates | scNoEndOfOptions,
	},
	"bundle": {
		flags: scNoRefUpdates,
		opts:  packConfiguration,
	},
	"cat-file": {
		flags: scNoRefUpdates,
	},
	"check-attr": {
		flags: scNoRefUpdates | scNoEndOfOptions,
	},
	"check-ref-format": {
		// git-check-ref-format(1) uses a hand-rolled option parser which doesn't support
		// `--end-of-options`.
		flags: scNoRefUpdates | scNoEndOfOptions,
	},
	"checkout": {
		// git-checkout(1) does not support disambiguating options from paths from
		// revisions.
		flags: scNoEndOfOptions,
	},
	"clone": {
		opts: func(ctx context.Context) []GlobalOption {
			return append(append([]GlobalOption{
				// See "init" for why we set the template directory to the empty string.
				ConfigPair{Key: "init.templateDir", Value: ""},
				// See "fetch" for why we disable following redirects.
				ConfigPair{Key: "http.followRedirects", Value: "false"},
				ConfigPair{Key: "transfer.bundleURI", Value: "true"},
			}, packConfiguration(ctx)...), fetchFsckConfiguration(ctx)...)
		},
	},
	"commit": {
		flags: 0,
	},
	"commit-graph": {
		flags: scNoRefUpdates,
	},
	"commit-tree": {
		flags: scNoRefUpdates,
	},
	"config": {
		flags: scNoRefUpdates,
	},
	"count-objects": {
		flags: scNoRefUpdates,
	},
	"diff": {
		flags: scNoRefUpdates,
	},
	"diff-tree": {
		flags: scNoRefUpdates,
	},
	"fast-export": {
		flags: scNoRefUpdates,
	},
	"fetch": {
		flags: 0,

		opts: func(ctx context.Context) []GlobalOption {
			return append(append([]GlobalOption{
				// We've observed performance issues when fetching into big repositories
				// part of an object pool. The root cause of this seems to be the
				// connectivity check, which by default will also include references of any
				// alternates. Given that object pools often have hundreds of thousands of
				// references, this is quite expensive to compute. Below config entry will
				// disable listing of alternate refs: they shouldn't even be included in the
				// negotiation phase, so they aren't going to matter in the connectivity
				// check either.
				ConfigPair{Key: "core.alternateRefsCommand", Value: "exit 0 #"},

				// While git-fetch(1) by default won't write commit graphs, both CNG and
				// Omnibus set this value to true. This has caused performance issues when
				// doing internal fetches, and furthermore it's not encouraged to run such
				// maintenance tasks on "normal" Git operations. Instead, writing commit
				// graphs should be done in our housekeeping RPCs, which already know to do
				// so. So let's disable writing commit graphs on fetches -- if it really is
				// required, we can enable it on a case-by-case basis.
				ConfigPair{Key: "fetch.writeCommitGraph", Value: "false"},

				// By default, Git follows HTTP redirects. Because it's easy for a malicious
				// user to set up a DNS redirect that points to a server that's internal for
				// us and unreachable from the outside, this is dangerous. We thus have to
				// disable redirects in all cases.
				ConfigPair{Key: "http.followRedirects", Value: "false"},

				// By default, Git will try to recurse into submodules on demand: if a fetch
				// retrieves a commit that updates a populated submodule, then it recurses
				// into that submodule and also updates it. Computing this condition takes
				// some resources though given that we need to check all fetched commits to
				// find out if any submodule was in fact updated. This is a complete waste
				// of time though because we never populate submodules at all. We thus
				// disable recursion into submodules.
				ConfigPair{Key: "fetch.recurseSubmodules", Value: "no"},
			}, fetchFsckConfiguration(ctx)...), packConfiguration(ctx)...)
		},
	},
	"filter-repo": {
		flags: scNoEndOfOptions,
	},
	"for-each-ref": {
		flags: scNoRefUpdates,
	},
	"format-patch": {
		flags: scNoRefUpdates,
	},
	"fsck": {
		flags: scNoRefUpdates,
		opts:  fsckConfiguration,
	},
	"gc": {
		flags: scNoRefUpdates,
		opts:  packConfiguration,
	},
	"grep": {
		// git-grep(1) does not support disambiguating options from paths from
		// revisions.
		flags: scNoRefUpdates | scNoEndOfOptions,
		opts: func(context.Context) []GlobalOption {
			return []GlobalOption{
				// This command by default spawns as many threads as there are CPUs. This
				// easily impacts concurrently running commands by exhausting cores and
				// generating excessive I/O load.
				ConfigPair{Key: "grep.threads", Value: threadsConfigValue(runtime.NumCPU())},
			}
		},
	},
	"hash-object": {
		flags: scNoRefUpdates,
	},
	"index-pack": {
		flags: scNoRefUpdates | scNoEndOfOptions,
	},
	"init": {
		flags: scNoRefUpdates,
		opts: func(context.Context) []GlobalOption {
			return []GlobalOption{
				// We're not prepared for a world where the user has configured the default
				// branch to be something different from "master" in Gitaly's git
				// configuration. There explicitly override it on git-init.
				ConfigPair{Key: "init.defaultBranch", Value: git.DefaultBranch},

				// When creating a new repository, then Git will by default copy over all
				// files from the template directory into the repository. These templates
				// are non-mandatory files which help the user to configure parts of Git
				// correctly, like hook templates or an exclude file. Given that repos
				// should not be touched by admins anyway as they are completely owned by
				// Gitaly, those templates don't serve much of a purpose except that they
				// take up disk space. By setting below config entry to the empty value we
				// can thus make sure that we do not use the template directory at all.
				ConfigPair{Key: "init.templateDir", Value: ""},
			}
		},
	},
	"log": {
		flags: scNoRefUpdates,
	},
	"ls-remote": {
		flags: scNoRefUpdates,
		opts: func(context.Context) []GlobalOption {
			return []GlobalOption{
				// See "fetch" for why we disable following redirects.
				ConfigPair{Key: "http.followRedirects", Value: "false"},
			}
		},
	},
	"ls-tree": {
		flags: scNoRefUpdates,
	},
	"merge-base": {
		flags: scNoRefUpdates,
	},
	"merge-file": {
		flags: scNoRefUpdates,
	},
	"merge-tree": {
		flags: scNoRefUpdates,
	},
	"mktag": {
		flags: scNoRefUpdates,
	},
	"mktree": {
		flags: scNoRefUpdates,
	},
	"multi-pack-index": {
		flags: scNoRefUpdates,
	},
	"pack-refs": {
		flags: scNoRefUpdates,
	},
	"pack-objects": {
		flags: scNoRefUpdates,
		opts:  packConfiguration,
	},
	"patch-id": {
		flags: scNoRefUpdates | scNoEndOfOptions,
	},
	"prune": {
		flags: scNoRefUpdates,
	},
	"prune-packed": {
		flags: scNoRefUpdates,
	},
	"push": {
		flags: scNoRefUpdates,
		opts: func(context.Context) []GlobalOption {
			return []GlobalOption{
				// See "fetch" for why we disable following redirects.
				ConfigPair{Key: "http.followRedirects", Value: "false"},
			}
		},
	},
	"range-diff": {
		flags: scNoRefUpdates | scNoEndOfOptions,
	},
	"receive-pack": {
		flags: 0,
		opts: func(ctx context.Context) []GlobalOption {
			return append(append(append([]GlobalOption{
				// In case the repository belongs to an object pool, we want to prevent
				// Git from including the pool's refs in the ref advertisement. We do
				// this by rigging core.alternateRefsCommand to produce no output.
				// Because Git itself will append the pool repository directory, the
				// command ends with a "#". The end result is that Git runs `/bin/sh -c 'exit 0 # /path/to/pool.git`.
				ConfigPair{Key: "core.alternateRefsCommand", Value: "exit 0 #"},

				// Make git-receive-pack(1) advertise the push options
				// capability to clients.
				ConfigPair{Key: "receive.advertisePushOptions", Value: "true"},
				// Disable automatic garbage collection. We want to control the GC scheduling ourselves.
				ConfigPair{Key: "receive.autogc", Value: "false"},
			}, hiddenReceivePackRefPrefixes(ctx)...), receiveFsckConfiguration(ctx)...), packConfiguration(ctx)...)
		},
	},
	"remote": {
		// While git-remote(1)'s `add` subcommand does support `--end-of-options`,
		// `remove` doesn't.
		flags: scNoEndOfOptions,
		opts: func(context.Context) []GlobalOption {
			return []GlobalOption{
				// See "fetch" for why we disable following redirects.
				ConfigPair{Key: "http.followRedirects", Value: "false"},
			}
		},
	},
	"repack": {
		flags: scNoRefUpdates,
		opts: func(ctx context.Context) []GlobalOption {
			return append([]GlobalOption{
				// Write bitmap indices when packing objects, which
				// speeds up packfile creation for fetches.
				ConfigPair{Key: "repack.writeBitmaps", Value: "true"},
				// Do not run git-update-server-info(1), which generates data structures
				// required to server repositories via the dumb HTTP protocol. We don't
				// serve this protocol though, so it's fine to skip it.
				ConfigPair{Key: "repack.updateServerInfo", Value: "false"},
			}, packConfiguration(ctx)...)
		},
	},
	"rev-list": {
		// We cannot use --end-of-options here because pseudo revisions like `--all`
		// and `--not` count as options.
		flags: scNoRefUpdates | scNoEndOfOptions,
		validatePositionalArgs: func(args []string) error {
			for _, arg := range args {
				// git-rev-list(1) supports pseudo-revision arguments which can be
				// intermingled with normal positional arguments. Given that these
				// pseudo-revisions have leading dashes, normal validation would
				// refuse them as positional arguments. We thus override validation
				// for two of these which we are using in our codebase.
				if strings.HasPrefix(arg, "-") {
					if err := git.ValidateRevision([]byte(arg), git.AllowPseudoRevision()); err != nil {
						return structerr.NewInvalidArgument(
							"validating positional argument: %w", err,
						).WithMetadata("argument", arg)
					}

					continue
				}
			}

			return nil
		},
	},
	"rev-parse": {
		// --end-of-options is echoed by git-rev-parse(1) if used without
		// `--verify`.
		flags: scNoRefUpdates | scNoEndOfOptions,
	},
	"show": {
		flags: scNoRefUpdates,
	},
	"show-index": {
		flags: scNoRefUpdates,
	},
	"show-ref": {
		flags: scNoRefUpdates,
	},
	"symbolic-ref": {
		flags: 0,
	},
	"tag": {
		flags: 0,
	},
	"unpack-objects": {
		flags: scNoRefUpdates | scNoEndOfOptions,
	},
	"update-ref": {
		flags: 0,
	},
	"upload-archive": {
		// git-upload-archive(1) has a handrolled parser which always interprets the
		// first argument as directory, so we cannot use `--end-of-options`.
		flags: scNoRefUpdates | scNoEndOfOptions,
	},
	"upload-pack": {
		flags: scNoRefUpdates,
		opts: func(ctx context.Context) []GlobalOption {
			return append(append([]GlobalOption{
				ConfigPair{Key: "uploadpack.allowFilter", Value: "true"},
				// Enables the capability to request individual SHA1's from the
				// remote repo.
				ConfigPair{Key: "uploadpack.allowAnySHA1InWant", Value: "true"},
			}, hiddenUploadPackRefPrefixes(ctx)...), packConfiguration(ctx)...)
		},
	},
	"version": {
		flags: scNoRefUpdates,
	},
	"worktree": {
		flags: 0,
	},
}

// mayUpdateRef indicates if a command is known to update references.
// This is useful to determine if a command requires reference hook
// configuration. A non-exhaustive list of commands is consulted to determine if
// refs are updated. When unknown, true is returned to err on the side of
// caution.
func (c commandDescription) mayUpdateRef() bool {
	return c.flags&scNoRefUpdates == 0
}

// supportsEndOfOptions indicates whether a command can handle the
// `--end-of-options` option.
func (c commandDescription) supportsEndOfOptions() bool {
	return c.flags&scNoEndOfOptions == 0
}

// args validates the given flags and arguments and, if valid, returns the complete command line.
func (c commandDescription) args(flags []Option, args []string, postSepArgs []string) ([]string, error) {
	var commandArgs []string

	for _, o := range flags {
		args, err := o.OptionArgs()
		if err != nil {
			return nil, err
		}
		commandArgs = append(commandArgs, args...)
	}

	if c.supportsEndOfOptions() {
		commandArgs = append(commandArgs, "--end-of-options")
	}

	if c.validatePositionalArgs != nil {
		if err := c.validatePositionalArgs(args); err != nil {
			return nil, err
		}
	} else {
		for _, a := range args {
			if err := validatePositionalArg(a); err != nil {
				return nil, err
			}
		}
	}
	commandArgs = append(commandArgs, args...)

	if len(postSepArgs) > 0 {
		commandArgs = append(commandArgs, "--")
	}

	// post separator args do not need any validation
	commandArgs = append(commandArgs, postSepArgs...)

	return commandArgs, nil
}

func validatePositionalArg(arg string) error {
	if strings.HasPrefix(arg, "-") {
		return fmt.Errorf("positional arg %q cannot start with dash '-': %w", arg, ErrInvalidArg)
	}
	return nil
}

func hiddenReceivePackRefPrefixes(ctx context.Context) []GlobalOption {
	config := make([]GlobalOption, 0, len(git.InternalRefPrefixes))

	for refPrefix, refType := range git.InternalRefPrefixes {
		switch refType {
		case git.InternalReferenceTypeReadonly, git.InternalReferenceTypeHidden:
			// We want to hide both read-only and hidden refs in git-receive-pack(1) so
			// that we make neither of them writeable.
			config = append(config, ConfigPair{Key: "receive.hideRefs", Value: refPrefix})
		default:
			panic(fmt.Sprintf("unhandled internal reference type: %v", refType))
		}
	}

	return config
}

func hiddenUploadPackRefPrefixes(context.Context) []GlobalOption {
	config := make([]GlobalOption, 0, len(git.InternalRefPrefixes))

	for refPrefix, refType := range git.InternalRefPrefixes {
		switch refType {
		case git.InternalReferenceTypeHidden:
			config = append(config, ConfigPair{Key: "uploadpack.hideRefs", Value: refPrefix})
		case git.InternalReferenceTypeReadonly:
			// git-upload-pack(1) doesn't allow writing references, and we do want to
			// announce read-only references that aren't hidden.
		default:
			panic(fmt.Sprintf("unhandled internal reference type: %v", refType))
		}
	}

	return config
}

// fsckConfiguration generates default fsck options used by git-fsck(1).
func fsckConfiguration(ctx context.Context) []GlobalOption {
	return templateFsckConfiguration(ctx, "fsck")
}

// fetchFsckConfiguration generates default fsck options used by git-fetch-pack(1).
func fetchFsckConfiguration(ctx context.Context) []GlobalOption {
	return templateFsckConfiguration(ctx, "fetch.fsck")
}

// receiveFsckConfiguration generates default fsck options used by git-receive-pack(1).
func receiveFsckConfiguration(ctx context.Context) []GlobalOption {
	return templateFsckConfiguration(ctx, "receive.fsck")
}

// templateFsckConfiguration generates our fsck configuration, including ignored checks.
// The prefix must either be "fsck", "receive.fsck" or "fetch.fsck" and indicates whether
// it should apply to git-fsck(1), git-receive-pack(1) or to git-fetch-pack(1).
func templateFsckConfiguration(_ context.Context, prefix string) []GlobalOption {
	configPairs := []GlobalOption{
		// When receiving objects from an untrusted source, we want to always assert that
		// all objects are valid. When fetch.fsckObjects or receive.fsckObjects are not set,
		// the value of transfer.fsckObjects is used instead. Since the fsck configuration
		// of git-fetch-pack(1) and git-receive-pack(1) is coupled, transfer.fsckObjects can
		// be used for both.
		ConfigPair{Key: "transfer.fsckObjects", Value: "true"},
	}

	for _, config := range []struct {
		key   string
		value string
	}{
		// In the past, there was a bug in git that caused users to create commits with
		// invalid timezones. As a result, some histories contain commits that do not match
		// the spec. As we fsck received packfiles by default, any push containing such
		// a commit will be rejected. As this is a mostly harmless issue, we add the
		// following flag to ignore this check.
		{key: "badTimezone", value: "ignore"},

		// git-fsck(1) complains in case a signature does not have a space
		// between mail and date. The most common case where this can be hit
		// is in case the date is missing completely. This error is harmless
		// enough and we cope just fine parsing such signatures, so we can
		// ignore this error.
		{key: "missingSpaceBeforeDate", value: "ignore"},

		// Oldish Git versions used to zero-pad some filemodes, e.g. instead of a
		// file mode of 40000 the tree object would have encoded the filemode as
		// 04000. This doesn't cause any and Git can cope with it alright, so let's
		// ignore it.
		{key: "zeroPaddedFilemode", value: "ignore"},
	} {
		configPairs = append(configPairs, ConfigPair{
			Key:   fmt.Sprintf("%s.%s", prefix, config.key),
			Value: config.value,
		})
	}

	return configPairs
}

func packConfiguration(context.Context) []GlobalOption {
	return []GlobalOption{
		ConfigPair{Key: "pack.windowMemory", Value: "100m"},
		ConfigPair{Key: "pack.writeReverseIndex", Value: "true"},
		ConfigPair{Key: "pack.threads", Value: threadsConfigValue(runtime.NumCPU())},
	}
}

// threadsConfigValue returns the log-2 number of threads based on the number of provided CPUs. This
// prevents us from using excessively many threads and thus avoids exhaustion of all available CPUs.
func threadsConfigValue(numCPUs int) string {
	return fmt.Sprintf("%d", int(math.Max(1, math.Floor(math.Log2(float64(numCPUs))))))
}
