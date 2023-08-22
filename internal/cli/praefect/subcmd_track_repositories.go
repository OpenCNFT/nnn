package praefect

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"

	"github.com/urfave/cli/v2"
	"gitlab.com/gitlab-org/gitaly/v16/internal/log"
	"gitlab.com/gitlab-org/gitaly/v16/internal/praefect/datastore"
	"gitlab.com/gitlab-org/labkit/correlation"
)

const (
	trackRepositoriesCmdName = "track-repositories"
	paramInputPath           = "input-path"
)

func newTrackRepositoriesCommand() *cli.Command {
	return &cli.Command{
		Name:  trackRepositoriesCmdName,
		Usage: "process bulk requests to track repositories",
		Description: "This command allows bulk requests for repositories to be tracked by Praefect.\n" +
			"The -input-path flag must be the path of a file containing the details of the repositories\n" +
			"to track as a list of newline-delimited JSON objects. Each line must contain the details for\n" +
			"one and only one repository. Each item must contain the following keys:\n\n" +
			"  relative_path - The relative path of the repository on-disk.\n" +
			"  virtual_storage - The Praefect virtual storage name.\n" +
			"  authoritative_storage - Which storage to consider as the canonical copy of the repository.\n\n" +
			"If -replicate-immediately is used, the command will attempt to replicate the repositories\n" +
			"to the secondaries. Otherwise, replication jobs will be created and will be executed\n" +
			"eventually by Praefect itself.\n",
		HideHelpCommand: true,
		Action:          trackRepositoriesAction,
		Flags: []cli.Flag{
			&cli.BoolFlag{
				Name:  "replicate-immediately",
				Usage: "kick off replication jobs immediately",
			},
			&cli.StringFlag{
				Name:     paramInputPath,
				Usage:    "path to file with details of repositories to track",
				Required: true,
			},
		},
		Before: func(ctx *cli.Context) error {
			if ctx.Args().Present() {
				_ = cli.ShowSubcommandHelp(ctx)
				return cli.Exit(unexpectedPositionalArgsError{Command: ctx.Command.Name}, 1)
			}
			return nil
		},
	}
}

func trackRepositoriesAction(appCtx *cli.Context) error {
	logger := log.Default()
	conf, err := getConfig(logger, appCtx.String(configFlagName))
	if err != nil {
		return err
	}

	db, clean, err := openDB(conf.DB, appCtx.App.ErrWriter)
	if err != nil {
		return fmt.Errorf("connect to database: %w", err)
	}
	defer clean()

	ctx := correlation.ContextWithCorrelation(context.Background(), correlation.SafeRandomID())
	logger = logger.WithField("correlation_id", correlation.ExtractFromContext(ctx))

	store := datastore.NewPostgresRepositoryStore(db, conf.StorageNames())

	inputPath := appCtx.String(paramInputPath)
	f, err := os.Open(inputPath)
	if err != nil {
		return fmt.Errorf("open input: %w", err)
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)

	fmt.Fprintf(appCtx.App.Writer, "Validating repository information in %q\n", inputPath)

	var requests []trackRepositoryRequest
	var line int
	var repoErrs []invalidRequest
	pathLines := make(map[string][]int)

	// Read in and validate all requests from input file before executing. This prevents us from
	// partially executing a file, which makes it difficult to tell which repos were actually
	// tracked.
	for scanner.Scan() {
		line++

		request := trackRepositoryRequest{}
		badReq := invalidRequest{line: line}

		if err := json.Unmarshal(scanner.Bytes(), &request); err != nil {
			badReq.errs = append(badReq.errs, err)
			repoErrs = append(repoErrs, badReq)

			// Invalid request, nothing to validate.
			continue
		}
		request.ReplicaPath = request.RelativePath

		if request.RelativePath == "" {
			badReq.errs = append(badReq.errs, requiredParameterError(paramRelativePath))
		}
		badReq.path = request.RelativePath

		if request.VirtualStorage == "" {
			badReq.errs = append(badReq.errs, requiredParameterError(paramVirtualStorage))
		}
		if request.AuthoritativeStorage == "" {
			badReq.errs = append(badReq.errs, requiredParameterError(paramAuthoritativeStorage))
		}
		if len(badReq.errs) > 0 {
			repoErrs = append(repoErrs, badReq)

			// Incomplete request, no further validation possible.
			continue
		}

		// Repo paths are globally unique, any attempt to add the same path multiple virtual storages
		// is invalid and must be rejected.
		prevLines, exists := pathLines[request.RelativePath]
		if exists {
			badReq.errs = append(badReq.errs, &dupPathError{path: request.RelativePath})
			repoErrs = append(repoErrs, badReq)

			prevLines = append(prevLines, line)
			pathLines[request.RelativePath] = prevLines

			// We've already checked this path, no need to run further checks.
			continue
		}
		pathLines[request.RelativePath] = []int{line}

		repoInDB, err := store.RepositoryExists(ctx, request.VirtualStorage, request.RelativePath)
		if err != nil {
			// Bail out if we're having trouble contacting the DB, nothing is going to work if this fails.
			return fmt.Errorf("checking database: %w", err)
		}
		if repoInDB {
			badReq.errs = append(badReq.errs, fmt.Errorf("repository is already tracked by Praefect"))
			repoErrs = append(repoErrs, badReq)
			// Repo already in Praefect DB, we can skip it.
			continue
		}

		authoritativeRepoExists, err := request.authoritativeRepositoryExists(ctx, conf, logger, appCtx.App.Writer, request.AuthoritativeStorage)
		if err != nil {
			badReq.errs = append(badReq.errs, fmt.Errorf("checking repository on disk: %w", err))
		} else if !authoritativeRepoExists {
			badReq.errs = append(badReq.errs, fmt.Errorf("not a valid git repository"))
		}

		if len(badReq.errs) > 0 {
			repoErrs = append(repoErrs, badReq)
			continue
		}
		requests = append(requests, request)
	}

	if len(repoErrs) > 0 {
		printInvalidRequests(appCtx.App.Writer, repoErrs, pathLines, inputPath)
		return fmt.Errorf("invalid entries found, aborting")
	}
	if len(requests) == 0 {
		return fmt.Errorf("no repository information found in %q", inputPath)
	}

	fmt.Fprintf(appCtx.App.Writer, "All repository details are correctly formatted\n")
	fmt.Fprintf(appCtx.App.Writer, "Tracking %v repositories in Praefect DB...\n", line)
	replicateImmediately := appCtx.Bool("replicate-immediately")
	for _, request := range requests {
		if err := request.execRequest(ctx, db, conf, appCtx.App.Writer, logger, replicateImmediately); err != nil {
			return fmt.Errorf("tracking repository %q: %w", request.RelativePath, err)
		}
	}

	return nil
}

type invalidRequest struct {
	line int
	path string
	errs []error
}

type dupPathError struct {
	path    string
	reqNums []int
}

func (d *dupPathError) Error() string {
	return fmt.Sprintf("duplicate entries for relative_path, line %v", d.reqNums)
}

func printInvalidRequests(w io.Writer, repoErrs []invalidRequest, pathLines map[string][]int, inputPath string) {
	fmt.Fprintf(w, "Found %v invalid request(s) in %q:\n", len(repoErrs), inputPath)

	for _, l := range repoErrs {
		fmt.Fprintf(w, "  line %v, relative_path: %q\n", l.line, l.path)
		for _, err := range l.errs {
			if dup, ok := err.(*dupPathError); ok {
				// The complete set of duplicate reqNums won't be known until input is
				// fully processed, fetch them now.
				err = &dupPathError{path: dup.path, reqNums: pathLines[dup.path]}
			}
			fmt.Fprintf(w, "    %v\n", err)
		}
	}
}
