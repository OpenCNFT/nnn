package praefect

import (
	"errors"
	"fmt"

	"github.com/urfave/cli/v2"
	"gitlab.com/gitlab-org/gitaly/v16/internal/log"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
)

func newMetadataCommand() *cli.Command {
	return &cli.Command{
		Name:  "metadata",
		Usage: "show metadata about a repository",
		Description: `Show detailed information about a repository. To specify a repository, provide either:

- A repository ID.
- A virtual storage and the repository's relative path on the virtual storage.

The output includes the following information about the specified repository:

- Repository ID.
- Virtual storage.
- Relative path on the virtual storage.
- Physical path on the physical storage.
- Which physical storage holds the primary replica of the repository.
- Detailed information about replicas of the repository.

Examples:

- praefect --config praefect.config.toml metadata --repository-id 1
- praefect --config praefect.config.toml metadata --virtual-storage default --relative-path <relative_path_on_the_physical_storage>`,
		Action: metadataAction,
		Flags: []cli.Flag{
			&cli.Int64Flag{
				Name:  "repository-id",
				Usage: "the repository's ID",
			},
			&cli.StringFlag{
				Name:  paramVirtualStorage,
				Usage: "the repository's virtual storage",
			},
			&cli.StringFlag{
				Name:  "relative-path",
				Usage: "the repository's relative path in the virtual storage",
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

func metadataAction(appCtx *cli.Context) error {
	log.ConfigureCommand()

	conf, err := readConfig(appCtx.String(configFlagName))
	if err != nil {
		return err
	}

	repositoryID := appCtx.Int64("repository-id")
	virtualStorage := appCtx.String(paramVirtualStorage)
	relativePath := appCtx.String("relative-path")

	var request gitalypb.GetRepositoryMetadataRequest
	switch {
	case repositoryID != 0:
		if virtualStorage != "" || relativePath != "" {
			return errors.New("virtual storage and relative path can't be provided with a repository ID")
		}
		request.Query = &gitalypb.GetRepositoryMetadataRequest_RepositoryId{RepositoryId: repositoryID}
	case virtualStorage != "" || relativePath != "":
		if virtualStorage == "" {
			return errors.New("virtual storage is required with relative path")
		} else if relativePath == "" {
			return errors.New("relative path is required with virtual storage")
		}
		request.Query = &gitalypb.GetRepositoryMetadataRequest_Path_{
			Path: &gitalypb.GetRepositoryMetadataRequest_Path{
				VirtualStorage: virtualStorage,
				RelativePath:   relativePath,
			},
		}
	default:
		return errors.New("repository ID or virtual storage and relative path required")
	}

	nodeAddr, err := getNodeAddress(conf)
	if err != nil {
		return fmt.Errorf("get node address: %w", err)
	}

	conn, err := subCmdDial(appCtx.Context, nodeAddr, conf.Auth.Token, defaultDialTimeout)
	if err != nil {
		return fmt.Errorf("dial: %w", err)
	}
	defer conn.Close()

	metadata, err := gitalypb.NewPraefectInfoServiceClient(conn).GetRepositoryMetadata(appCtx.Context, &request)
	if err != nil {
		return fmt.Errorf("get metadata: %w", err)
	}

	fmt.Fprintf(appCtx.App.Writer, "Repository ID: %d\n", metadata.GetRepositoryId())
	fmt.Fprintf(appCtx.App.Writer, "Virtual Storage: %q\n", metadata.GetVirtualStorage())
	fmt.Fprintf(appCtx.App.Writer, "Relative Path: %q\n", metadata.GetRelativePath())
	fmt.Fprintf(appCtx.App.Writer, "Replica Path: %q\n", metadata.GetReplicaPath())
	fmt.Fprintf(appCtx.App.Writer, "Primary: %q\n", metadata.GetPrimary())
	fmt.Fprintf(appCtx.App.Writer, "Generation: %d\n", metadata.GetGeneration())
	fmt.Fprintf(appCtx.App.Writer, "Replicas:\n")
	for _, replica := range metadata.GetReplicas() {
		fmt.Fprintf(appCtx.App.Writer, "- Storage: %q\n", replica.GetStorage())
		fmt.Fprintf(appCtx.App.Writer, "  Assigned: %v\n", replica.GetAssigned())

		generationText := fmt.Sprintf("%d, fully up to date", replica.GetGeneration())
		if replica.GetGeneration() == -1 {
			generationText = "replica not yet created"
		} else if replica.GetGeneration() < metadata.GetGeneration() {
			generationText = fmt.Sprintf("%d, behind by %d changes", replica.GetGeneration(), metadata.GetGeneration()-replica.GetGeneration())
		}

		verifiedAt := "unverified"
		if replica.GetVerifiedAt().IsValid() {
			verifiedAt = replica.GetVerifiedAt().AsTime().String()
		}

		fmt.Fprintf(appCtx.App.Writer, "  Generation: %s\n", generationText)
		fmt.Fprintf(appCtx.App.Writer, "  Healthy: %v\n", replica.GetHealthy())
		fmt.Fprintf(appCtx.App.Writer, "  Valid Primary: %v\n", replica.GetValidPrimary())
		fmt.Fprintf(appCtx.App.Writer, "  Verified At: %s\n", verifiedAt)
	}
	return nil
}
