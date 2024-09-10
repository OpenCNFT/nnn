package praefect

import (
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"

	"github.com/urfave/cli/v2"
	"gitlab.com/gitlab-org/gitaly/v16/internal/log"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
)

func newDatalossCommand() *cli.Command {
	return &cli.Command{
		Name:  "dataloss",
		Usage: "identify potential data loss in repositories",
		Description: `Search through incomplete replication jobs to identify potential data loss in repositories
with out-of-date Gitaly nodes.

Detects potential data loss where repositories are no longer accessible because all of the Gitaly nodes with up-to-date
copies are unavailable. This can happen if a primary node becomes inaccessible before completing replication to other
Gitaly nodes.

For each unavailable repository with potential data loss, the output lists:

- Gitaly relative path.
- The primary Gitaly node.
- The up-to-date Gitaly nodes (in-sync storages).
- Any out-of-date Gitaly nodes (outdated storages).

Example: praefect --config praefect.config.toml dataloss`,
		HideHelpCommand: true,
		Action:          datalossAction,
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name: paramVirtualStorage,
				Usage: "specifies which virtual storage to check for data loss. If not specified,\n" +
					"the check is performed for every configured virtual storage.",
			},
			&cli.BoolFlag{
				Name:  "partially-unavailable",
				Value: false,
				Usage: "Additionally include repositories which are available but some assigned replicas\n" +
					"are unavailable. Such repositories are available but are not fully replicated. This\n" +
					"increases the chance of data loss on primary failure",
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

func datalossAction(ctx *cli.Context) error {
	logger := log.ConfigureCommand()

	conf, err := readConfig(ctx.String(configFlagName))
	if err != nil {
		return err
	}

	includePartiallyAvailable := ctx.Bool("partially-unavailable")

	virtualStorages := []string{ctx.String(paramVirtualStorage)}
	if virtualStorages[0] == "" {
		virtualStorages = make([]string, len(conf.VirtualStorages))
		for i := range conf.VirtualStorages {
			virtualStorages[i] = conf.VirtualStorages[i].Name
		}
	}
	sort.Strings(virtualStorages)

	nodeAddr, err := getNodeAddress(conf)
	if err != nil {
		return err
	}

	conn, err := subCmdDial(ctx.Context, nodeAddr, conf.Auth.Token, defaultDialTimeout)
	if err != nil {
		return fmt.Errorf("error dialing: %w", err)
	}
	defer func() {
		if err := conn.Close(); err != nil {
			logger.WithError(err).Info("error closing connection")
		}
	}()

	client := gitalypb.NewPraefectInfoServiceClient(conn)

	for _, vs := range virtualStorages {
		stream, err := client.Dataloss(ctx.Context, &gitalypb.DatalossRequest{
			VirtualStorage:             vs,
			IncludePartiallyReplicated: includePartiallyAvailable,
		})
		if err != nil {
			return fmt.Errorf("error checking: %w", err)
		}

		var foundUnavailableRepos bool

		indentPrintln(ctx.App.Writer, 0, "Virtual storage: %s", vs)
		for {
			resp, err := stream.Recv()
			if err != nil {
				if !errors.Is(err, io.EOF) {
					return fmt.Errorf("getting response: %w", err)
				}

				break
			}

			if len(resp.Repositories) > 0 {
				indentPrintln(ctx.App.Writer, 1, "Repositories:")
			}

			for _, repo := range resp.Repositories {
				foundUnavailableRepos = true

				unavailable := ""
				if repo.Unavailable {
					unavailable = " (unavailable)"
				}

				indentPrintln(ctx.App.Writer, 2, "%s%s:", repo.RelativePath, unavailable)

				primary := repo.Primary
				if primary == "" {
					primary = "No Primary"
				}
				indentPrintln(ctx.App.Writer, 3, "Primary: %s", primary)

				indentPrintln(ctx.App.Writer, 3, "In-Sync Storages:")
				for _, storage := range repo.Storages {
					if storage.BehindBy != 0 {
						continue
					}

					indentPrintln(ctx.App.Writer, 4, "%s%s%s",
						storage.Name,
						assignedMessage(storage.Assigned),
						unhealthyMessage(storage.Healthy),
					)
				}

				indentPrintln(ctx.App.Writer, 3, "Outdated Storages:")
				for _, storage := range repo.Storages {
					if storage.BehindBy == 0 {
						continue
					}

					plural := ""
					if storage.BehindBy > 1 {
						plural = "s"
					}

					indentPrintln(ctx.App.Writer, 4, "%s is behind by %d change%s or less%s%s",
						storage.Name,
						storage.BehindBy,
						plural,
						assignedMessage(storage.Assigned),
						unhealthyMessage(storage.Healthy),
					)
				}
			}
		}

		if !foundUnavailableRepos {
			msg := "All repositories are available!"
			if includePartiallyAvailable {
				msg = "All repositories are fully available on all assigned storages!"
			}

			indentPrintln(ctx.App.Writer, 1, "%s", msg)
			continue
		}
	}

	return nil
}

func indentPrintln(w io.Writer, indent int, msg string, args ...interface{}) {
	fmt.Fprint(w, strings.Repeat("  ", indent))
	fmt.Fprintf(w, msg, args...)
	fmt.Fprint(w, "\n")
}

func unhealthyMessage(healthy bool) string {
	if healthy {
		return ""
	}

	return ", unhealthy"
}

func assignedMessage(assigned bool) string {
	assignedMsg := ""
	if assigned {
		assignedMsg = ", assigned host"
	}

	return assignedMsg
}
