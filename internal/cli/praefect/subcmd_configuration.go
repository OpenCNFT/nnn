package praefect

import "github.com/urfave/cli/v2"

const configurationCmdName = "configuration"

func newConfigurationCommand() *cli.Command {
	return &cli.Command{
		Name:        configurationCmdName,
		Usage:       "manage configuration",
		Description: "Manage Praefect configuration.",
		Subcommands: []*cli.Command{
			newConfigurationValidateCommand(),
		},
	}
}
