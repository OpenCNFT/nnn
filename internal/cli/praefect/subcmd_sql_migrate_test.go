package praefect

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/urfave/cli/v2"
	"gitlab.com/gitlab-org/gitaly/v16/internal/praefect/config"
	"gitlab.com/gitlab-org/gitaly/v16/internal/praefect/datastore/migrations"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper/testdb"
)

func TestSubCmdSqlMigrate(t *testing.T) {
	db := testdb.New(t)
	dbCfg := testdb.GetConfig(t, db.Name)
	cfg := config.Config{
		ListenAddr:      "/dev/null",
		VirtualStorages: []*config.VirtualStorage{{Name: "p", Nodes: []*config.Node{{Storage: "s", Address: "localhost"}}}},
		DB:              dbCfg,
	}
	confPath := writeConfigToFile(t, cfg)

	migrationCt := len(migrations.All())

	for _, tc := range []struct {
		desc           string
		up             int
		args           []string
		expectedOutput []string
		expectedErr    error
	}{
		{
			desc:        "unexpected positional arguments",
			args:        []string{"positonal-arg"},
			expectedErr: cli.Exit(unexpectedPositionalArgsError{Command: "sql-migrate"}, 1),
		},
		{
			desc:           "All migrations up",
			up:             migrationCt,
			expectedOutput: []string{"praefect sql-migrate: all migrations are up"},
		},
		{
			desc: "All migrations down",
			up:   0,
			expectedOutput: []string{
				fmt.Sprintf("praefect sql-migrate: migrations to apply: %d", migrationCt),
				"20200109161404_hello_world: migrating",
				"20200109161404_hello_world: applied (",
				fmt.Sprintf("praefect sql-migrate: OK (applied %d migrations)", migrationCt),
			},
		},
		{
			desc: "Some migrations down",
			up:   10,
			expectedOutput: []string{
				fmt.Sprintf("praefect sql-migrate: migrations to apply: %d", migrationCt-10),
				"20201126165633_repository_assignments_table: migrating",
				"20201126165633_repository_assignments_table: applied (",
				fmt.Sprintf("praefect sql-migrate: OK (applied %d migrations)", migrationCt-10),
			},
		},
		{
			desc: "Verbose output",
			up:   0,
			args: []string{"-verbose"},
			expectedOutput: []string{
				fmt.Sprintf("praefect sql-migrate: migrations to apply: %d", migrationCt),
				"20200109161404_hello_world: migrating",
				"[CREATE TABLE hello_world (id integer)]",
				"20200109161404_hello_world: applied (",
				fmt.Sprintf("praefect sql-migrate: OK (applied %d migrations)", migrationCt),
			},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			testdb.SetMigrations(t, db, cfg, tc.up)

			var stdout bytes.Buffer
			var stderr bytes.Buffer
			app := cli.App{
				Reader:          bytes.NewReader(nil),
				Writer:          &stdout,
				ErrWriter:       &stderr,
				HideHelpCommand: true,
				Commands: []*cli.Command{
					newSQLMigrateCommand(),
				},
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:  "config",
						Value: confPath,
					},
				},
			}
			err := app.Run(append([]string{progname, sqlMigrateCmdName, "-ignore-unknown"}, tc.args...))
			require.Equal(t, tc.expectedErr, err)
			assert.Empty(t, stderr.String())
			for _, out := range tc.expectedOutput {
				assert.Contains(t, stdout.String(), out)
			}
		})
	}
}
