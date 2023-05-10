package praefect

import (
	"bytes"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/urfave/cli/v2"
	"gitlab.com/gitlab-org/gitaly/v15/internal/praefect/config"
	"gitlab.com/gitlab-org/gitaly/v15/internal/praefect/datastore"
	"gitlab.com/gitlab-org/gitaly/v15/internal/praefect/service/info"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper/testdb"
)

func TestDatalossSubcommand(t *testing.T) {
	t.Parallel()
	cfg := config.Config{
		ListenAddr: ":0",
		VirtualStorages: []*config.VirtualStorage{
			{
				Name: "virtual-storage-1",
				Nodes: []*config.Node{
					{Storage: "gitaly-1"},
					{Storage: "gitaly-2"},
					{Storage: "gitaly-3"},
				},
			},
			{
				Name: "virtual-storage-2",
				Nodes: []*config.Node{
					{Storage: "gitaly-4"},
				},
			},
		},
	}

	tx := testdb.New(t).Begin(t)
	defer tx.Rollback(t)
	ctx := testhelper.Context(t)

	testdb.SetHealthyNodes(t, ctx, tx, map[string]map[string][]string{"praefect-0": {
		"virtual-storage-1": {"gitaly-1", "gitaly-3"},
	}})
	gs := datastore.NewPostgresRepositoryStore(tx, cfg.StorageNames())

	for _, q := range []string{
		`
				INSERT INTO repositories (repository_id, virtual_storage, relative_path, "primary")
				VALUES
					(1, 'virtual-storage-1', 'repository-1', 'gitaly-1'),
					(2, 'virtual-storage-1', 'repository-2', 'gitaly-3')
				`,
		`
				INSERT INTO repository_assignments (repository_id, virtual_storage, relative_path, storage)
				VALUES
					(1, 'virtual-storage-1', 'repository-1', 'gitaly-1'),
					(1, 'virtual-storage-1', 'repository-1', 'gitaly-2'),
					(2, 'virtual-storage-1', 'repository-2', 'gitaly-1'),
					(2, 'virtual-storage-1', 'repository-2', 'gitaly-3')
				`,
	} {
		_, err := tx.ExecContext(ctx, q)
		require.NoError(t, err)
	}

	require.NoError(t, gs.SetGeneration(ctx, 1, "gitaly-1", "repository-1", 1))
	require.NoError(t, gs.SetGeneration(ctx, 1, "gitaly-2", "repository-1", 0))
	require.NoError(t, gs.SetGeneration(ctx, 1, "gitaly-3", "repository-1", 0))

	require.NoError(t, gs.SetGeneration(ctx, 2, "gitaly-2", "repository-2", 1))
	require.NoError(t, gs.SetGeneration(ctx, 2, "gitaly-3", "repository-2", 0))

	ln, clean := listenAndServe(t, []svcRegistrar{
		registerPraefectInfoServer(info.NewServer(cfg, gs, nil, nil, nil)),
	})
	defer clean()
	cfg.SocketPath = ln.Addr().String()

	for _, tc := range []struct {
		desc            string
		args            []string
		virtualStorages []*config.VirtualStorage
		output          string
		error           error
	}{
		{
			desc:            "positional arguments",
			args:            []string{"-virtual-storage=virtual-storage-1", "positional-arg"},
			virtualStorages: []*config.VirtualStorage{{Name: "virtual-storage", Nodes: []*config.Node{{Storage: "s", Address: "a"}}}},
			error:           cli.Exit(unexpectedPositionalArgsError{Command: "dataloss"}, 1),
		},
		{
			desc:            "data loss with unavailable repositories",
			args:            []string{"-virtual-storage=virtual-storage-1"},
			virtualStorages: []*config.VirtualStorage{{Name: "virtual-storage", Nodes: []*config.Node{{Storage: "s", Address: "a"}}}},
			output: `Virtual storage: virtual-storage-1
  Repositories:
    repository-2 (unavailable):
      Primary: gitaly-3
      In-Sync Storages:
        gitaly-2, unhealthy
      Outdated Storages:
        gitaly-1 is behind by 2 changes or less, assigned host
        gitaly-3 is behind by 1 change or less, assigned host
`,
		},
		{
			desc:            "data loss with partially unavailable repositories",
			args:            []string{"-virtual-storage=virtual-storage-1", "-partially-unavailable"},
			virtualStorages: []*config.VirtualStorage{{Name: "virtual-storage", Nodes: []*config.Node{{Storage: "s", Address: "a"}}}},
			output: `Virtual storage: virtual-storage-1
  Repositories:
    repository-1:
      Primary: gitaly-1
      In-Sync Storages:
        gitaly-1, assigned host
      Outdated Storages:
        gitaly-2 is behind by 1 change or less, assigned host, unhealthy
        gitaly-3 is behind by 1 change or less
    repository-2 (unavailable):
      Primary: gitaly-3
      In-Sync Storages:
        gitaly-2, unhealthy
      Outdated Storages:
        gitaly-1 is behind by 2 changes or less, assigned host
        gitaly-3 is behind by 1 change or less, assigned host
`,
		},
		{
			desc: "multiple virtual storages with unavailable repositories",
			virtualStorages: []*config.VirtualStorage{
				{Name: "virtual-storage-2", Nodes: []*config.Node{{Storage: "s", Address: "a"}}},
				{Name: "virtual-storage-1", Nodes: []*config.Node{{Storage: "s", Address: "a"}}},
			},
			output: `Virtual storage: virtual-storage-1
  Repositories:
    repository-2 (unavailable):
      Primary: gitaly-3
      In-Sync Storages:
        gitaly-2, unhealthy
      Outdated Storages:
        gitaly-1 is behind by 2 changes or less, assigned host
        gitaly-3 is behind by 1 change or less, assigned host
Virtual storage: virtual-storage-2
  All repositories are available!
`,
		},
		{
			desc: "multiple virtual storages with partially unavailable repositories",
			args: []string{"-partially-unavailable"},
			virtualStorages: []*config.VirtualStorage{
				{Name: "virtual-storage-2", Nodes: []*config.Node{{Storage: "s", Address: "a"}}},
				{Name: "virtual-storage-1", Nodes: []*config.Node{{Storage: "s", Address: "a"}}},
			},
			output: `Virtual storage: virtual-storage-1
  Repositories:
    repository-1:
      Primary: gitaly-1
      In-Sync Storages:
        gitaly-1, assigned host
      Outdated Storages:
        gitaly-2 is behind by 1 change or less, assigned host, unhealthy
        gitaly-3 is behind by 1 change or less
    repository-2 (unavailable):
      Primary: gitaly-3
      In-Sync Storages:
        gitaly-2, unhealthy
      Outdated Storages:
        gitaly-1 is behind by 2 changes or less, assigned host
        gitaly-3 is behind by 1 change or less, assigned host
Virtual storage: virtual-storage-2
  All repositories are fully available on all assigned storages!
`,
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			cfg.VirtualStorages = tc.virtualStorages
			confPath := writeConfigToFile(t, cfg)

			var stdout bytes.Buffer
			app := cli.App{
				Reader:          bytes.NewReader(nil),
				Writer:          &stdout,
				ErrWriter:       io.Discard,
				HideHelpCommand: true,
				Commands: []*cli.Command{
					newDatalossCommand(),
				},
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:  "config",
						Value: confPath,
					},
				},
			}

			err := app.Run(append([]string{progname, "dataloss"}, tc.args...))
			require.Equal(t, tc.error, err, err)
			if tc.error == nil {
				require.Equal(t, tc.output, stdout.String())
			}
		})
	}
}
