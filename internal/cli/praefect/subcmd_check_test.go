package praefect

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/urfave/cli/v2"
	"gitlab.com/gitlab-org/gitaly/v15/internal/praefect/config"
	"gitlab.com/gitlab-org/gitaly/v15/internal/praefect/service"
)

func TestCheckSubcommand(t *testing.T) {
	t.Parallel()
	conf := config.Config{
		ListenAddr: ":0",
		VirtualStorages: []*config.VirtualStorage{
			{
				Name: "vs",
				Nodes: []*config.Node{
					{Address: "stub", Storage: "st"},
				},
			},
		},
	}

	confPath := writeConfigToFile(t, conf)

	testCases := []struct {
		desc                string
		checks              []service.CheckFunc
		args                []string
		expectedQuietOutput string
		expectedOutput      string
		expectedError       error
	}{
		{
			desc:          "positional arguments",
			args:          []string{"positional-arg"},
			expectedError: cli.Exit(unexpectedPositionalArgsError{Command: "check"}, 1),
		},
		{
			desc: "all checks pass",
			checks: []service.CheckFunc{
				func(cfg config.Config, w io.Writer, quiet bool) *service.Check {
					return &service.Check{
						Name:        "check 1",
						Description: "checks a",
						Run:         func(ctx context.Context) error { return nil },
						Severity:    service.Fatal,
					}
				},
				func(cfg config.Config, w io.Writer, quiet bool) *service.Check {
					return &service.Check{
						Name:        "check 2",
						Description: "checks b",
						Run:         func(ctx context.Context) error { return nil },
						Severity:    service.Fatal,
					}
				},
				func(cfg config.Config, w io.Writer, quiet bool) *service.Check {
					return &service.Check{
						Name:        "check 3",
						Description: "checks c",
						Run:         func(ctx context.Context) error { return nil },
						Severity:    service.Fatal,
					}
				},
			},
			expectedOutput: `Checking check 1 - checks a [fatal]
Passed
Checking check 2 - checks b [fatal]
Passed
Checking check 3 - checks c [fatal]
Passed

All checks passed.
`,
			expectedQuietOutput: `Checking check 1...Passed
Checking check 2...Passed
Checking check 3...Passed

All checks passed.
`,
			expectedError: nil,
		},
		{
			desc: "a fatal check fails",
			checks: []service.CheckFunc{
				func(cfg config.Config, w io.Writer, quiet bool) *service.Check {
					return &service.Check{
						Name:        "check 1",
						Description: "checks a",
						Run:         func(ctx context.Context) error { return nil },
						Severity:    service.Fatal,
					}
				},
				func(cfg config.Config, w io.Writer, quiet bool) *service.Check {
					return &service.Check{
						Name:        "check 2",
						Description: "checks b",
						Run:         func(ctx context.Context) error { return errors.New("i failed") },
						Severity:    service.Fatal,
					}
				},
				func(cfg config.Config, w io.Writer, quiet bool) *service.Check {
					return &service.Check{
						Name:        "check 3",
						Description: "checks c",
						Run:         func(ctx context.Context) error { return nil },
						Severity:    service.Fatal,
					}
				},
			},
			expectedOutput: `Checking check 1 - checks a [fatal]
Passed
Checking check 2 - checks b [fatal]
Failed (fatal) error: i failed
Checking check 3 - checks c [fatal]
Passed

1 check(s) failed, at least one was fatal.
`,
			expectedQuietOutput: `Checking check 1...Passed
Checking check 2...Failed (fatal) error: i failed
Checking check 3...Passed

1 check(s) failed, at least one was fatal.
`,
			expectedError: errFatalChecksFailed,
		},
		{
			desc: "only warning checks fail",
			checks: []service.CheckFunc{
				func(cfg config.Config, w io.Writer, quiet bool) *service.Check {
					return &service.Check{
						Name:        "check 1",
						Description: "checks a",
						Run:         func(ctx context.Context) error { return nil },
						Severity:    service.Fatal,
					}
				},
				func(cfg config.Config, w io.Writer, quiet bool) *service.Check {
					return &service.Check{
						Name:        "check 2",
						Description: "checks b",
						Run:         func(ctx context.Context) error { return errors.New("i failed but not too badly") },
						Severity:    service.Warning,
					}
				},
				func(cfg config.Config, w io.Writer, quiet bool) *service.Check {
					return &service.Check{
						Name:        "check 3",
						Description: "checks c",
						Run:         func(ctx context.Context) error { return errors.New("i failed but not too badly") },
						Severity:    service.Warning,
					}
				},
			},
			expectedOutput: `Checking check 1 - checks a [fatal]
Passed
Checking check 2 - checks b [warning]
Failed (warning) error: i failed but not too badly
Checking check 3 - checks c [warning]
Failed (warning) error: i failed but not too badly

2 check(s) failed, but none are fatal.
`,
			expectedQuietOutput: `Checking check 1...Passed
Checking check 2...Failed (warning) error: i failed but not too badly
Checking check 3...Failed (warning) error: i failed but not too badly

2 check(s) failed, but none are fatal.
`,
			expectedError: nil,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			var stdout bytes.Buffer
			app := cli.App{
				Writer: &stdout,
				Commands: []*cli.Command{
					newCheckCommand(tc.checks),
				},
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:  "config",
						Value: confPath,
					},
				},
			}

			t.Run("quiet", func(t *testing.T) {
				stdout.Reset()
				err := app.Run(append([]string{progname, "check", "-q"}, tc.args...))
				assert.Equal(t, tc.expectedError, err)
				if len(tc.args) == 0 {
					assert.Equal(t, tc.expectedQuietOutput, stdout.String())
				}
			})

			t.Run("normal", func(t *testing.T) {
				stdout.Reset()
				err := app.Run(append([]string{progname, "check"}, tc.args...))
				assert.Equal(t, tc.expectedError, err)
				if len(tc.args) == 0 {
					assert.Equal(t, tc.expectedOutput, stdout.String())
				}
			})
		})
	}
}
