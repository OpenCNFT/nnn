package main

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"text/template"
	"time"

	"github.com/urfave/cli/v2"
	"gitlab.com/gitlab-org/gitaly/v16/client"
	"google.golang.org/grpc"
)

type gitalyConfig struct {
	SocketPath     string
	BinDir         string
	Dir            string
	UseBundledGit  bool
	GitPath        string
	GitalyDir      string
	GitlabShellDir string
}

const configTemplate = `
socket_path = "{{.SocketPath}}"
bin_dir = "{{.BinDir}}"

[[storage]]
name = "default"
path = "{{.Dir}}"

[git]
use_bundled_binaries = {{.UseBundledGit}}
bin_path = "{{.GitPath}}"

[gitlab-shell]
dir = "{{.GitlabShellDir}}"

[gitlab]
url = 'http://gitlab_url'
`

func checkVersion(gitalyDir, gitalyBin string) error {
	versionCmd := exec.Command(gitalyBin, "-version")
	versionOutput, err := versionCmd.Output()
	if err != nil {
		return fmt.Errorf("failed to get Gitaly version output: %w", err)
	}

	version := strings.TrimSpace(strings.TrimPrefix(string(versionOutput), "Gitaly, version "))

	versionFromFile, err := os.ReadFile(filepath.Join(gitalyDir, "VERSION"))
	if err != nil {
		return fmt.Errorf("failed to read Gitaly version file: %w", err)
	}
	// Use strings.HasPrefix() because the version output could use git describe, if it is a source install
	// e.g.: Gitaly, version 1.75.0-14-gd1ecb43f
	if !strings.HasPrefix(version, string(versionFromFile)) {
		return fmt.Errorf("version check failed: VERSION file contained %q\n"+ //nolint:stylecheck
			"but 'gitaly -version' reported %q.\n"+
			"If you are working from a fork, please fetch the latest tags.\n",
			versionFromFile, version)
	}

	return nil
}

func writeGitalyConfig(path string, params gitalyConfig) error {
	t, err := template.New("config").Parse(configTemplate)
	if err != nil {
		return fmt.Errorf("parse template: %w", err)
	}

	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("create Gitaly config file: %w", err)
	}
	defer f.Close()

	if err := t.Execute(f, params); err != nil {
		return fmt.Errorf("generate Gitaly config file: %w", err)
	}

	return nil
}

func spawnAndWait(ctx context.Context, gitalyBin, configPath, socketPath string) (returnedError error) {
	cmd := exec.CommandContext(ctx, gitalyBin, configPath)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	if err := cmd.Start(); err != nil {
		return fmt.Errorf("start gitaly: %w", err)
	}

	defer func() {
		_ = cmd.Process.Signal(syscall.SIGTERM)
		_ = cmd.Wait()
		if returnedError != nil {
			fmt.Fprintf(os.Stdout, "%s\n", stdout.String())
			fmt.Fprintf(os.Stderr, "%s\n", stderr.String())
		}
	}()

	start := time.Now()
	for i := 0; i < 100; i++ {
		ctx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)

		conn, err := client.DialContext(ctx, "unix://"+socketPath, []grpc.DialOption{
			grpc.WithBlock(),
		})

		cancel()

		if err != nil {
			fmt.Printf(".")
			continue
		}

		fmt.Printf("\n\nconnection established after %v\n\n", time.Since(start))
		conn.Close()
		return nil
	}

	fmt.Println("")

	return fmt.Errorf("failed to connect to gitaly after %v", time.Since(start))
}

func testBoot(appCtx *cli.Context) error {
	ctx := appCtx.Context

	useBundledGit := appCtx.Bool("bundled-git")

	gitalyDir := appCtx.String("gitaly-directory")
	buildDir := filepath.Join(gitalyDir, "_build")
	binDir := filepath.Join(buildDir, "bin")

	gitPath := filepath.Join(buildDir, "deps", "git-distribution", "bin-wrappers", "git")
	if useBundledGit {
		gitPath = ""
	}

	gitalyBin := filepath.Join(binDir, "gitaly")
	if err := checkVersion(gitalyDir, gitalyBin); err != nil {
		return err
	}

	tempDir, err := os.MkdirTemp("", "gitaly-test-boot")
	if err != nil {
		return fmt.Errorf("create temp directory: %w", err)
	}
	defer func() {
		_ = os.RemoveAll(tempDir)
	}()

	gitlabShellDir := filepath.Join(tempDir, "gitlab-shell")
	if err := os.Mkdir(gitlabShellDir, 0o755); err != nil {
		return fmt.Errorf("create gitlab-shell directory: %w", err)
	}

	err = os.WriteFile(filepath.Join(gitlabShellDir, ".gitlab_shell_secret"), []byte("test_gitlab_shell_token"), 0o644)
	if err != nil {
		return fmt.Errorf("write gitlab-shell secret: %w", err)
	}

	socketPath := filepath.Join(tempDir, "socket")
	configPath := filepath.Join(tempDir, "config.toml")
	err = writeGitalyConfig(configPath,
		gitalyConfig{
			SocketPath:     socketPath,
			BinDir:         binDir,
			Dir:            tempDir,
			UseBundledGit:  useBundledGit,
			GitPath:        gitPath,
			GitalyDir:      gitalyDir,
			GitlabShellDir: gitlabShellDir,
		})
	if err != nil {
		return nil
	}

	if err := spawnAndWait(ctx, gitalyBin, configPath, socketPath); err != nil {
		return err
	}
	return nil
}

func main() {
	app := cli.App{
		Name:            "test-boot",
		Usage:           "smoke-test the bootup process of Gitaly",
		Action:          testBoot,
		HideHelpCommand: true,
		Flags: []cli.Flag{
			&cli.BoolFlag{
				Name:  "bundled-git",
				Usage: "Set up Gitaly with bundled Git binaries",
			},
			&cli.PathFlag{
				Name:  "gitaly-directory",
				Usage: "Path of the Gitaly directory.",
				Value: ".",
			},
		},
		Before: func(ctx *cli.Context) error {
			if ctx.Args().Present() {
				_ = cli.ShowSubcommandHelp(ctx)
				return cli.Exit("this command does not accept positional arguments", 1)
			}

			return nil
		},
	}

	if err := app.Run(os.Args); err != nil {
		log.Fatal(err)
	}
}
