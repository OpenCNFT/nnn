package helper

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"syscall"
	"time"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus"

	"gitlab.com/gitlab-org/gitaly/internal/config"
	"gitlab.com/gitlab-org/gitaly/internal/middleware/objectdirhandler"

	log "github.com/Sirupsen/logrus"
)

// Command encapsulates operations with commands creates with NewCommand
type Command struct {
	io.Reader
	*exec.Cmd
	context   context.Context
	startTime time.Time
}

// GitPath returns the path to the `git` binary. See `SetGitPath` for details
// on how this is set
func GitPath() string {
	if config.Config.Git.BinPath == "" {
		// This shouldn't happen outside of testing, SetGitPath should be called by
		// main.go to ensure correctness of the configuration on start-up.
		if err := config.SetGitPath(); err != nil {
			log.Fatal(err) // Bail out.
		}
	}

	return config.Config.Git.BinPath
}

// GitCommandReader creates a git Command with the given args
func GitCommandReader(ctx context.Context, args ...string) (*Command, error) {
	return NewCommand(ctx, exec.Command(GitPath(), args...), nil, nil, nil)
}

// NewCommand creates a Command from an exec.Cmd
func NewCommand(ctx context.Context, cmd *exec.Cmd, stdin io.Reader, stdout, stderr io.Writer, env ...string) (*Command, error) {
	grpc_logrus.Extract(ctx).WithFields(log.Fields{
		"path": cmd.Path,
		"args": cmd.Args,
	}).Info("spawn")

	command := &Command{Cmd: cmd, startTime: time.Now(), context: ctx}

	// Explicitly set the environment for the command
	cmd.Env = []string{
		fmt.Sprintf("HOME=%s", os.Getenv("HOME")),
		fmt.Sprintf("PATH=%s", os.Getenv("PATH")),
		fmt.Sprintf("LD_LIBRARY_PATH=%s", os.Getenv("LD_LIBRARY_PATH")),
		fmt.Sprintf("TZ=%s", os.Getenv("TZ")),
	}
	cmd.Env = append(cmd.Env, env...)
	if dir, ok := objectdirhandler.ObjectDir(ctx); ok {
		cmd.Env = append(cmd.Env, fmt.Sprintf("GIT_OBJECT_DIRECTORY=%s", dir))
	}
	if dirs, ok := objectdirhandler.AltObjectDirs(ctx); ok {
		dirsList := strings.Join(dirs, ":")
		cmd.Env = append(cmd.Env, fmt.Sprintf("GIT_ALTERNATE_OBJECT_DIRECTORIES=%s", dirsList))
	}

	// Start the command in its own process group (nice for signalling)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

	if stdin != nil {
		cmd.Stdin = stdin
	}

	if stdout != nil {
		// We don't assign a reader if an stdout override was passed. We assume
		// output is going to be directly handled by the caller.
		cmd.Stdout = stdout
	} else {
		pipe, err := cmd.StdoutPipe()
		if err != nil {
			return nil, fmt.Errorf("GitCommand: stdout: %v", err)
		}
		command.Reader = pipe
	}

	if stderr != nil {
		cmd.Stderr = stderr
	} else {
		// If we don't do something with cmd.Stderr, Git errors will be lost
		cmd.Stderr = os.Stderr
	}

	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("GitCommand: start %v: %v", cmd.Args, err)
	}

	return command, nil
}

// Close will send a SIGTERM signal to the process group
// belonging to the `cmd` process
func (c *Command) Close() error {
	cmd := c.Cmd
	ctx := c.context

	process := cmd.Process
	if process != nil && process.Pid > 0 {
		// Send SIGTERM to the process group of cmd
		syscall.Kill(-process.Pid, syscall.SIGTERM)
	}

	// reap our child process
	err := cmd.Wait()

	exitCode := 0
	if err != nil {
		if exitStatus, ok := ExitStatus(err); ok {
			exitCode = exitStatus
		}
	}

	c.logProcessComplete(ctx, exitCode)

	return err
}

// ExitStatus will return the exit-code from an error
func ExitStatus(err error) (int, bool) {
	exitError, ok := err.(*exec.ExitError)
	if !ok {
		return 0, false
	}

	waitStatus, ok := exitError.Sys().(syscall.WaitStatus)
	if !ok {
		return 0, false
	}

	return waitStatus.ExitStatus(), true
}

func (c *Command) logProcessComplete(ctx context.Context, exitCode int) {
	cmd := c.Cmd

	systemTime := cmd.ProcessState.SystemTime()
	userTime := cmd.ProcessState.UserTime()
	realTime := time.Now().Sub(c.startTime)

	entry := grpc_logrus.Extract(ctx).WithFields(log.Fields{
		"path":                   cmd.Path,
		"args":                   cmd.Args,
		"command.exitCode":       exitCode,
		"command.system_time_ms": systemTime.Seconds() * 1000,
		"command.user_time_ms":   userTime.Seconds() * 1000,
		"command.real_time_ms":   realTime.Seconds() * 1000,
	})

	if rusage, ok := cmd.ProcessState.SysUsage().(*syscall.Rusage); ok {
		entry = entry.WithFields(log.Fields{
			"command.maxrss":  rusage.Maxrss,
			"command.inblock": rusage.Inblock,
			"command.oublock": rusage.Oublock,
		})
	}

	entry.Info("spawn complete")
}
