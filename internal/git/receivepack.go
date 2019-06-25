package git

import (
	"fmt"

	"gitlab.com/gitlab-org/gitaly/internal/config"
	"gitlab.com/gitlab-org/gitaly/internal/git/hooks"
)

type ReceivePackRequest interface {
	GetGlId() string
	GetGlUsername() string
	GetGlRepository() string
}

func ReceivePackEnv(req ReceivePackRequest) []string {
	return []string{
		fmt.Sprintf("GL_ID=%s", req.GetGlId()),
		fmt.Sprintf("GL_USERNAME=%s", req.GetGlUsername()),
		fmt.Sprintf("GITLAB_SHELL_DIR=%s", config.Config.GitlabShell.Dir),
		fmt.Sprintf("GL_REPOSITORY=%s", req.GetGlRepository()),
	}
}

func ReceivePackConfig() []string {
	return []string{
		fmt.Sprintf("core.hooksPath=%s", hooks.Path()),

		// In case the repository belongs to an object pool, we want to prevent
		// Git from including the pool's refs in the ref advertisement. We do
		// this by rigging core.alternateRefsCommand to produce no output.
		// Because Git itself will append the pool repository directory, the
		// command ends with a "#". The end result is that Git runs `/bin/sh -c 'exit 0 # /path/to/pool.git`.
		"core.alternateRefsCommand=exit 0 #",
	}
}
