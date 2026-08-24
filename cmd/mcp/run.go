package mcp

import (
	"github.com/spf13/cobra"

	"go.5x5.cz/ptah/cmd/internal/agentflags"
	"go.5x5.cz/ptah/cmd/internal/cmdutil"
	"go.5x5.cz/ptah/internal/agentaudit"
	"go.5x5.cz/ptah/internal/buildinfo"
	"go.5x5.cz/ptah/internal/mcpserver"
)

// run resolves the operator's configuration and serves until the session ends.
//
// The resolution is agentflags', not this command's: `ptah assist` asks the
// same questions and has to get the same answers, and two copies of it would
// stop agreeing the first time one grew a flag.
func run(cmd *cobra.Command, opts *agentflags.Options) error {
	session, cleanup, err := agentflags.Build(cmd, opts, agentaudit.SurfaceMCP)
	if err != nil {
		return cmdutil.Fail(cmd, err)
	}
	defer cleanup()

	return mcpserver.Run(cmd.Context(), mcpserver.Config{
		Version: buildinfo.Resolve().Version,
		Session: session,
	})
}
