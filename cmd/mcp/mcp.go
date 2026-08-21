// Package mcp implements "ptah mcp", the Model Context Protocol server that
// exposes Ptah's read-only operations to external AI clients.
package mcp

import (
	"github.com/spf13/cobra"

	"go.5x5.cz/ptah/cmd/internal/cmdutil"
	"go.5x5.cz/ptah/internal/buildinfo"
	"go.5x5.cz/ptah/internal/mcpserver"
)

// NewCommand returns the MCP server command.
func NewCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "mcp",
		Short: "Serve Ptah's read-only operations over the Model Context Protocol",
		Long: `Serve Ptah's read-only operations to an AI client over the Model Context
Protocol, on stdin and stdout.

This is not a command to run by hand: an MCP client starts it and speaks the
protocol to it. Point one at the Ptah binary with "mcp" as the argument.

Every tool it exposes reads. Nothing here applies a migration, writes a file, or
changes a database, and the operation set is frozen to that in ADR 0002. Three
of Ptah's own reading verbs are deliberately absent -- schema inspect, schema
diff and migrations lint -- because each needs a scratch database it resets
destructively, and a destructive capability must not sit behind a read-only name
on a surface an agent drives.

The tools are:

  ptah_validate_schema   structural problems in a declared schema, no database
  ptah_render_schema     the DDL a declared schema becomes, in dependency order
  ptah_schema_lineage    which base columns feed each view column
  ptah_read_database     the schema a live database currently holds

Credentials are the client's to supply. This server holds none, stores none, and
sends nothing anywhere: it runs locally and talks to whatever the caller names.`,
		Args:          cmdutil.NoPositionalArgs,
		SilenceErrors: true,
		SilenceUsage:  true,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return mcpserver.Run(cmd.Context(), buildinfo.Resolve().Version)
		},
	}
	cmdutil.ConfigureCommandArgs(cmd, cmdutil.NoPositionalArgs)
	return cmd
}
