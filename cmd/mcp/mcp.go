// Package mcp implements "ptah mcp", the Model Context Protocol server that
// exposes Ptah's operations to external AI clients.
package mcp

import (
	"github.com/spf13/cobra"

	"go.5x5.cz/ptah/cmd/internal/agentflags"
	"go.5x5.cz/ptah/cmd/internal/cmdutil"
)

// NewCommand returns the MCP server command.
func NewCommand() *cobra.Command {
	var opts *agentflags.Options
	cmd := &cobra.Command{
		Use:   "mcp",
		Short: "Serve Ptah's operations over the Model Context Protocol",
		Long: `Serve Ptah's operations to an AI client over the Model Context Protocol, on
stdin and stdout.

This is not a command to run by hand: an MCP client starts it and speaks the
protocol to it. Point one at the Ptah binary with "mcp" as the argument.

Without --workspace it serves four reading tools and nothing else. None of them
applies a migration, writes a file, or changes a database:

  validate_schema   structural problems in a declared schema, no database
  render_schema     the DDL a declared schema becomes, in dependency order
  schema_lineage    which base columns feed each view column
  read_database     the schema a live database currently holds

Three of Ptah's own reading verbs are deliberately absent -- schema inspect,
schema diff and migrations lint -- because each needs a scratch database it
resets destructively, and a destructive capability must not sit behind a
read-only name on a surface an agent drives.

With --workspace it also serves four artifact tools, confined to the directories
you name:

  describe_workspace  what this session may read and write, with digests
  read_artifact       one artifact directory, or one file inside it
  preview_patch       what a proposed change would do; writes nothing
  apply_patch         apply a previewed patch, verify it, undo a break

Writing is off until you turn it on. --allow-write names the artifact classes an
agent may propose changes to, and without it every apply is refused. A named
class still asks for approval per patch through the client, unless
--auto-approve says otherwise -- which is what a client that cannot show a
prompt needs.

The gates run for one target. --dialect names it, and --server-version pins the
release within that dialect so a rule gated on a capability the family gained
later answers for the server this project runs.

A patch cannot leave the directory its class names, cannot write the migration
integrity file, and cannot be applied if the directory changed after it was
previewed. Ptah recomputes the integrity file itself and runs its validation and
lint gates after every write, undoing the whole patch when the write introduced
an error.

Applying a migration to a database is not available here at any setting.

Credentials are the client's to supply. This server holds none, stores none, and
sends nothing anywhere: it runs locally and talks to whatever the caller names.`,
		Args:          cmdutil.NoPositionalArgs,
		SilenceErrors: true,
		SilenceUsage:  true,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return run(cmd, opts)
		},
	}
	opts = agentflags.Register(cmd)
	cmdutil.ConfigureCommandArgs(cmd, cmdutil.NoPositionalArgs)
	return cmd
}
