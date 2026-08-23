// Package mcp implements "ptah mcp", the Model Context Protocol server that
// exposes Ptah's operations to external AI clients.
package mcp

import (
	"github.com/spf13/cobra"

	"go.5x5.cz/ptah/cmd/internal/cmdflags"
	"go.5x5.cz/ptah/cmd/internal/cmdutil"
	"go.5x5.cz/ptah/cmd/internal/serverversion"
)

// Flag names, in one place because the help text, the refusals and the
// documentation all quote them.
const (
	workspaceFlag     = "workspace"
	migrationsDirFlag = "migrations-dir"
	schemaDirFlag     = "schema-dir"
	testsDirFlag      = "tests-dir"
	dialectFlag       = "dialect"
	allowWriteFlag    = "allow-write"
	autoApproveFlag   = "auto-approve"
	auditLogFlag      = "audit-log"
)

// NewCommand returns the MCP server command.
func NewCommand() *cobra.Command {
	opts := &options{}
	cmd := &cobra.Command{
		Use:   "mcp",
		Short: "Serve Ptah's operations over the Model Context Protocol",
		Long: `Serve Ptah's operations to an AI client over the Model Context Protocol, on
stdin and stdout.

This is not a command to run by hand: an MCP client starts it and speaks the
protocol to it. Point one at the Ptah binary with "mcp" as the argument.

Without --workspace it serves four reading tools and nothing else. None of them
applies a migration, writes a file, or changes a database:

  ptah_validate_schema   structural problems in a declared schema, no database
  ptah_render_schema     the DDL a declared schema becomes, in dependency order
  ptah_schema_lineage    which base columns feed each view column
  ptah_read_database     the schema a live database currently holds

Three of Ptah's own reading verbs are deliberately absent -- schema inspect,
schema diff and migrations lint -- because each needs a scratch database it
resets destructively, and a destructive capability must not sit behind a
read-only name on a surface an agent drives.

With --workspace it also serves four artifact tools, confined to the directories
you name:

  ptah_describe_workspace  what this session may read and write, with digests
  ptah_read_artifact       one artifact directory, or one file inside it
  ptah_preview_patch       what a proposed change would do; writes nothing
  ptah_apply_patch         apply a previewed patch, verify it, undo a break

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
	registerFlags(cmd, opts)
	cmdutil.ConfigureCommandArgs(cmd, cmdutil.NoPositionalArgs)
	return cmd
}

// registerFlags declares what the operator decides at startup.
//
// Everything an agent may reach is here rather than in a tool argument, and
// that is the design rather than a convenience: ADR 0003 records that the model
// chooses the arguments, so a server taking its root or its permissions from a
// tool call would let the untrusted party choose its own confinement.
func registerFlags(cmd *cobra.Command, opts *options) {
	flags := cmd.Flags()
	flags.StringVar(&opts.workspace, workspaceFlag, "",
		"Project root the artifact tools work within; without it only the reading tools are served")
	flags.StringVar(&opts.migrationsDir, migrationsDirFlag, "",
		"Migration directory, inside the workspace")
	flags.StringVar(&opts.schemaDir, schemaDirFlag, "",
		"Declared-schema directory, inside the workspace")
	flags.StringVar(&opts.testsDir, testsDirFlag, "",
		"Ptah test directory, inside the workspace")
	flags.StringVar(&opts.dialect, dialectFlag, "",
		"Target dialect the validation and lint gates run for; required with --workspace")
	serverversion.Register(flags, &opts.serverVersion)
	flags.StringSliceVar(&opts.allowWrite, allowWriteFlag, nil,
		"Artifact classes an agent may propose writes to: migrations, schema, tests")
	flags.BoolVar(&opts.autoApprove, autoApproveFlag, false,
		"Apply patches without asking for approval through the client")
	flags.StringVar(&opts.auditLog, auditLogFlag, "",
		"Where to append the agent audit record (default <workspace>/.ptah/agent-audit.jsonl)")

	// --auto-approve carries no environment binding, for the reason
	// `db drop-all --auto-approve` carries none: a variable exported once in a
	// shell profile is not a decision somebody made about this session.
	_ = cmdflags.DisableEnvBinding(flags, autoApproveFlag)
}
