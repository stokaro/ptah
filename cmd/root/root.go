// Package root wires the Ptah command tree.
package root

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"

	"go.5x5.cz/ptah/cmd/db"
	"go.5x5.cz/ptah/cmd/internal/buildinfo"
	"go.5x5.cz/ptah/cmd/internal/cmdflags"
	"go.5x5.cz/ptah/cmd/internal/cmdutil"
	"go.5x5.cz/ptah/cmd/internal/exitcode"
	"go.5x5.cz/ptah/cmd/introspect"
	"go.5x5.cz/ptah/cmd/license"
	"go.5x5.cz/ptah/cmd/migrations"
	"go.5x5.cz/ptah/cmd/oci"
	"go.5x5.cz/ptah/cmd/schema"
	"go.5x5.cz/ptah/cmd/seed"
	sqlcmd "go.5x5.cz/ptah/cmd/sql"
	"go.5x5.cz/ptah/cmd/version"
	"go.5x5.cz/ptah/cmd/viz"
)

const envPrefix = "PTAH"

// NewRootCommand returns the root Ptah command with every subcommand registered.
func NewRootCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "ptah",
		Short:   "Ptah schema management and migration tooling",
		Long:    rootLongDescription,
		Version: buildinfo.Resolve().Version,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return cmd.Help()
		},
	}
	cmdutil.ConfigureCommandArgs(cmd, nil)

	cmd.AddCommand(introspect.NewIntrospectCommand())
	cmd.AddCommand(schema.NewSchemaCommand())
	cmd.AddCommand(db.NewDBCommand())
	cmd.AddCommand(migrations.NewMigrationsCommand())
	cmd.AddCommand(oci.NewCommand())
	cmd.AddCommand(seed.NewSeedCommand())
	cmd.AddCommand(sqlcmd.NewSQLCommand())
	cmd.AddCommand(viz.NewCommand())
	cmd.AddCommand(version.NewVersionCommand())
	cmd.AddCommand(license.NewLicenseCommand())

	cmdflags.InstallEnvBinding(envPrefix, cmd)

	return cmd
}

// Execute runs the root command and exits the process with the command's
// declared exit-code contract.
func Execute(args ...string) {
	ExecuteCommand(NewRootCommand(), args...)
}

// ExecuteCommand runs cmd and exits the process with Ptah's CLI exit-code
// contract.
func ExecuteCommand(cmd *cobra.Command, args ...string) {
	cmd.SetArgs(args)

	err := executeWithRecovery(cmd)
	if err != nil {
		os.Exit(exitcode.Code(err, 2)) //revive:disable-line:deep-exit
	}
}

func executeWithRecovery(cmd *cobra.Command) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = exitcode.New(2, fmt.Errorf("internal error: %v", recovered))
			fmt.Fprintf(cmd.ErrOrStderr(), "%s: %v\n", cmdutil.ErrorPrefix(cmd), err)
		}
	}()

	executed, err := cmd.ExecuteC()
	if executed == nil {
		executed = cmd
	}
	return cmdutil.NormalizeCommandError(executed, err, 2)
}

const rootLongDescription = `Ptah generates database schemas from Go entities,
compares desired schemas with live databases, and manages database migrations.

It supports PostgreSQL-family targets, MySQL, MariaDB, SQLite, ClickHouse, and
Spanner-oriented schema workflows. Scripts that expect the Atlas CLI can use
the separate ptah-compat binary, a drop-in Atlas replacement.`
