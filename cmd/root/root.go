// Package root wires the Ptah command tree.
package root

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"

	"github.com/stokaro/ptah/cmd/atlas"
	"github.com/stokaro/ptah/cmd/db"
	"github.com/stokaro/ptah/cmd/internal/buildinfo"
	"github.com/stokaro/ptah/cmd/internal/cmdflags"
	"github.com/stokaro/ptah/cmd/internal/cmdutil"
	"github.com/stokaro/ptah/cmd/internal/exitcode"
	"github.com/stokaro/ptah/cmd/introspect"
	"github.com/stokaro/ptah/cmd/migrations"
	"github.com/stokaro/ptah/cmd/schema"
	"github.com/stokaro/ptah/cmd/seed"
	sqlcmd "github.com/stokaro/ptah/cmd/sql"
	"github.com/stokaro/ptah/cmd/version"
	"github.com/stokaro/ptah/cmd/viz"
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
	cmd.AddCommand(seed.NewSeedCommand())
	cmd.AddCommand(sqlcmd.NewSQLCommand())
	cmd.AddCommand(viz.NewCommand())
	cmd.AddCommand(atlas.NewAtlasCommand())
	cmd.AddCommand(version.NewVersionCommand())

	cmdflags.InstallEnvBinding(envPrefix, cmd)

	return cmd
}

// Execute runs the root command and exits the process with the command's
// declared exit-code contract.
func Execute(args ...string) {
	cmd := NewRootCommand()
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
			fmt.Fprintf(cmd.ErrOrStderr(), "error: %v\n", err)
		}
	}()

	err = cmd.Execute()
	if err == nil || exitcode.Code(err, -1) != -1 {
		return err
	}
	fmt.Fprintf(cmd.ErrOrStderr(), "error: %v\n", err)
	return exitcode.New(2, err)
}

const rootLongDescription = `Ptah generates database schemas from Go entities,
compares desired schemas with live databases, and manages database migrations.

It supports PostgreSQL-family targets, MySQL, MariaDB, SQLite, ClickHouse, and
Spanner-oriented schema workflows, with Atlas-compatible commands grouped
under ptah atlas.`
