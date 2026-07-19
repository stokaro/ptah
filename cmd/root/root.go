// Package root wires the Ptah command tree.
package root

import (
	"fmt"
	"os"

	"github.com/go-extras/cobraflags"
	"github.com/spf13/cobra"

	"github.com/stokaro/ptah/cmd/atlas"
	"github.com/stokaro/ptah/cmd/compare"
	"github.com/stokaro/ptah/cmd/drift"
	"github.com/stokaro/ptah/cmd/dropall"
	"github.com/stokaro/ptah/cmd/generate"
	"github.com/stokaro/ptah/cmd/internal/buildinfo"
	"github.com/stokaro/ptah/cmd/internal/exitcode"
	"github.com/stokaro/ptah/cmd/lint"
	"github.com/stokaro/ptah/cmd/migrate"
	"github.com/stokaro/ptah/cmd/migratebaseline"
	"github.com/stokaro/ptah/cmd/migratedown"
	"github.com/stokaro/ptah/cmd/migratehash"
	"github.com/stokaro/ptah/cmd/migraterepair"
	"github.com/stokaro/ptah/cmd/migratestatus"
	"github.com/stokaro/ptah/cmd/migrateup"
	"github.com/stokaro/ptah/cmd/migratevalidate"
	"github.com/stokaro/ptah/cmd/readdb"
	"github.com/stokaro/ptah/cmd/schema"
	"github.com/stokaro/ptah/cmd/seed"
	"github.com/stokaro/ptah/cmd/version"
)

const envPrefix = "PTAH"

// NewRootCommand returns the root Ptah command with every subcommand registered.
func NewRootCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "ptah",
		Short:   "Ptah schema management and migration tooling",
		Long:    rootLongDescription,
		Version: buildinfo.Resolve().Version,
		Args:    cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return cmd.Help()
		},
	}

	cmd.AddCommand(generate.NewGenerateCommand())
	cmd.AddCommand(readdb.NewReadDBCommand())
	cmd.AddCommand(schema.NewSchemaCommand())
	cmd.AddCommand(compare.NewCompareCommand())
	cmd.AddCommand(drift.NewDriftCommand())
	cmd.AddCommand(migrate.NewMigrateCommand())
	cmd.AddCommand(migratebaseline.NewMigrateBaselineCommand())
	cmd.AddCommand(migrateup.NewMigrateUpCommand())
	cmd.AddCommand(migratedown.NewMigrateDownCommand())
	cmd.AddCommand(migraterepair.NewMigrateRepairCommand())
	cmd.AddCommand(migratestatus.NewMigrateStatusCommand())
	cmd.AddCommand(migratehash.NewMigrateHashCommand())
	cmd.AddCommand(migratevalidate.NewMigrateValidateCommand())
	cmd.AddCommand(seed.NewSeedCommand())
	cmd.AddCommand(dropall.NewDropAllCommand())
	cmd.AddCommand(lint.NewLintCommand())
	cmd.AddCommand(atlas.NewAtlasCommand())
	cmd.AddCommand(version.NewVersionCommand())

	cobraflags.CobraOnInitialize(envPrefix, cmd)

	return cmd
}

// Execute runs the root command and exits the process with the command's
// declared exit-code contract.
func Execute(args ...string) {
	cmd := NewRootCommand()
	cmd.SetArgs(args)

	err := executeWithRecovery(cmd)
	if err != nil {
		os.Exit(exitcode.Code(err, 1)) //revive:disable-line:deep-exit
	}
}

func executeWithRecovery(cmd *cobra.Command) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("internal error: %v", recovered)
			fmt.Fprintf(cmd.ErrOrStderr(), "error: %v\n", err)
		}
	}()

	return cmd.Execute()
}

const rootLongDescription = `Ptah generates database schemas from Go entities,
compares desired schemas with live databases, and manages database migrations.

It supports PostgreSQL, MySQL, MariaDB, ClickHouse, CockroachDB, YugabyteDB,
and Spanner-oriented schema workflows, with Atlas-compatible commands grouped
under ptah atlas.`
