// Package migrations contains Ptah's native migration command group.
package migrations

import (
	"github.com/spf13/cobra"

	"github.com/stokaro/ptah/cmd/internal/cmdutil"
	"github.com/stokaro/ptah/cmd/lint"
	"github.com/stokaro/ptah/cmd/migrate"
	"github.com/stokaro/ptah/cmd/migratebaseline"
	"github.com/stokaro/ptah/cmd/migratedown"
	"github.com/stokaro/ptah/cmd/migratehash"
	"github.com/stokaro/ptah/cmd/migraterepair"
	"github.com/stokaro/ptah/cmd/migratestatus"
	"github.com/stokaro/ptah/cmd/migrateup"
	"github.com/stokaro/ptah/cmd/migratevalidate"
)

// NewMigrationsCommand returns the native migration command namespace.
func NewMigrationsCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "migrations",
		Short: "Manage migration plans, files, and revision state",
		Long: `Manage migration plans, files, and revision state.

This is Ptah's native migration namespace. It deliberately uses Ptah-owned
spellings such as "plan" and "up" instead of root-level Atlas aliases such as
"migrate diff" or "migrate apply". Atlas-compatible commands stay under
ptah atlas.`,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return cmd.Help()
		},
	}
	cmdutil.ConfigureCommandArgs(cmd, cmdutil.NoPositionalArgs)

	cmd.AddCommand(migrationCommand(migrate.NewMigrateCommand(), "Plan migration SQL from schema differences", "Plan migration SQL from schema differences without writing migration files."))
	cmd.AddCommand(migrationCommand(migrate.NewMigrateGenerateCommand(), "Generate migration files from schema differences", "Generate migration files from schema differences and write them to the migrations directory."))
	cmd.AddCommand(migrationCommand(migrate.NewMigrateCreateCommand(), "Create empty migration files for manual SQL", "Create empty migration files for manual SQL."))
	cmd.AddCommand(migrationCommand(migrateup.NewMigrateUpCommand(), "Run pending migrations", "Run pending migrations against a live database."))
	cmd.AddCommand(migrationCommand(migratedown.NewMigrateDownCommand(), "Roll back migrations", "Roll back migrations against a live database."))
	cmd.AddCommand(migrationCommand(migratestatus.NewMigrateStatusCommand(), "Show migration status", "Show migration status for a live database and migrations directory."))
	cmd.AddCommand(migrationCommand(migratebaseline.NewMigrateBaselineCommand(), "Record existing migrations as applied", "Record existing migrations as already applied in the revision table."))
	cmd.AddCommand(migrationCommand(migraterepair.NewMigrateRepairCommand(), "Repair migration revision metadata", "Repair migration revision metadata after a dirty or partial migration state."))
	cmd.AddCommand(migrationCommand(migratehash.NewMigrateHashCommand(), "Write or update migration directory integrity", "Write or update the migration directory integrity file."))
	cmd.AddCommand(migrationCommand(migratevalidate.NewMigrateValidateCommand(), "Validate migration directory integrity", "Validate the migration directory against its integrity file."))
	cmd.AddCommand(migrationCommand(lint.NewLintCommand(), "Lint migration files", "Lint migration files for production-unsafe patterns."))

	return cmd
}

func migrationCommand(cmd *cobra.Command, short string, long string) *cobra.Command {
	cmd.Short = short
	cmd.Long = long
	return cmd
}
