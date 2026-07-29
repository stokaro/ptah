// Package migrations contains Ptah's native migration command group.
package migrations

import (
	"github.com/spf13/cobra"

	"github.com/stokaro/ptah/cmd/internal/cmdutil"
	"github.com/stokaro/ptah/cmd/lint"
	"github.com/stokaro/ptah/cmd/migrate"
	"github.com/stokaro/ptah/cmd/migratebaseline"
	"github.com/stokaro/ptah/cmd/migratecheckpoint"
	"github.com/stokaro/ptah/cmd/migratedata"
	"github.com/stokaro/ptah/cmd/migratedown"
	"github.com/stokaro/ptah/cmd/migrateedit"
	"github.com/stokaro/ptah/cmd/migratehash"
	"github.com/stokaro/ptah/cmd/migraterebase"
	"github.com/stokaro/ptah/cmd/migraterepair"
	"github.com/stokaro/ptah/cmd/migraterm"
	"github.com/stokaro/ptah/cmd/migrateset"
	"github.com/stokaro/ptah/cmd/migratestatus"
	"github.com/stokaro/ptah/cmd/migrateup"
	"github.com/stokaro/ptah/cmd/migratevalidate"
	"github.com/stokaro/ptah/cmd/migrationsimport"
	"github.com/stokaro/ptah/cmd/migrationspull"
	"github.com/stokaro/ptah/cmd/migrationspush"
	"github.com/stokaro/ptah/cmd/migrationstest"
)

// NewMigrationsCommand returns the native migration command namespace.
func NewMigrationsCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "migrations",
		Short: "Manage migration plans, files, and revision state",
		Long: `Manage migration plans, files, and revision state.

This is Ptah's native migration namespace. It deliberately uses Ptah-owned
spellings such as "plan" and "up" instead of root-level Atlas-looking paths such as
"migrate diff" or "migrate apply". Atlas-compatible commands live in the
separate ptah-compat binary.`,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return cmd.Help()
		},
	}
	cmdutil.ConfigureCommandArgs(cmd, cmdutil.NoPositionalArgs)

	cmd.AddCommand(migrationCommand(migrate.NewMigrateCommand(), "Plan migration SQL from schema differences", "Plan migration SQL from schema differences without writing migration files."))
	cmd.AddCommand(migrationCommand(migrate.NewMigrateGenerateCommand(), "Generate migration files from schema differences", "Generate migration files from schema differences and write them to the migrations directory."))
	cmd.AddCommand(migrationCommand(migrate.NewMigrateCreateCommand(), "Create empty migration files for manual SQL", "Create empty migration files for manual SQL."))
	cmd.AddCommand(migrationCommand(
		migratedata.NewMigrateDataCommand(),
		"Generate a migration from reference/seed data drift",
		"Generate an ordinary migration from the drift between declarative reference/seed data "+
			"(//ptah:schema:data) and a live database. It applies no safety/risk gating of its own "+
			"(a deferred follow-up); review the generated file before applying.",
	))
	cmd.AddCommand(migrationCommand(migrationsimport.NewMigrationsImportCommand(), "Import migrations from another tool", "Convert a golang-migrate, Goose, Flyway, or Liquibase migration directory into Ptah's native format."))
	cmd.AddCommand(migrationCommand(migrationspush.NewMigrationsPushCommand(), "Push a migration directory to an OCI registry", "Push a migration directory to an OCI-compliant registry as an immutable artifact."))
	cmd.AddCommand(migrationCommand(migrationspull.NewMigrationsPullCommand(), "Pull a migration directory from an OCI registry", "Pull and reconstruct a migration directory from an OCI-compliant registry."))
	cmd.AddCommand(migrationCommand(migrateup.NewMigrateUpCommand(), "Run pending migrations", "Run pending migrations against a live database."))
	cmd.AddCommand(migrationCommand(migratedown.NewMigrateDownCommand(), "Roll back migrations", "Roll back migrations against a live database."))
	cmd.AddCommand(migrationCommand(migratestatus.NewMigrateStatusCommand(), "Show migration status", "Show migration status for a live database and migrations directory."))
	cmd.AddCommand(migrationCommand(migratebaseline.NewMigrateBaselineCommand(), "Record existing migrations as applied", "Record existing migrations as already applied in the revision table."))
	cmd.AddCommand(migrationCommand(migrateset.NewMigrateSetCommand(), "Set the revision boundary to a version", "Move the revision boundary to an arbitrary migration version in both directions without executing migration SQL."))
	cmd.AddCommand(migrationCommand(migratecheckpoint.NewMigrateCheckpointCommand(), "Squash history into a checkpoint", "Squash a migration directory's history into a cumulative-schema checkpoint that fresh databases bootstrap from."))
	cmd.AddCommand(migrationCommand(migraterepair.NewMigrateRepairCommand(), "Repair migration revision metadata", "Repair migration revision metadata after a dirty or partial migration state."))
	cmd.AddCommand(migrationCommand(migratehash.NewMigrateHashCommand(), "Write or update migration directory integrity", "Write or update the migration directory integrity file."))
	cmd.AddCommand(migrationCommand(migratevalidate.NewMigrateValidateCommand(), "Validate migration directory integrity", "Validate the migration directory against its integrity file."))
	cmd.AddCommand(migrationCommand(lint.NewLintCommand(), "Lint migration files", "Lint migration files for production-unsafe patterns."))
	cmd.AddCommand(migrationCommand(migrateedit.NewMigrateEditCommand(), "Edit a migration and re-hash", "Edit a migration's SQL and rewrite the integrity file, refusing already-applied migrations."))
	cmd.AddCommand(migrationCommand(migraterebase.NewMigrateRebaseCommand(), "Move a migration to the end of history", "Re-timestamp a migration to the end of history and rewrite the integrity file, refusing already-applied migrations."))
	cmd.AddCommand(migrationCommand(migraterm.NewMigrateRmCommand(), "Delete a migration and re-hash", "Delete a migration's up/down pair and rewrite the integrity file, refusing already-applied migrations."))
	cmd.AddCommand(migrationstest.NewMigrationsTestCommand())

	return cmd
}

func migrationCommand(cmd *cobra.Command, short string, long string) *cobra.Command {
	cmd.Short = short
	cmd.Long = long
	return cmd
}
