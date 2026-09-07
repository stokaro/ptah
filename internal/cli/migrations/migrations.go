// Package migrations contains Ptah's native migration command group.
package migrations

import (
	"github.com/spf13/cobra"

	"ptah.run/internal/cli/internal/cmdutil"
	"ptah.run/internal/cli/lint"
	"ptah.run/internal/cli/migrate"
	"ptah.run/internal/cli/migratebaseline"
	"ptah.run/internal/cli/migratecheckpoint"
	"ptah.run/internal/cli/migratedata"
	"ptah.run/internal/cli/migratedown"
	"ptah.run/internal/cli/migrateedit"
	"ptah.run/internal/cli/migratehash"
	"ptah.run/internal/cli/migratels"
	"ptah.run/internal/cli/migraterebase"
	"ptah.run/internal/cli/migraterepair"
	"ptah.run/internal/cli/migraterm"
	"ptah.run/internal/cli/migrateset"
	"ptah.run/internal/cli/migrateshow"
	"ptah.run/internal/cli/migratestatus"
	"ptah.run/internal/cli/migratetag"
	"ptah.run/internal/cli/migrateup"
	"ptah.run/internal/cli/migratevalidate"
	"ptah.run/internal/cli/migrationsimport"
	"ptah.run/internal/cli/migrationspull"
	"ptah.run/internal/cli/migrationspush"
	"ptah.run/internal/cli/migrationstest"
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
	cmd.AddCommand(migrationCommand(migratels.NewMigrateLsCommand(), "List the migration files in a directory", "List the migration files a migration directory holds, without contacting a database."))
	cmd.AddCommand(migrationCommand(migrateshow.NewMigrateShowCommand(), "Print the SQL of one or more migrations", "Print the SQL a migration directory stores, without contacting a database."))
	cmd.AddCommand(migrationCommand(migratebaseline.NewMigrateBaselineCommand(), "Record existing migrations as applied", "Record existing migrations as already applied in the revision table."))
	cmd.AddCommand(migrationCommand(migratetag.NewMigrateTagCommand(), "Record, list, or remove a migration tag", "Record a tag naming the migration version a database has reached, list the tags recorded against it, or remove one."))
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

func migrationCommand(cmd *cobra.Command, short, long string) *cobra.Command {
	cmd.Short = short
	cmd.Long = long
	return cmd
}
