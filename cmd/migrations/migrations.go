// Package migrations contains Ptah's native migration command group.
package migrations

import (
	"github.com/spf13/cobra"

	"github.com/stokaro/ptah/cmd/internal/cmdalias"
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

	for _, alias := range []struct {
		use        string
		short      string
		long       string
		native     string
		factory    func() *cobra.Command
		prefixArgs []string
	}{
		{
			use:     "plan",
			short:   "Plan migration SQL from schema differences",
			long:    "Plan migration SQL from schema differences without writing migration files.",
			native:  "migrate",
			factory: migrate.NewMigrateCommand,
		},
		{
			use:        "generate",
			short:      "Generate migration files from schema differences",
			long:       "Generate migration files from schema differences and write them to the migrations directory.",
			native:     "migrate generate",
			factory:    migrate.NewMigrateCommand,
			prefixArgs: []string{"generate"},
		},
		{
			use:        "create",
			short:      "Create empty migration files for manual SQL",
			long:       "Create empty migration files for manual SQL.",
			native:     "migrate new",
			factory:    migrate.NewMigrateCommand,
			prefixArgs: []string{"new"},
		},
		{use: "up", short: "Run pending migrations", long: "Run pending migrations against a live database.", native: "migrations up", factory: migrateup.NewMigrateUpCommand},
		{use: "down", short: "Roll back migrations", long: "Roll back migrations against a live database.", native: "migrations down", factory: migratedown.NewMigrateDownCommand},
		{use: "status", short: "Show migration status", long: "Show migration status for a live database and migrations directory.", native: "migrations status", factory: migratestatus.NewMigrateStatusCommand},
		{use: "baseline", short: "Record existing migrations as applied", long: "Record existing migrations as already applied in the revision table.", native: "migrations baseline", factory: migratebaseline.NewMigrateBaselineCommand},
		{use: "repair", short: "Repair migration revision metadata", long: "Repair migration revision metadata after a dirty or partial migration state.", native: "migrations repair", factory: migraterepair.NewMigrateRepairCommand},
		{use: "hash", short: "Write or update migration directory integrity", long: "Write or update the migration directory integrity file.", native: "migrations hash", factory: migratehash.NewMigrateHashCommand},
		{use: "validate", short: "Validate migration directory integrity", long: "Validate the migration directory against its integrity file.", native: "migrations validate", factory: migratevalidate.NewMigrateValidateCommand},
		{use: "lint", short: "Lint migration files", long: "Lint migration files for production-unsafe patterns.", native: "lint", factory: lint.NewLintCommand},
	} {
		aliasCmd := cmdalias.NewForwardCommandWithTargetHelp(
			alias.use,
			alias.short,
			alias.native,
			alias.factory,
			alias.prefixArgs...,
		)
		aliasCmd.Long = alias.long
		cmd.AddCommand(aliasCmd)
	}

	return cmd
}
