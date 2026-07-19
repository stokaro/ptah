// Package atlas exposes Atlas-compatible command paths.
package atlas

import (
	"fmt"

	"github.com/spf13/cobra"

	"github.com/stokaro/ptah/cmd/compare"
	"github.com/stokaro/ptah/cmd/dropall"
	"github.com/stokaro/ptah/cmd/internal/cmdalias"
	"github.com/stokaro/ptah/cmd/lint"
	"github.com/stokaro/ptah/cmd/migrate"
	"github.com/stokaro/ptah/cmd/migratedown"
	"github.com/stokaro/ptah/cmd/migratehash"
	"github.com/stokaro/ptah/cmd/migraterepair"
	"github.com/stokaro/ptah/cmd/migratestatus"
	"github.com/stokaro/ptah/cmd/migrateup"
	"github.com/stokaro/ptah/cmd/migratevalidate"
	"github.com/stokaro/ptah/cmd/readdb"
)

type atlasVerb struct {
	use        string
	short      string
	native     string
	factory    func() *cobra.Command
	prefixArgs []string
}

// NewAtlasCommand returns the Atlas compatibility namespace.
func NewAtlasCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "atlas",
		Short: "Atlas-compatible command namespace",
		Long: `Atlas-compatible command namespace.

These commands reserve the Atlas OSS CLI surface under Ptah. Commands that have
an existing Ptah equivalent forward to that native command while keeping the
native Ptah command tree separate for future redesign.`,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return cmd.Help()
		},
	}
	cmd.AddCommand(newAtlasVersionCommand())
	cmd.AddCommand(newAtlasLicenseCommand())
	cmd.AddCommand(newAtlasSchemaCommand())
	cmd.AddCommand(newAtlasMigrateCommand())
	return cmd
}

func newAtlasSchemaCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "schema",
		Short: "Atlas schema command compatibility",
		RunE: func(cmd *cobra.Command, _ []string) error {
			return cmd.Help()
		},
	}
	for _, verb := range []atlasVerb{
		{use: "inspect", short: "Inspect a database schema", native: "read-db", factory: readdb.NewReadDBCommand},
		{use: "apply", short: "Apply a desired schema to a database", native: ""},
		{use: "diff", short: "Diff desired schema against a database", native: "compare", factory: compare.NewCompareCommand},
		{use: "fmt", short: "Format schema files", native: ""},
		{use: "clean", short: "Clean database schema objects", native: "drop-all", factory: dropall.NewDropAllCommand},
	} {
		cmd.AddCommand(newAtlasAliasCommand("schema", verb))
	}
	return cmd
}

func newAtlasMigrateCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "migrate",
		Short: "Atlas migrate command compatibility",
		RunE: func(cmd *cobra.Command, _ []string) error {
			return cmd.Help()
		},
	}
	for _, verb := range []atlasVerb{
		{use: "apply", short: "Apply pending migrations", native: "migrate-up", factory: migrateup.NewMigrateUpCommand},
		{use: "diff", short: "Generate migration SQL from differences", native: "migrate", factory: migrate.NewMigrateCommand},
		{use: "down", short: "Roll back migrations", native: "migrate-down", factory: migratedown.NewMigrateDownCommand},
		{use: "hash", short: "Write or update the migration directory checksum", native: "migrate-hash", factory: migratehash.NewMigrateHashCommand},
		{use: "import", short: "Import migrations from another tool", native: ""},
		{use: "lint", short: "Lint migration files", native: "lint", factory: lint.NewLintCommand},
		{use: "new", short: "Create a new migration file", native: "migrate new", factory: migrate.NewMigrateCommand, prefixArgs: []string{"new"}},
		{use: "set", short: "Set migration revision state", native: "migrate-repair", factory: migraterepair.NewMigrateRepairCommand},
		{use: "status", short: "Show migration status", native: "migrate-status", factory: migratestatus.NewMigrateStatusCommand},
		{use: "validate", short: "Validate migration directory integrity", native: "migrate-validate", factory: migratevalidate.NewMigrateValidateCommand},
	} {
		cmd.AddCommand(newAtlasAliasCommand("migrate", verb))
	}
	return cmd
}

func newAtlasVersionCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "version",
		Short: "Print Ptah version information",
		RunE: func(cmd *cobra.Command, _ []string) error {
			return fmt.Errorf("atlas version compatibility is not implemented yet; use the native Ptah version command once issue #268 lands")
		},
	}
}

func newAtlasLicenseCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "license",
		Short: "Print license information",
		RunE: func(cmd *cobra.Command, _ []string) error {
			return fmt.Errorf("atlas license compatibility is not implemented yet")
		},
	}
}

func newAtlasAliasCommand(group string, verb atlasVerb) *cobra.Command {
	if verb.factory != nil {
		return cmdalias.NewForwardCommandWithArgs(
			verb.use,
			verb.short,
			verb.native,
			verb.factory,
			verb.prefixArgs...,
		)
	}
	cmd := &cobra.Command{
		Use:   verb.use,
		Short: verb.short,
		Long:  atlasAliasLong(group, verb),
		RunE: func(_ *cobra.Command, _ []string) error {
			if verb.native == "" {
				return fmt.Errorf("atlas %s %s compatibility is not implemented yet", group, verb.use)
			}
			return fmt.Errorf("atlas %s %s execution is not implemented yet; use `ptah %s`", group, verb.use, verb.native)
		},
	}
	return cmd
}

func atlasAliasLong(group string, verb atlasVerb) string {
	if verb.native == "" {
		return fmt.Sprintf("Atlas-compatible `atlas %s %s` command path. Runtime compatibility is not implemented yet.", group, verb.use)
	}
	return fmt.Sprintf("Atlas-compatible `atlas %s %s` command path. The current native Ptah equivalent is `ptah %s`.", group, verb.use, verb.native)
}
