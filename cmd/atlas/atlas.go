// Package atlas exposes Atlas-compatible command paths.
package atlas

import (
	"fmt"

	"github.com/spf13/cobra"
)

type atlasVerb struct {
	use    string
	short  string
	native string
}

// NewAtlasCommand returns the Atlas compatibility namespace.
func NewAtlasCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "atlas",
		Short: "Atlas-compatible command namespace",
		Long: `Atlas-compatible command namespace.

These commands reserve the Atlas OSS CLI surface under Ptah. Commands that have
an existing Ptah equivalent point to that native command. Runtime-compatible
flag translation and Atlas revision-table compatibility are tracked separately.`,
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
		{use: "inspect", short: "Inspect a database schema", native: "read-db"},
		{use: "apply", short: "Apply a desired schema to a database", native: ""},
		{use: "diff", short: "Diff desired schema against a database", native: "compare"},
		{use: "fmt", short: "Format schema files", native: ""},
		{use: "clean", short: "Clean database schema objects", native: "drop-all"},
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
		{use: "apply", short: "Apply pending migrations", native: "migrate-up"},
		{use: "diff", short: "Generate migration SQL from differences", native: "migrate"},
		{use: "hash", short: "Write or update the migration directory checksum", native: "migrate-hash"},
		{use: "import", short: "Import migrations from another tool", native: ""},
		{use: "lint", short: "Lint migration files", native: "lint"},
		{use: "new", short: "Create a new migration file", native: "migrate generate"},
		{use: "set", short: "Set migration revision state", native: "migrate-repair"},
		{use: "status", short: "Show migration status", native: "migrate-status"},
		{use: "validate", short: "Validate migration directory integrity", native: "migrate-validate"},
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
