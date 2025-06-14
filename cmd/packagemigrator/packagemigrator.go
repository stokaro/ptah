package packagemigrator

import (
	"os"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"github.com/stokaro/ptah/cmd/compare"
	"github.com/stokaro/ptah/cmd/dropall"
	"github.com/stokaro/ptah/cmd/generate"
	"github.com/stokaro/ptah/cmd/migrate"
	"github.com/stokaro/ptah/cmd/migratedown"
	"github.com/stokaro/ptah/cmd/migratestatus"
	"github.com/stokaro/ptah/cmd/migrateup"
	"github.com/stokaro/ptah/cmd/readdb"
)

const (
	envPrefix = "PACKAGE_MIGRATOR"
)

var rootCmd = &cobra.Command{
	Use:   "package-migrator",
	Short: "Package-wide schema generator with dependency-ordered table creation",
	Long: `Package-migrator is a tool for generating database schemas from Go entities,
comparing schemas, and managing database migrations.

It supports multiple database dialects (PostgreSQL, MySQL, MariaDB) and provides
commands for schema generation, database operations, and migration management.`,
	Args: cobra.NoArgs, // Disallow unknown subcommands
	RunE: func(cmd *cobra.Command, _ []string) error {
		return cmd.Help()
	},
}

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute(args ...string) {
	viper.AutomaticEnv()
	viper.SetEnvPrefix(envPrefix)

	rootCmd.SetArgs(args)
	rootCmd.AddCommand(generate.NewGenerateCommand())
	rootCmd.AddCommand(readdb.NewReadDBCommand())
	rootCmd.AddCommand(compare.NewCompareCommand())
	rootCmd.AddCommand(migrate.NewMigrateCommand())
	rootCmd.AddCommand(migrateup.NewMigrateUpCommand())
	rootCmd.AddCommand(migratedown.NewMigrateDownCommand())
	rootCmd.AddCommand(migratestatus.NewMigrateStatusCommand())
	rootCmd.AddCommand(dropall.NewDropAllCommand())

	err := rootCmd.Execute()
	if err != nil {
		os.Exit(1) //revive:disable-line:deep-exit
	}
}
