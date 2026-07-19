package migrate

import (
	"fmt"
	"strings"

	"github.com/spf13/cobra"

	"github.com/stokaro/ptah/internal/pathguard"
	"github.com/stokaro/ptah/migration/generator"
)

const (
	newMigrationsDirFlag = "migrations-dir"
	newNameFlag          = "name"
)

func newMigrateNewCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "new [name]",
		Short: "Create empty migration files for manual SQL",
		Long: `Create paired empty migration files for manual SQL authoring.

The command writes timestamped .up.sql and .down.sql files using Ptah's paired
migration naming convention.`,
		Args: cobra.MaximumNArgs(1),
		RunE: migrateNewCommand,
	}

	flags := cmd.Flags()
	flags.String(newMigrationsDirFlag, "", "Directory receiving generated migration files (required)")
	flags.String(newNameFlag, "", "Migration name; optional when [name] is provided")

	return cmd
}

func migrateNewCommand(cmd *cobra.Command, args []string) error {
	migrationsDir, err := cmd.Flags().GetString(newMigrationsDirFlag)
	if err != nil {
		return err
	}
	name, err := cmd.Flags().GetString(newNameFlag)
	if err != nil {
		return err
	}
	if len(args) > 0 {
		if strings.TrimSpace(name) != "" {
			return fmt.Errorf("migration name must be provided either as an argument or --name, not both")
		}
		name = args[0]
	}

	if strings.TrimSpace(name) == "" {
		return fmt.Errorf("migration name is required")
	}
	if strings.TrimSpace(migrationsDir) == "" {
		return fmt.Errorf("migrations directory is required")
	}
	migrationsDir, err = pathguard.ResolveCLIPath(migrationsDir)
	if err != nil {
		return fmt.Errorf("invalid migrations directory: %w", err)
	}

	files, err := generator.GenerateEmptyMigration(generator.EmptyMigrationOptions{
		MigrationName: name,
		OutputDir:     migrationsDir,
	})
	if err != nil {
		return err
	}

	out := cmd.OutOrStdout()
	fmt.Fprintf(out, "Generated empty migration files:\n")
	fmt.Fprintf(out, "UP:   %s\n", files.UpFile)
	fmt.Fprintf(out, "DOWN: %s\n", files.DownFile)
	return nil
}
