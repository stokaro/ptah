package migrate

import (
	"fmt"
	"strings"

	"github.com/spf13/cobra"

	"github.com/stokaro/ptah/cmd/internal/cmdutil"
	"github.com/stokaro/ptah/internal/pathguard"
	"github.com/stokaro/ptah/migration/generator"
	"github.com/stokaro/ptah/migration/migrator"
)

const (
	newMigrationsDirFlag = "migrations-dir"
	newDirFormatFlag     = "dir-format"
	newNameFlag          = "name"
)

func NewMigrateCreateCommand() *cobra.Command {
	var dirFormat string
	cmd := &cobra.Command{
		Use:   "create [name]",
		Short: "Create empty migration files for manual SQL",
		Long: `Create empty migration files for manual SQL authoring.

The command writes timestamped .up.sql and .down.sql files by default using
Ptah's paired migration naming convention. With --dir-format atlas it writes a
single Atlas-style .sql file and updates atlas.sum.`,
		Args: cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return migrateNewCommand(cmd, args, dirFormat)
		},
	}

	flags := cmd.Flags()
	flags.String(newMigrationsDirFlag, "", "Directory receiving generated migration files (required)")
	flags.StringVar(&dirFormat, newDirFormatFlag, string(migrator.MigrationDirFormatAuto), "Migration directory format: auto, ptah, or atlas")
	flags.String(newNameFlag, "", "Migration name; optional when [name] is provided")

	cmdutil.ConfigureCommandArgs(cmd, cobra.MaximumNArgs(1))
	return cmd
}

func migrateNewCommand(cmd *cobra.Command, args []string, dirFormatValue string) error {
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

	if strings.TrimSpace(migrationsDir) == "" {
		return fmt.Errorf("migrations directory is required")
	}
	dirFormat, err := migrator.ParseMigrationDirFormat(dirFormatValue)
	if err != nil {
		return err
	}
	if strings.TrimSpace(name) == "" && dirFormat != migrator.MigrationDirFormatAtlas {
		return fmt.Errorf("migration name is required")
	}
	migrationsDir, err = pathguard.ResolveCLIPath(migrationsDir)
	if err != nil {
		return fmt.Errorf("invalid migrations directory: %w", err)
	}

	files, err := generator.GenerateEmptyMigration(generator.EmptyMigrationOptions{
		MigrationName: name,
		OutputDir:     migrationsDir,
		DirFormat:     dirFormat,
	})
	if err != nil {
		return err
	}

	out := cmd.OutOrStdout()
	if files.DownFile == "" {
		fmt.Fprintf(out, "Generated empty migration file:\n")
		fmt.Fprintf(out, "SQL:  %s\n", files.UpFile)
		return nil
	}
	fmt.Fprintf(out, "Generated empty migration files:\n")
	fmt.Fprintf(out, "UP:   %s\n", files.UpFile)
	fmt.Fprintf(out, "DOWN: %s\n", files.DownFile)
	return nil
}
