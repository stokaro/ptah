package migrate

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/spf13/cobra"

	"go.5x5.cz/ptah/cmd/internal/cmdutil"
	"go.5x5.cz/ptah/cmd/internal/editor"
	"go.5x5.cz/ptah/internal/migrateops"
	"go.5x5.cz/ptah/internal/pathguard"
	"go.5x5.cz/ptah/migration/generator"
	"go.5x5.cz/ptah/migration/migrator"
)

const (
	newMigrationsDirFlag = "migrations-dir"
	newDirFormatFlag     = "dir-format"
	newNameFlag          = "name"
	newEditFlag          = "edit"
	newEditorFlag        = "editor"
)

func NewMigrateCreateCommand() *cobra.Command {
	var dirFormat string
	cmd := &cobra.Command{
		Use:   "create [name]",
		Short: "Create empty migration files for manual SQL",
		Long: `Create empty migration files for manual SQL authoring.

The command writes timestamped .up.sql and .down.sql files by default using
Ptah's paired migration naming convention. With --dir-format atlas it writes a
single Atlas-style .sql file and updates atlas.sum. With --edit it opens the
created files in $VISUAL, $EDITOR, or --editor before reporting them, and
refreshes atlas.sum afterwards for Atlas-format directories.`,
		Args: cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return migrateNewCommand(cmd, args, dirFormat)
		},
	}

	flags := cmd.Flags()
	flags.String(newMigrationsDirFlag, "", "Directory receiving generated migration files (required)")
	flags.StringVar(&dirFormat, newDirFormatFlag, string(migrator.MigrationDirFormatAuto), "Migration directory format: auto, ptah, or atlas")
	flags.String(newNameFlag, "", "Migration name; optional when [name] is provided")
	flags.Bool(newEditFlag, false, "Open the created migration files in an editor (atlas.sum is refreshed for Atlas-format directories)")
	flags.String(newEditorFlag, "", "Editor command used with --edit (defaults to $VISUAL, then $EDITOR)")

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
	edit, err := cmd.Flags().GetBool(newEditFlag)
	if err != nil {
		return err
	}
	editorCmd, err := cmd.Flags().GetString(newEditorFlag)
	if err != nil {
		return err
	}
	if editorCmd != "" && !edit {
		return fmt.Errorf("--editor requires --edit")
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
	pair := files.Files[0]
	if edit {
		if err := editCreatedMigration(cmd.Context(), pair, editorCmd, migrationsDir, dirFormat); err != nil {
			return err
		}
	}

	out := cmd.OutOrStdout()
	if pair.DownFile == "" {
		fmt.Fprintf(out, "Generated empty migration file:\n")
		fmt.Fprintf(out, "SQL:  %s\n", pair.UpFile)
		return nil
	}
	fmt.Fprintf(out, "Generated empty migration files:\n")
	fmt.Fprintf(out, "UP:   %s\n", pair.UpFile)
	fmt.Fprintf(out, "DOWN: %s\n", pair.DownFile)
	return nil
}

// editCreatedMigration opens the just-created files in the resolved editor.
// Editing changes content the Atlas-format create path has already hashed into
// atlas.sum, so for Atlas-format directories the checksum is refreshed
// afterwards; ptah-format create does not maintain a checksum file, so there is
// nothing to refresh.
func editCreatedMigration(
	ctx context.Context,
	pair generator.MigrationFilePair,
	editorCmd string,
	migrationsDir string,
	dirFormat migrator.MigrationDirFormat,
) error {
	err := editor.Open(ctx, editorCmd, pair.UpFile, pair.DownFile)
	if errors.Is(err, editor.ErrNoEditor) {
		return fmt.Errorf("%w, or pass --editor", err)
	}
	if err != nil {
		return err
	}
	if dirFormat != migrator.MigrationDirFormatAtlas {
		return nil
	}
	if _, err := migrateops.Rehash(migrationsDir, dirFormat); err != nil {
		return fmt.Errorf("refresh atlas.sum after editing: %w", err)
	}
	return nil
}
