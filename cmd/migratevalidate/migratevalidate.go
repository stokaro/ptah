// Package migratevalidate implements the migration validation command: it
// verifies a migrations directory against its committed ptah.sum and exits
// non-zero on any drift (#161).
package migratevalidate

import (
	"errors"
	"fmt"

	"github.com/spf13/cobra"

	"github.com/stokaro/ptah/cmd/internal/cmdutil"
	"github.com/stokaro/ptah/cmd/internal/exitcode"
	"github.com/stokaro/ptah/internal/migrationvalidate"
	"github.com/stokaro/ptah/migration/migrator"
)

// NewMigrateValidateCommand returns the migration validation command.
func NewMigrateValidateCommand() *cobra.Command {
	var dir string
	var dirFormatValue string
	var devURL string

	cmd := &cobra.Command{
		Use:   "validate",
		Short: "Verify a migrations directory against its committed ptah.sum",
		Long: `migrations validate recomputes the integrity hashes of a migrations directory
and compares them against the committed ptah.sum. It exits:

  0  the directory matches ptah.sum
  1  a migration file was added, removed, or edited out of band (drift)
  2  ptah.sum is missing or unreadable, or the directory is inaccessible

Run it in CI to guarantee already-committed migrations are never changed.`,
		Args:          cmdutil.NoPositionalArgs,
		SilenceUsage:  true,
		SilenceErrors: true,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runValidate(cmd, dir, dirFormatValue, devURL)
		},
	}
	cmd.Flags().StringVar(&dir, "dir", "./migrations", "Directory containing migration files")
	cmd.Flags().StringVar(&dirFormatValue, "dir-format", string(migrator.MigrationDirFormatAuto), "Migration directory format: auto, ptah, or atlas")
	cmd.Flags().StringVar(&devURL, "dev-url", "", "Dev database URL used to clean and replay migrations for SQL validation")
	cmd.SetFlagErrorFunc(cmdutil.FlagErrorFunc)
	return cmd
}

func runValidate(cmd *cobra.Command, dir, dirFormatValue, devURL string) error {
	if err := cmdutil.StatDir(dir); err != nil {
		return cmdutil.Fail(cmd, err)
	}

	dirFormat, err := migrator.ParseMigrationDirFormat(dirFormatValue)
	if err != nil {
		return cmdutil.Fail(cmd, err)
	}

	// A missing or unreadable sum file, and any other verify error, is a
	// usage failure (exit 2) distinct from a content drift (exit 1). Its
	// message - including the actionable "run ptah migrations hash" for a
	// missing sum — must reach the user, so print it here (the command
	// silences cobra's own error output).
	result, err := migrationvalidate.Validate(cmd.Context(), migrationvalidate.Options{
		Dir:       dir,
		DirFormat: dirFormat,
		DevURL:    devURL,
	})
	if err != nil {
		return cmdutil.Fail(cmd, err)
	}

	if !result.Integrity.OK() {
		fmt.Fprintln(cmd.ErrOrStderr(), result.Integrity.Describe())
		return exitcode.New(1, errors.New("migration directory integrity check failed"))
	}

	out := cmd.OutOrStdout()
	fmt.Fprintf(out, "OK: migrations directory matches %s\n", result.Integrity.SumFileName)
	if result.DevSQLValidated {
		fmt.Fprintln(out, "OK: migration SQL validated on dev database")
	}
	return nil
}
