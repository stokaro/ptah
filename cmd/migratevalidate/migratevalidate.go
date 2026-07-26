// Package migratevalidate implements the migration validation command: it
// verifies a migrations directory against its committed ptah.sum and exits
// non-zero on any drift (#161).
package migratevalidate

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/spf13/cobra"

	"github.com/stokaro/ptah/cmd/internal/cmdutil"
	"github.com/stokaro/ptah/cmd/internal/exitcode"
	"github.com/stokaro/ptah/internal/migratesum"
	"github.com/stokaro/ptah/internal/migrationvalidate"
	"github.com/stokaro/ptah/migration/migrator"
)

const (
	atlasChecksumHeader = "You have a checksum error in your migration directory.\n"
	atlasChecksumFooter = "Please check your migration files and run 'atlas migrate hash' to re-hash the contents\n\n"
)

var errAtlasChecksumMismatch = errors.New("checksum mismatch")

// NewMigrateValidateCommand returns the migration validation command.
func NewMigrateValidateCommand() *cobra.Command {
	return newMigrateValidateCommand(runNativeValidate)
}

// NewAtlasMigrateValidateCommand returns migration validation with Atlas CE
// checksum-mismatch output semantics.
func NewAtlasMigrateValidateCommand() *cobra.Command {
	return newMigrateValidateCommand(runAtlasValidate)
}

type validateRunner func(*cobra.Command, string, string, string) error

func newMigrateValidateCommand(run validateRunner) *cobra.Command {
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
			return run(cmd, dir, dirFormatValue, devURL)
		},
	}
	cmd.Flags().StringVar(&dir, "dir", "./migrations", "Directory containing migration files")
	cmd.Flags().StringVar(&dirFormatValue, "dir-format", string(migrator.MigrationDirFormatAuto), "Migration directory format: auto, ptah, or atlas")
	cmd.Flags().StringVar(&devURL, "dev-url", "", "Dev database URL used to clean and replay migrations for SQL validation")
	cmd.SetFlagErrorFunc(cmdutil.FlagErrorFunc)
	return cmd
}

func runNativeValidate(cmd *cobra.Command, dir, dirFormatValue, devURL string) error {
	result, err := validate(cmd.Context(), dir, dirFormatValue, devURL)
	if err != nil {
		// Native validation treats missing, unreadable, and malformed sum files
		// as usage failures distinct from content drift.
		return cmdutil.Fail(cmd, err)
	}

	if !result.Integrity.OK() {
		fmt.Fprintln(cmd.ErrOrStderr(), result.Integrity.Describe())
		return exitcode.New(1, errors.New("migration directory integrity check failed"))
	}

	return writeValidationSuccess(cmd, result)
}

func runAtlasValidate(cmd *cobra.Command, dir, dirFormatValue, devURL string) error {
	result, err := validate(cmd.Context(), dir, dirFormatValue, devURL)
	if errors.Is(err, migratesum.ErrSumFileMalformed) {
		return failAtlasChecksum(cmd, nil)
	}
	if err != nil {
		return cmdutil.Fail(cmd, err)
	}

	if !result.Integrity.OK() {
		return failAtlasChecksum(cmd, result.Integrity.FirstMismatch())
	}

	return writeValidationSuccess(cmd, result)
}

func validate(
	ctx context.Context,
	dir, dirFormatValue, devURL string,
) (migrationvalidate.Result, error) {
	if err := cmdutil.StatDir(dir); err != nil {
		return migrationvalidate.Result{}, err
	}

	dirFormat, err := migrator.ParseMigrationDirFormat(dirFormatValue)
	if err != nil {
		return migrationvalidate.Result{}, err
	}

	return migrationvalidate.Validate(ctx, migrationvalidate.Options{
		Dir:       dir,
		DirFormat: dirFormat,
		DevURL:    devURL,
	})
}

func writeValidationSuccess(cmd *cobra.Command, result migrationvalidate.Result) error {
	out := cmd.OutOrStdout()
	fmt.Fprintf(out, "OK: migrations directory matches %s\n", result.Integrity.SumFileName)
	if result.DevSQLValidated {
		fmt.Fprintln(out, "OK: migration SQL validated on dev database")
	}
	return nil
}

func failAtlasChecksum(cmd *cobra.Command, mismatch *migratesum.Mismatch) error {
	writeErr := errors.Join(
		writeAtlasChecksumGuidance(cmd.OutOrStdout(), mismatch),
		writeAtlasChecksumError(cmd.ErrOrStderr()),
	)
	if writeErr != nil {
		return exitcode.New(1, fmt.Errorf("%w: failed to write checksum output: %w", errAtlasChecksumMismatch, writeErr))
	}
	return exitcode.New(1, errAtlasChecksumMismatch)
}

func writeAtlasChecksumGuidance(w io.Writer, mismatch *migratesum.Mismatch) error {
	var guidance strings.Builder
	guidance.WriteString(atlasChecksumHeader)
	if mismatch != nil {
		fmt.Fprintf(&guidance, "\n\tL%d: %s was %s\n\n", mismatch.Line, mismatch.File, mismatch.Reason)
	}
	guidance.WriteString(atlasChecksumFooter)
	if _, err := fmt.Fprint(w, guidance.String()); err != nil {
		return fmt.Errorf("write checksum guidance: %w", err)
	}
	return nil
}

func writeAtlasChecksumError(w io.Writer) error {
	if _, err := fmt.Fprintln(w, "Error: checksum mismatch"); err != nil {
		return fmt.Errorf("write checksum error: %w", err)
	}
	return nil
}
