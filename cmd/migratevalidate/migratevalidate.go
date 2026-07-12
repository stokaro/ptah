// Package migratevalidate implements the `ptah migrate-validate` command: it
// verifies a migrations directory against its committed ptah.sum and exits
// non-zero on any drift (#161).
package migratevalidate

import (
	"errors"
	"fmt"

	"github.com/spf13/cobra"

	"github.com/stokaro/ptah/cmd/internal/cmdutil"
	"github.com/stokaro/ptah/cmd/internal/exitcode"
	"github.com/stokaro/ptah/migration/migratesum"
)

// NewMigrateValidateCommand returns the migrate-validate command.
func NewMigrateValidateCommand() *cobra.Command {
	var dir string

	cmd := &cobra.Command{
		Use:   "migrate-validate",
		Short: "Verify a migrations directory against its committed ptah.sum",
		Long: `migrate-validate recomputes the integrity hashes of a migrations directory
and compares them against the committed ptah.sum. It exits:

  0  the directory matches ptah.sum
  1  a migration file was added, removed, or edited out of band (drift)
  2  ptah.sum is missing or unreadable, or the directory is inaccessible

Run it in CI to guarantee already-committed migrations are never changed.`,
		Args:          cmdutil.NoPositionalArgs,
		SilenceUsage:  true,
		SilenceErrors: true,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runValidate(cmd, dir)
		},
	}
	cmd.Flags().StringVar(&dir, "dir", "./migrations", "Directory containing migration files")
	cmd.SetFlagErrorFunc(cmdutil.FlagErrorFunc)
	return cmd
}

func runValidate(cmd *cobra.Command, dir string) error {
	if err := cmdutil.StatDir(dir); err != nil {
		return cmdutil.Fail(cmd, err)
	}

	// A missing or unreadable ptah.sum, and any other verify error, is a
	// usage failure (exit 2) distinct from a content drift (exit 1). Its
	// message — including the actionable "run ptah migrate-hash" for a
	// missing sum — must reach the user, so print it here (the command
	// silences cobra's own error output).
	result, err := migratesum.VerifyDir(dir)
	if err != nil {
		return cmdutil.Fail(cmd, err)
	}

	if !result.OK() {
		fmt.Fprintln(cmd.ErrOrStderr(), result.Describe())
		return exitcode.New(1, errors.New("migration directory integrity check failed"))
	}

	fmt.Fprintf(cmd.OutOrStdout(), "OK: migrations directory matches %s\n", migratesum.FileName)
	return nil
}
