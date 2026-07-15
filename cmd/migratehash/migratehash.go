// Package migratehash implements the `ptah migrate-hash` command: it writes
// or updates the ptah.sum integrity file for a migrations directory (#161).
package migratehash

import (
	"fmt"

	"github.com/spf13/cobra"

	"github.com/stokaro/ptah/cmd/internal/cmdutil"
	"github.com/stokaro/ptah/migration/migratesum"
	"github.com/stokaro/ptah/migration/migrator"
)

// NewMigrateHashCommand returns the migrate-hash command.
func NewMigrateHashCommand() *cobra.Command {
	var dir string
	var dirFormatValue string

	cmd := &cobra.Command{
		Use:   "migrate-hash",
		Short: "Write or update the ptah.sum integrity file for a migrations directory",
		Long: `migrate-hash recomputes the integrity hashes of every migration file in a
directory and writes them to ptah.sum. Run it whenever you add, remove, or
intentionally edit a migration file, and commit the updated ptah.sum.

CI can then run 'ptah migrate-validate' to fail on any out-of-band change to
an already-committed migration.`,
		Args:          cmdutil.NoPositionalArgs,
		SilenceUsage:  true,
		SilenceErrors: true,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runHash(cmd, dir, dirFormatValue)
		},
	}
	cmd.Flags().StringVar(&dir, "dir", "./migrations", "Directory containing migration files")
	cmd.Flags().StringVar(&dirFormatValue, "dir-format", string(migrator.MigrationDirFormatAuto), "Migration directory format: auto, ptah, or atlas")
	cmd.SetFlagErrorFunc(cmdutil.FlagErrorFunc)
	return cmd
}

func runHash(cmd *cobra.Command, dir, dirFormatValue string) error {
	if err := cmdutil.StatDir(dir); err != nil {
		return cmdutil.Fail(cmd, err)
	}

	dirFormat, err := migrator.ParseMigrationDirFormat(dirFormatValue)
	if err != nil {
		return cmdutil.Fail(cmd, err)
	}

	sum, err := migratesum.WriteWithFormat(dir, dirFormat)
	if err != nil {
		return cmdutil.Fail(cmd, err)
	}

	out := cmd.OutOrStdout()
	fmt.Fprintf(out, "Wrote %s/%s\n", dir, migratesum.FileName)
	fmt.Fprintf(out, "%d migration file(s) hashed\n", len(sum.Entries))
	return nil
}
