// Package migratehash implements the `ptah migrate-hash` command: it writes
// or updates the ptah.sum integrity file for a migrations directory (#161).
package migratehash

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"

	"github.com/stokaro/ptah/cmd/internal/exitcode"
	"github.com/stokaro/ptah/migration/migratesum"
)

// NewMigrateHashCommand returns the migrate-hash command.
func NewMigrateHashCommand() *cobra.Command {
	var dir string

	cmd := &cobra.Command{
		Use:   "migrate-hash",
		Short: "Write or update the ptah.sum integrity file for a migrations directory",
		Long: `migrate-hash recomputes the integrity hashes of every migration file in a
directory and writes them to ptah.sum. Run it whenever you add, remove, or
intentionally edit a migration file, and commit the updated ptah.sum.

CI can then run 'ptah migrate-validate' to fail on any out-of-band change to
an already-committed migration.`,
		Args:          cobra.NoArgs,
		SilenceUsage:  true,
		SilenceErrors: true,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runHash(cmd, dir)
		},
	}
	cmd.Flags().StringVar(&dir, "dir", "./migrations", "Directory containing migration files")
	return cmd
}

func runHash(cmd *cobra.Command, dir string) error {
	if info, err := os.Stat(dir); err != nil || !info.IsDir() {
		return fail(cmd, fmt.Errorf("migrations directory %s is not accessible", dir))
	}

	sum, err := migratesum.Write(dir)
	if err != nil {
		return fail(cmd, err)
	}

	out := cmd.OutOrStdout()
	fmt.Fprintf(out, "Wrote %s/%s\n", dir, migratesum.FileName)
	fmt.Fprintf(out, "%d migration file(s) hashed\n", len(sum.Entries))
	return nil
}

// fail prints an error to stderr and returns it as an exit-2 error.
func fail(cmd *cobra.Command, err error) error {
	fmt.Fprintf(cmd.ErrOrStderr(), "error: %s\n", err)
	return exitcode.New(2, err)
}
