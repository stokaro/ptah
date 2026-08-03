// Package migratehash implements the migration hash command: it writes
// or updates the ptah.sum integrity file for a migrations directory (#161).
package migratehash

import (
	"fmt"
	"strings"

	"github.com/spf13/cobra"

	"go.5x5.cz/ptah/cmd/internal/cmdutil"
	"go.5x5.cz/ptah/internal/migratesum"
	"go.5x5.cz/ptah/internal/ociartifact"
	"go.5x5.cz/ptah/migration/migrator"
)

// NewMigrateHashCommand returns the migration hash command.
func NewMigrateHashCommand() *cobra.Command {
	var dir string
	var dirFormatValue string

	cmd := &cobra.Command{
		Use:   "hash",
		Short: "Write or update the ptah.sum integrity file for a migrations directory",
		Long: `migrations hash recomputes the integrity hashes of every migration file in a
directory and writes them to ptah.sum. Run it whenever you add, remove, or
intentionally edit a migration file, and commit the updated ptah.sum.

CI can then run 'ptah migrations validate' to fail on any out-of-band change to
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

// refuseOCIDir refuses an oci:// migration directory by name.
//
// Every read verb resolves oci:// references, including the digest-pinned
// oci://registry/repository:tag@sha256:D form (stokaro/ptah#1094). hash is the
// one verb that cannot: it writes the integrity file into the directory it
// hashed, and a registry artifact is immutable content addressed by its own
// digest, so the result has nowhere to go. Falling through to os.Stat reported
// "no such file or directory" for a reference that was never a path, which
// reads as "this tool does not know oci://" rather than "this verb writes".
func refuseOCIDir(dir string) error {
	if !strings.HasPrefix(dir, ociartifact.Scheme) {
		return nil
	}
	return fmt.Errorf(
		"cannot hash %s: an OCI artifact is immutable, so there is nowhere to write the integrity "+
			"file; hash the local migration directory instead and publish the result with "+
			"'ptah migrations push'",
		dir,
	)
}

func runHash(cmd *cobra.Command, dir, dirFormatValue string) error {
	if err := refuseOCIDir(dir); err != nil {
		return cmdutil.Fail(cmd, err)
	}
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
	sumFileName, err := migratesum.FileNameForFormat(dirFormat)
	if err != nil {
		return cmdutil.Fail(cmd, err)
	}

	out := cmd.OutOrStdout()
	fmt.Fprintf(out, "Wrote %s/%s\n", dir, sumFileName)
	fmt.Fprintf(out, "%d migration file(s) hashed\n", len(sum.Entries))
	return nil
}
