// Package migraterebase implements the migrations rebase command: it moves a
// migration to the end of history by re-timestamping its up/down pair and
// rewriting the integrity file, refusing to rebase an already-applied migration
// unless forced (#662).
package migraterebase

import (
	"fmt"

	"github.com/spf13/cobra"

	"go.5x5.cz/ptah/cmd/internal/cmdutil"
	"go.5x5.cz/ptah/cmd/internal/migratemaint"
	"go.5x5.cz/ptah/internal/migrateops"
)

// NewMigrateRebaseCommand returns the migrations rebase command.
func NewMigrateRebaseCommand() *cobra.Command {
	opts := &migratemaint.Options{}
	cmd := &cobra.Command{
		Use:   "rebase",
		Short: "Move a migration to the end of history and rewrite the integrity file",
		Long: `migrations rebase re-timestamps a migration's up/down pair to a fresh version
greater than every existing version, so it applies after concurrently-merged
work, then rewrites ptah.sum / atlas.sum. Because it changes the migration's
version, it is only valid for an unapplied migration: it refuses a migration that
is already applied in the database given by --db-url unless --force is passed;
without --db-url it warns that applied state was not verified.`,
		Args:          cmdutil.NoPositionalArgs,
		SilenceUsage:  true,
		SilenceErrors: true,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runRebase(cmd, opts)
		},
	}
	migratemaint.RegisterFlags(cmd.Flags(), opts)
	cmd.SetFlagErrorFunc(cmdutil.FlagErrorFunc)
	return cmd
}

func runRebase(cmd *cobra.Command, opts *migratemaint.Options) error {
	r, err := opts.Resolve()
	if err != nil {
		return cmdutil.Fail(cmd, err)
	}
	dir, version, format := r.Dir, r.Version, r.Format
	if err := opts.Guard(cmd.Context(), cmd.OutOrStdout(), version, format); err != nil {
		return cmdutil.Fail(cmd, err)
	}
	newVersion, res, err := migrateops.Rebase(dir, version, format)
	if err != nil {
		return cmdutil.Fail(cmd, err)
	}
	out := cmd.OutOrStdout()
	fmt.Fprintf(out, "Rebased migration %d to %d\n", version, newVersion)
	for _, f := range res.Files {
		fmt.Fprintf(out, "  %s/%s\n", dir, f)
	}
	fmt.Fprintf(out, "Wrote %s/%s\n", dir, res.SumFile)
	return nil
}
