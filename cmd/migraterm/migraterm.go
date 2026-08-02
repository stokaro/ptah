// Package migraterm implements the migrations rm command: it deletes a
// migration's up/down pair and rewrites the integrity file, refusing to remove
// an already-applied migration unless forced (#662).
package migraterm

import (
	"fmt"

	"github.com/spf13/cobra"

	"go.5x5.cz/ptah/cmd/internal/cmdutil"
	"go.5x5.cz/ptah/cmd/internal/migratemaint"
	"go.5x5.cz/ptah/internal/migrateops"
)

// NewMigrateRmCommand returns the migrations rm command.
func NewMigrateRmCommand() *cobra.Command {
	opts := &migratemaint.Options{}
	cmd := &cobra.Command{
		Use:   "rm",
		Short: "Delete a migration and rewrite the integrity file",
		Long: `migrations rm deletes the up/down pair for a migration version and rewrites
ptah.sum / atlas.sum so the directory still validates. It refuses to delete a
migration that is already applied in the database given by --db-url unless --force
is passed; without --db-url it warns that applied state was not verified.`,
		Args:          cmdutil.NoPositionalArgs,
		SilenceUsage:  true,
		SilenceErrors: true,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runRm(cmd, opts)
		},
	}
	migratemaint.RegisterFlags(cmd.Flags(), opts)
	cmd.SetFlagErrorFunc(cmdutil.FlagErrorFunc)
	return cmd
}

func runRm(cmd *cobra.Command, opts *migratemaint.Options) error {
	r, err := opts.Resolve()
	if err != nil {
		return cmdutil.Fail(cmd, err)
	}
	dir, version, format := r.Dir, r.Version, r.Format
	if err := opts.Guard(cmd.Context(), cmd.OutOrStdout(), version, format); err != nil {
		return cmdutil.Fail(cmd, err)
	}
	res, err := migrateops.Remove(dir, version, format)
	if err != nil {
		return cmdutil.Fail(cmd, err)
	}
	out := cmd.OutOrStdout()
	for _, f := range res.Files {
		fmt.Fprintf(out, "Removed %s/%s\n", dir, f)
	}
	fmt.Fprintf(out, "Wrote %s/%s\n", dir, res.SumFile)
	return nil
}
