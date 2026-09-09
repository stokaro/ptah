// Package db contains native live-database command groups.
package db

import (
	"github.com/spf13/cobra"

	"ptah.run/internal/cli/dbcapabilities"
	"ptah.run/internal/cli/dropall"
	"ptah.run/internal/cli/internal/cmdutil"
	"ptah.run/internal/cli/readdb"
)

// NewDBCommand returns the native live-database command namespace.
func NewDBCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "db",
		Short: "Work with live database schemas",
		Long: `Work with live database schemas.

This is Ptah's native live-database namespace. Atlas-compatible spellings live
in the separate ptah-compat binary.`,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return cmd.Help()
		},
	}
	cmdutil.ConfigureCommandArgs(cmd, cmdutil.NoPositionalArgs)

	// Only Short is set here, and only where this namespace reads better in the
	// parent listing than the verb's own one-liner does. Long is left alone.
	//
	// Overwriting Long too, with a sentence restating the verb's name and
	// adding "using Ptah's native database namespace", tells the reader what
	// they already know, having typed it. What that costs is worth naming:
	// `db drop-all --help` then shows one such line in place of the command's
	// own irreversibility warning, so the one verb in this namespace that
	// destroys data is the one whose help says the least about it.
	readCmd := readdb.NewReadDBCommand()
	readCmd.Short = "Read schema from a live database"
	cmd.AddCommand(readCmd)

	cmd.AddCommand(dropall.NewDropAllCommand())

	capabilitiesCmd := dbcapabilities.NewCapabilitiesCommand()
	capabilitiesCmd.Short = "Report the capability profile Ptah resolves for a live database"
	cmd.AddCommand(capabilitiesCmd)
	return cmd
}
