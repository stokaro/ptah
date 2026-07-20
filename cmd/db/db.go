// Package db contains native live-database command groups.
package db

import (
	"github.com/spf13/cobra"

	"github.com/stokaro/ptah/cmd/dropall"
	"github.com/stokaro/ptah/cmd/internal/cmdutil"
	"github.com/stokaro/ptah/cmd/readdb"
)

// NewDBCommand returns the native live-database command namespace.
func NewDBCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "db",
		Short: "Work with live database schemas",
		Long: `Work with live database schemas.

This is Ptah's native live-database namespace. Atlas-compatible spellings stay
under ptah atlas.`,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return cmd.Help()
		},
	}
	cmdutil.ConfigureCommandArgs(cmd, cmdutil.NoPositionalArgs)
	readCmd := readdb.NewReadDBCommand()
	readCmd.Short = "Read schema from a live database"
	readCmd.Long = "Read schema from a live database using Ptah's native database namespace."
	cmd.AddCommand(readCmd)

	dropAllCmd := dropall.NewDropAllCommand()
	dropAllCmd.Short = "Drop all schema objects in a live database"
	dropAllCmd.Long = "Drop all schema objects in a live database using Ptah's native database namespace."
	cmd.AddCommand(dropAllCmd)
	return cmd
}
