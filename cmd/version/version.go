// Package version contains the ptah version command.
package version

import (
	"github.com/spf13/cobra"

	"go.5x5.cz/ptah/cmd/internal/buildinfo"
	"go.5x5.cz/ptah/cmd/internal/cmdutil"
)

// NewVersionCommand returns the version-reporting command.
func NewVersionCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "version",
		Short: "Print Ptah version information",
		RunE: func(cmd *cobra.Command, _ []string) error {
			buildinfo.Write(cmd.OutOrStdout(), buildinfo.Resolve())
			return nil
		},
	}
	cmdutil.ConfigureCommand(cmd)
	return cmd
}
