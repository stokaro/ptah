// Package version contains the ptah version command.
package version

import (
	"fmt"

	"github.com/spf13/cobra"

	"github.com/stokaro/ptah/cmd/internal/buildinfo"
)

// NewVersionCommand returns the version-reporting command.
func NewVersionCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "version",
		Short: "Print Ptah version information",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			info := buildinfo.Resolve()
			fmt.Fprintf(cmd.OutOrStdout(), "Version: %s\n", info.Version)
			fmt.Fprintf(cmd.OutOrStdout(), "Commit: %s\n", info.Commit)
			fmt.Fprintf(cmd.OutOrStdout(), "Date: %s\n", info.Date)
			fmt.Fprintf(cmd.OutOrStdout(), "Go: %s\n", info.Go)
			fmt.Fprintf(cmd.OutOrStdout(), "Platform: %s\n", info.Platform)
			return nil
		},
	}
}
