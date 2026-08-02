// Package license implements the native `ptah license` command.
package license

import (
	"github.com/spf13/cobra"

	"go.5x5.cz/ptah/cmd/internal/cmdutil"
	"go.5x5.cz/ptah/cmd/internal/licensetext"
)

// NewLicenseCommand returns the native license command.
func NewLicenseCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "license",
		Short: "Print license and attribution information",
		Long:  "Print Ptah's license, copyright, source location, and Atlas-compatibility attribution notice.",
		RunE: func(cmd *cobra.Command, _ []string) error {
			licensetext.Write(cmd.OutOrStdout())
			return nil
		},
	}
	cmdutil.ConfigureCommandArgs(cmd, cmdutil.NoPositionalArgs)
	return cmd
}
