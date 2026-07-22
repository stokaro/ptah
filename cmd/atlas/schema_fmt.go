package atlas

import (
	"fmt"

	"github.com/spf13/cobra"

	"github.com/stokaro/ptah/cmd/internal/cmdutil"
	"github.com/stokaro/ptah/internal/atlashclfmt"
)

func newAtlasSchemaFmtCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "fmt [path ...]",
		Short: "Format schema files",
		Long: `Atlas OSS ` + "`atlas schema fmt`" + ` command path.

Formats .hcl files using HashiCorp HCL's canonical layout. Directory arguments
are walked recursively. When no path is provided, the current directory is used.
Only files whose content changes are printed.`,
		RunE: runAtlasSchemaFmt,
	}
	cmdutil.ConfigureCommandArgs(cmd, cobra.ArbitraryArgs)
	return cmd
}

func runAtlasSchemaFmt(cmd *cobra.Command, args []string) error {
	paths := args
	if len(paths) == 0 {
		paths = []string{"."}
	}

	changed, err := atlashclfmt.FormatPaths(paths)
	if err != nil {
		return err
	}
	for _, file := range changed {
		fmt.Fprintln(cmd.OutOrStdout(), file)
	}
	return nil
}
