package schema

import (
	"fmt"

	"github.com/spf13/cobra"

	"go.5x5.cz/ptah/cmd/internal/cmdutil"
	"go.5x5.cz/ptah/internal/atlashclfmt"
)

const fmtCheckFlag = "check"

func newSchemaFmtCommand() *cobra.Command {
	var check bool
	cmd := &cobra.Command{
		Use:   "fmt [path ...]",
		Short: "Format HCL schema files",
		Long: `Format .hcl schema files using HashiCorp HCL's canonical layout. Directory
arguments are walked recursively. When no path is provided, the current
directory is used. Only files whose content changes are printed.

With --check nothing is rewritten: files that are not in canonical format are
printed and the command exits non-zero, for CI formatting gates.`,
		SilenceErrors: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			if check {
				return runSchemaFmtCheck(cmd, fmtPathArgs(args))
			}
			return runSchemaFmtWrite(cmd, fmtPathArgs(args))
		},
	}
	cmd.Flags().BoolVar(&check, fmtCheckFlag, false, "Report files that are not canonically formatted without rewriting them; exit non-zero when any are found")
	cmdutil.ConfigureCommandArgs(cmd, cobra.ArbitraryArgs)
	return cmd
}

func fmtPathArgs(args []string) []string {
	if len(args) == 0 {
		return []string{"."}
	}
	return args
}

func runSchemaFmtWrite(cmd *cobra.Command, paths []string) error {
	changed, err := atlashclfmt.FormatPaths(paths)
	if err != nil {
		return cmdutil.Fail(cmd, err)
	}
	for _, file := range changed {
		fmt.Fprintln(cmd.OutOrStdout(), file)
	}
	return nil
}

func runSchemaFmtCheck(cmd *cobra.Command, paths []string) error {
	unformatted, err := atlashclfmt.CheckPaths(paths)
	if err != nil {
		return cmdutil.Fail(cmd, err)
	}
	for _, file := range unformatted {
		fmt.Fprintln(cmd.OutOrStdout(), file)
	}
	if len(unformatted) > 0 {
		return cmdutil.Fail(cmd, fmt.Errorf(
			"%d file(s) are not canonically formatted; run `ptah schema fmt` to rewrite them", len(unformatted)))
	}
	return nil
}
