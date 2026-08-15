// Command lintrules renders the documented lint-rule enumeration from the
// registries the linters actually consult (stokaro/ptah#1482).
//
// `markdown` prints the generated block of docs/site/src/content/docs/reference/lint-rules.md.
// `check` validates the catalog against those registries without printing
// anything, which is what a caller wants when it only needs the exit code.
//
//	lintrules markdown
//	lintrules check
package main

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"

	"go.5x5.cz/ptah/internal/lintcatalog"
)

func main() {
	if err := newCommand().Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "lintrules: %v\n", err)
		os.Exit(1)
	}
}

func newCommand() *cobra.Command {
	root := &cobra.Command{
		Use:           "lintrules",
		Short:         "Render the lint-rule enumeration from the rule registries",
		SilenceUsage:  true,
		SilenceErrors: true,
	}
	root.AddCommand(newMarkdownCommand(), newCheckCommand())
	return root
}

func newMarkdownCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "markdown",
		Short: "Print the generated documentation block",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return lintcatalog.WriteMarkdown(cmd.OutOrStdout())
		},
	}
}

func newCheckCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "check",
		Short: "Fail when the catalog and the rule registries disagree",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			entries, err := lintcatalog.Entries()
			if err != nil {
				return err
			}
			if err := lintcatalog.Validate(entries); err != nil {
				return err
			}
			fmt.Fprintf(cmd.OutOrStdout(), "lintrules: OK (%d rules)\n", len(entries))
			return nil
		},
	}
}
