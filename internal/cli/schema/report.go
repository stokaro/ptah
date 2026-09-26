package schema

import (
	"encoding/json"
	"fmt"
	"io"

	"github.com/spf13/cobra"
)

// humanOutput is where `schema plan` writes for a person.
//
// Under --json the document is the output, so everything written for a person
// goes to standard error. A `Planned schema changes:` block in front of the
// document would reach a caller as a parse error rather than as a help.
func (o schemaPlanOptions) humanOutput(cmd *cobra.Command) io.Writer {
	if o.jsonOutput {
		return cmd.ErrOrStderr()
	}
	return cmd.OutOrStdout()
}

// humanOutput is where `schema apply` writes for a person, the confirmation
// prompt included, under the rule [schemaPlanOptions.humanOutput] states.
func (o schemaApplyOptions) humanOutput(cmd *cobra.Command) io.Writer {
	if o.jsonOutput {
		return cmd.ErrOrStderr()
	}
	return cmd.OutOrStdout()
}

// writeReport writes one JSON document to w, indented the way `ptah
// migrations up --json` indents its own.
//
// It is written on the failing path as well as the successful one, because the
// caller that most needs the result is the one whose run stopped.
func writeReport(w io.Writer, report any) error {
	encoder := json.NewEncoder(w)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(report); err != nil {
		return fmt.Errorf("write the JSON result: %w", err)
	}
	return nil
}
