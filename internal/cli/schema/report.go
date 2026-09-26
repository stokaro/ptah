package schema

import (
	"encoding/json"
	"fmt"
	"io"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"

	"ptah.run/internal/cli/internal/cmdflags"
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

// disableJSONEnvBinding keeps --json off the PTAH_* binding every other flag
// on the verb has.
//
// --json decides what standard output is, so it belongs to the invocation.
// PTAH_JSON is also what `ptah migrations up --json` and the other versioned
// verbs read, and an environment that exported it for them would turn every
// `schema plan` and `schema apply` there into a JSON run, changing the output
// of commands that never asked for a document. A caller that parses the
// document types the flag.
func disableJSONEnvBinding(flags *pflag.FlagSet, name string) {
	if err := cmdflags.DisableEnvBinding(flags, name); err != nil {
		panic(err)
	}
}

// resultDocument is the one --json document a verb writes on standard output.
//
// It is written on the failing path as well as the successful one, because the
// caller that most needs the result is the one whose run stopped. It is written
// at most once, so a panic after the document went out does not add a second
// one to the stream.
type resultDocument struct {
	enabled bool
	out     io.Writer
	written bool
}

func newResultDocument(cmd *cobra.Command, enabled bool) *resultDocument {
	return &resultDocument{enabled: enabled, out: cmd.OutOrStdout()}
}

// write writes report, and does nothing without --json or when a document was
// already written.
func (d *resultDocument) write(report any) error {
	if !d.enabled || d.written {
		return nil
	}
	d.written = true
	return writeReport(d.out, report)
}

// writeOnPanic writes report for a run that panicked. A write error is dropped:
// the panic is already the run's failure, and it is what the process reports.
func (d *resultDocument) writeOnPanic(report any) {
	_ = d.write(report)
}

// writeReport writes one JSON document to w, indented the way `ptah
// migrations up --json` indents its own.
func writeReport(w io.Writer, report any) error {
	encoder := json.NewEncoder(w)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(report); err != nil {
		return fmt.Errorf("write the JSON result: %w", err)
	}
	return nil
}
