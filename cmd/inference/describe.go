package inference

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"

	"github.com/spf13/cobra"

	"go.5x5.cz/ptah/internal/embedreport"
)

// newDescribeCommand returns "ptah inference describe".
func newDescribeCommand() *cobra.Command {
	var options commonOptions
	var format string

	cmd := &cobra.Command{
		Use:   "describe",
		Short: "Show what a specification says, without a database",
		Long: `Read a specification and report what it says on its own.

Every other verb needs a live database, so until this one a specification could
not be checked at all without PostgreSQL. Two readers need the file's own
answer and have no server to hand: an author writing a specification, and a CI
job asking whether an edit changed the corpus.

What it reports is the generation identity, whether that generation can be
rebuilt and why not, what running it would send out of the database, what the
consistency mode can establish, and the objects a generation would write.

Nothing here is measured. The row count is absent rather than zero, because
counting needs the database and an uncounted source rendered as zero says the
disclosure is empty.

--release reads a published release instead of a file, which is how a promotion
is checked by the environment receiving it before anything there is touched.`,
		SilenceUsage: true,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runDescribe(cmd.Context(), cmd.OutOrStdout(), options, format)
		},
	}
	addSpecFlags(cmd, &options)
	cmd.Flags().StringVar(&format, "format", "text",
		"Output format: text or json")
	return cmd
}

// runDescribe prints what the specification says.
func runDescribe(
	ctx context.Context, out io.Writer, options commonOptions, format string,
) error {
	loaded, err := options.spec.resolve(ctx)
	if err != nil {
		return err
	}
	described, err := embedreport.DescribeSpecification(loaded)
	if err != nil {
		return err
	}
	if format == "json" {
		return writeDescribedJSON(out, described)
	}
	if format != "text" {
		return fmt.Errorf("invalid --format value %q: text or json", format)
	}
	return writeDescribedText(out, described)
}

// writeDescribedJSON is the form a CI job diffs.
//
// Indented, because a diff of one line says a specification changed and not
// what changed in it.
func writeDescribedJSON(out io.Writer, described embedreport.Specification) error {
	body, err := json.MarshalIndent(described, "", "  ")
	if err != nil {
		return fmt.Errorf("render the specification: %w", err)
	}
	_, err = fmt.Fprintf(out, "%s\n", body)
	return err
}

// writeDescribedText is the form a person reads.
func writeDescribedText(out io.Writer, described embedreport.Specification) error {
	lines := []string{
		fmt.Sprintf("%s: generation %s", described.Name, described.Generation),
		bullet("reproducibility: " + reproducibilityText(described)),
		bullet(fmt.Sprintf("target: %s.%s %s",
			described.TargetTable, described.TargetColumn, described.TargetType)),
	}
	if described.IndexName != "" {
		lines = append(lines, bullet(fmt.Sprintf("index: %s using %s",
			described.IndexName, described.IndexMethod)))
	}
	lines = append(lines,
		section("What would leave the database:"),
		bullet(fmt.Sprintf("%s at %s, declared %s",
			described.Disclosure.Model, described.Disclosure.Endpoint,
			described.Disclosure.EndpointClass)),
		bullet("the text of "+strings.Join(described.Disclosure.Fields, ", ")),
		bullet("for a number of rows nobody counted, because this reads no database"),
		section("Consistency mode: "+described.ConsistencyMode),
		bullet(described.Consistency))
	return writeLines(out, lines...)
}

// reproducibilityText renders the answer with its reason.
//
// A reproducibility with no reason beside it reads as "yes" whatever word it
// carries, which is the one reading it must not produce.
func reproducibilityText(described embedreport.Specification) string {
	if described.Reason == "" {
		return described.Reproducibility
	}
	return described.Reproducibility + " (" + described.Reason + ")"
}
