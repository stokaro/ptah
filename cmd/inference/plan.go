package inference

import (
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/spf13/cobra"

	"go.5x5.cz/ptah/internal/embedreport"
)

// newPlanCommand returns "ptah inference plan".
func newPlanCommand() *cobra.Command {
	var options commonOptions
	var currentGeneration string

	cmd := &cobra.Command{
		Use:   "plan",
		Short: "Show what a generation change would do, and where each answer came from",
		Long: `Resolve a specification against a live database and print the plan.

Nothing is created and nothing is written. What the plan adds over a list of
steps is provenance: every fact says whether it was measured against the
database, configured by you, inferred by Ptah, unknown, or unsupported by this
build.

That distinction is the point. A source nobody counted, rendered as zero, says
the backfill is free.`,
		SilenceUsage: true,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runPlan(cmd.Context(), cmd.OutOrStdout(), options, currentGeneration)
		},
	}
	addCommonFlags(cmd, &options)
	cmd.Flags().StringVar(&currentGeneration, "current", "",
		"Identity of the generation queries read now, when there is one")
	return cmd
}

// addCommonFlags registers the flags every verb takes.
func addCommonFlags(cmd *cobra.Command, options *commonOptions) {
	cmd.Flags().StringVar(&options.specPath, "spec", "",
		"Path to the embedding-migration specification (required)")
	cmd.Flags().StringVar(&options.dbURL, "db-url", "",
		"Database URL (required). Example: postgres://localhost:5432/dbname")
}

// runPlan resolves and prints.
func runPlan(ctx context.Context, out io.Writer, options commonOptions, current string) error {
	opened, err := open(ctx, options)
	if err != nil {
		return err
	}
	defer opened.close()

	plan, err := embedreport.BuildPlan(ctx, opened.db, opened.loaded, current)
	if err != nil {
		return err
	}
	return printPlan(out, plan)
}

// printPlan renders the plan for a person.
//
// The plan itself is built by internal/embedreport, which both this verb and
// the agent surface consume. What is here is the rendering, which is the only
// half that differs between a terminal and a protocol.
func printPlan(out io.Writer, plan embedreport.Plan) error {
	lines := []string{"generation " + plan.Desired}
	for _, fact := range plan.Facts {
		detail := ""
		if fact.Detail != "" {
			detail = ": " + fact.Detail
		}
		lines = append(lines, fmt.Sprintf("  %s = %s (%s%s)",
			fact.Name, fact.Value, fact.Provenance, detail))
	}
	for _, step := range plan.Steps {
		lines = append(lines, fmt.Sprintf("  [%s] %s", step.Phase, step.Detail))
	}
	for _, blocker := range plan.Blockers {
		lines = append(lines, "  blocked: "+blocker)
	}
	if err := writeLines(out, lines...); err != nil {
		return err
	}
	if err := printUncertain(out, plan); err != nil {
		return err
	}
	return printDisclosure(out, plan)
}

// printUncertain says what the plan needed and does not have.
func printUncertain(out io.Writer, plan embedreport.Plan) error {
	if len(plan.Uncertain) == 0 {
		return nil
	}
	lines := []string{section("What is not established:")}
	for _, fact := range plan.Uncertain {
		lines = append(lines, bullet(fact))
	}
	return writeLines(out, lines...)
}

// printDisclosure says what would leave the database, which is a decision an
// operator takes separately from whether the plan is right.
func printDisclosure(out io.Writer, plan embedreport.Plan) error {
	disclosure := plan.Disclosure
	lines := []string{
		section("What leaves the database:"),
		bullet(fmt.Sprintf("%s at %s, declared %s",
			disclosure.Model, disclosure.Endpoint, disclosure.EndpointClass)),
		bullet("the text of " + strings.Join(disclosure.Fields, ", ")),
		bullet(rowsInScopeText(disclosure.RowsInScope)),
		section("Consistency mode: " + plan.ConsistencyMode),
		bullet(plan.Consistency),
	}
	return writeLines(out, lines...)
}

// rowsInScopeText renders a count, including nobody having taken one.
func rowsInScopeText(rows int64) string {
	if rows < 0 {
		return "for a number of rows nobody counted"
	}
	return fmt.Sprintf("for %d rows", rows)
}
