package inference

import (
	"context"
	"fmt"
	"io"

	"github.com/spf13/cobra"

	"go.5x5.cz/ptah/internal/embedcatchup"
	"go.5x5.cz/ptah/internal/embedpg"
	"go.5x5.cz/ptah/internal/embedplan"
	"go.5x5.cz/ptah/internal/embedspec"
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

	inputs, err := resolvePlanInputs(ctx, opened, current)
	if err != nil {
		return err
	}
	plan := embedplan.Build(inputs)
	return printPlan(out, plan, opened.loaded)
}

// resolvePlanInputs measures what it can and reports what it cannot.
func resolvePlanInputs(
	ctx context.Context, opened *session, current string,
) (embedplan.Inputs, error) {
	spec := opened.loaded.Spec
	facts := embedplan.Facts{
		embedplan.ConfiguredFact("source.table", spec.Source.Table, opened.loaded.Spec.Name),
		embedplan.ConfiguredFact("model.identifier", spec.Model.Identifier, "the specification"),
		embedplan.ConfiguredFact("model.revision", spec.Model.Revision, "the specification"),
		embedplan.ConfiguredFact("provider.credential", opened.loaded.Credential,
			"the specification, as a reference rather than a value"),
	}

	targetExists, err := embedpg.ColumnExists(ctx, opened.db, spec.Target.Table, spec.Target.Column)
	if err != nil {
		return embedplan.Inputs{}, err
	}
	rows, err := embedpg.CountRows(ctx, opened.db, spec)
	if err != nil {
		return embedplan.Inputs{}, err
	}
	capabilities, err := embedpg.VectorCapabilities(ctx, opened.db)
	if err != nil {
		return embedplan.Inputs{}, err
	}

	return embedplan.Inputs{
		Current:         current,
		Desired:         spec.Identity().Digest,
		Facts:           facts,
		TargetExists:    targetExists,
		SourceMutable:   opened.loaded.Source.Mutable,
		ConsistencyMode: string(opened.loaded.Mode),
		EstimatedRows:   rows,
		Capabilities:    capabilities,
		// Planning reads. The permission a plan needs is the one it has by
		// being able to open the database at all, and pretending otherwise
		// would be a check with nothing behind it.
		Permissions: map[string]bool{"inference:plan": true},
	}, nil
}

// printPlan renders the plan for a person.
func printPlan(out io.Writer, plan embedplan.Plan, loaded embedspec.Loaded) error {
	if err := writeLines(out, plan.String()); err != nil {
		return err
	}
	if len(plan.Uncertain) > 0 {
		lines := []string{section("What is not established:")}
		for _, fact := range plan.Uncertain {
			lines = append(lines, bullet(fact))
		}
		if err := writeLines(out, lines...); err != nil {
			return err
		}
	}
	return printConsistency(out, plan, loaded)
}

// printConsistency says what the selected mode can prove, before anything runs.
//
// It is printed at plan time rather than at cutover time because that is when
// an operator can still change it: a dual-write migration whose writer has no
// evidence contract is a decision to take now, not a refusal to meet in an hour.
func printConsistency(out io.Writer, plan embedplan.Plan, loaded embedspec.Loaded) error {
	if !loaded.Source.Mutable {
		return writeLines(out, section("The source is declared immutable, so no changes have to be accounted for."))
	}
	lines := []string{section(fmt.Sprintf("Consistency mode: %s", modeName(loaded.Mode)))}
	switch loaded.Mode {
	case embedcatchup.ModeOutbox:
		lines = append(lines, bullet(
			"the outbox event and the source change are one transaction, so a change that "+
				"committed has an event"))
	case embedcatchup.ModeDualWrite:
		lines = append(lines, bullet(
			"completeness will rest on what the writer reports; Ptah observes the reports and "+
				"not the writes"))
	case embedcatchup.ModeImmutable:
		lines = append(lines, bullet(
			"this requires writes to be paused for the duration, and the run refuses to declare "+
				"itself ready if they are not"))
	case embedcatchup.ModeNone:
		lines = append(lines, bullet(
			"nothing will establish that the backfill covers the source as it is now, and the "+
				"cutover will refuse"))
	}
	_ = plan
	return writeLines(out, lines...)
}

// modeName renders a mode for a person, including the absence of one.
func modeName(mode embedcatchup.Mode) string {
	if mode == embedcatchup.ModeNone {
		return "none selected"
	}
	return string(mode)
}
