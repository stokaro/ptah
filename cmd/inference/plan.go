package inference

import (
	"context"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"go.5x5.cz/ptah/cmd/internal/dbcli"
	"go.5x5.cz/ptah/internal/embedrelease"
	"go.5x5.cz/ptah/internal/embedreport"
	"go.5x5.cz/ptah/internal/embedspec"
)

// newPlanCommand returns "ptah inference plan".
func newPlanCommand() *cobra.Command {
	var options commonOptions
	var currentGeneration string
	var evidence evidenceOptions

	cmd := &cobra.Command{
		Use:   "plan",
		Short: "Show what a generation change would do, and where each answer came from",
		Long: `Resolve a specification against a live database and print the plan.

The database is not written to. What the plan adds over a list of steps is
provenance: every fact says whether it was measured against the database,
configured by you, inferred by Ptah, unknown, or unsupported by this build.

That distinction is the point. A source nobody counted, rendered as zero, says
the backfill is free.

This is also where a generation change is put on the record. Naming
--publish-evidence or --evidence-file leaves a release: what this change
proposes, addressed by its own digest, carrying the specification it was built
from. A verification published later attaches to it as an OCI referrer, which is
how several verifications of one generation are found without remembering a tag
for each. Naming neither leaves nothing behind, and a verification with no
release to attach to is still publishable.

A release is what the next environment runs. --release on any verb reads the
specification out of it instead of a file, so development, staging and
production run one document rather than three copies of it.`,
		SilenceUsage: true,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runPlan(cmd.Context(), cmd.OutOrStdout(), options, currentGeneration, evidence)
		},
	}
	addCommonFlags(cmd, &options)
	cmd.Flags().StringVar(&currentGeneration, "current", "",
		"Identity of the generation queries read now, when there is one")
	addEvidenceFlags(cmd.Flags(), &evidence)
	return cmd
}

// addCommonFlags registers the flags every verb takes.
func addCommonFlags(cmd *cobra.Command, options *commonOptions) {
	addSpecFlags(cmd, options)
	cmd.Flags().StringVar(&options.dbURL, "db-url", "",
		"Database URL (required). Example: postgres://localhost:5432/dbname")
}

// addSpecFlags registers the two ways to name a specification.
//
// Mutually exclusive rather than one falling back to the other. A run given
// both would have been told twice which corpus to rebuild, and silently
// preferring either is how an environment promotes a digest and embeds the file
// that happened to be in its working directory.
//
// The source is shared by every copy of the options rather than read at each
// use, so one invocation runs against one specification whatever a mutable
// reference does in the meantime. [specSource] carries the rest of that
// reasoning.
func addSpecFlags(cmd *cobra.Command, options *commonOptions) {
	// ErrOrStderr as a method value rather than a writer, because a test
	// installs its own streams on the root command after this runs.
	options.spec = &specSource{notices: cmd.ErrOrStderr}
	cmd.Flags().StringVar(&options.spec.path, "spec", "",
		"Path to the embedding-migration specification; --spec or --release is required")
	cmd.Flags().StringVar(&options.spec.reference, "release", "",
		"OCI reference of a published release to run, which carries the specification; "+
			"an oci-layout:// directory is accepted for an air-gapped promotion")
	dbcli.RegisterPlainHTTPFlag(cmd.Flags(), &options.spec.plainHTTP)
	cmd.MarkFlagsMutuallyExclusive("spec", "release")
}

// runPlan resolves, prints, and records.
func runPlan(
	ctx context.Context, out io.Writer, options commonOptions,
	current string, evidence evidenceOptions,
) error {
	opened, err := open(ctx, options)
	if err != nil {
		return err
	}
	defer opened.close()

	plan, err := embedreport.BuildPlan(ctx, opened.db, opened.loaded, current)
	if err != nil {
		return err
	}
	if err := printPlan(out, plan); err != nil {
		return err
	}
	return publishRelease(ctx, out, options.spec.plainHTTP, opened.loaded, plan, evidence)
}

// publishRelease leaves the record of what this change proposes.
//
// A blocked plan is published too. The release states the generation, the
// document that proposed it, what it replaces and whether it can be rebuilt,
// and every one of those is true of a plan that cannot run yet -- it makes no
// claim that anything ran, which is what the verification record is for.
// Refusing here would lose the proposal an operator most wants to circulate,
// the one that is waiting on something.
func publishRelease(
	ctx context.Context, out io.Writer, plainHTTP bool, loaded embedspec.Loaded,
	plan embedreport.Plan, evidence evidenceOptions,
) error {
	// Any destination is a reason to build the record, and none of them is a
	// reason to build one: a plan that names no destination is the reading verb
	// it has always been.
	if !evidence.destinationNamed() {
		return nil
	}
	record, buildErr := embedrelease.NewReleaseRecord(
		embedreport.BuildRelease(loaded, plan, time.Now().UTC()), loaded.Document)
	return publishRecord(ctx, out, plainHTTP, evidence, record, buildErr)
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
