package embedplan_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/embedplan"
)

// resolved is a plan whose every question has an answer.
func resolved() embedplan.Inputs {
	return embedplan.Inputs{
		Current: "gen-old",
		Desired: "gen-new",
		Facts: embedplan.Facts{
			embedplan.MeasuredFact("source.table", "public.articles"),
			embedplan.ConfiguredFact("model.identifier", "text-embedding-3-large",
				"the migration specification"),
		},
		SourceExists:         true,
		TargetTableExists:    true,
		VectorIndexBuildable: true,
		SourceMutable:        true,
		ConsistencyMode:      "outbox",
		EstimatedRows:        120_000,
		Capabilities:         map[string]bool{"vector_type": true, "vector_index": true},
		Permissions:          map[string]bool{"inference:plan": true},
	}
}

// factNames lists the names a plan carries.
func factNames(plan embedplan.Plan) []string {
	names := make([]string, 0, len(plan.Facts))
	for _, fact := range plan.Facts {
		names = append(names, fact.Name)
	}
	return names
}

// stepSummaries lists what a plan would do.
func stepSummaries(plan embedplan.Plan) []string {
	summaries := make([]string, 0, len(plan.Steps))
	for _, step := range plan.Steps {
		summaries = append(summaries, step.Summary)
	}
	return summaries
}

// TestBuild_AFullyResolvedPlanIsRunnable is the control.
//
// Without it a planner that blocked everything satisfies every negative row
// below and no migration is ever plannable.
func TestBuild_AFullyResolvedPlanIsRunnable(t *testing.T) {
	c := qt.New(t)

	plan := embedplan.Build(resolved())

	c.Assert(plan.Blockers, qt.HasLen, 0, qt.Commentf("%v", plan.Blockers))
	c.Assert(plan.Runnable(), qt.IsTrue)
	c.Assert(plan.Uncertain, qt.HasLen, 0, qt.Commentf("%v", plan.Uncertain))
}

// TestBuild_AnUncountedSourceIsUnknownRatherThanZero is the plan's central
// promise.
//
// A row count nobody took, rendered as zero, tells the operator the backfill is
// free. The number is the same shape either way, which is why the provenance
// travels with it rather than beside it.
func TestBuild_AnUncountedSourceIsUnknownRatherThanZero(t *testing.T) {
	c := qt.New(t)
	inputs := resolved()
	inputs.EstimatedRows = -1

	plan := embedplan.Build(inputs)

	fact, found := plan.Facts.Lookup("source.estimated_rows")
	c.Assert(found, qt.IsTrue)
	c.Assert(fact.Value, qt.Equals, "unknown")
	c.Assert(fact.Provenance, qt.Equals, embedplan.Unknown)
	c.Assert(fact.Established(), qt.IsFalse)
	c.Assert(plan.Runnable(), qt.IsTrue)
	c.Assert(stepSummaries(plan), qt.Contains,
		"embed every in-scope source row; the source was not counted, so this is unsized")
}

// TestBuild_AnUnaskedCapabilityIsNotAnAbsentOne separates two answers a
// planner is most tempted to fold together.
//
// Not asked and not supported look alike in a boolean map and have opposite
// consequences: one refuses a database that would have worked, the other
// promises one that will not. Both block, and they block with different
// sentences.
func TestBuild_AnUnaskedCapabilityIsNotAnAbsentOne(t *testing.T) {
	tests := []struct {
		name         string
		capabilities map[string]bool
		wantFact     embedplan.Provenance
		wantBlocker  string
	}{
		{
			name:         "never asked",
			capabilities: map[string]bool{"vector_index": true},
			wantFact:     embedplan.Unknown,
			wantBlocker: "vector_type is required and was never established: a generation stores " +
				"vectors, and this build has no way to store them elsewhere",
		},
		{
			name:         "asked and answered no",
			capabilities: map[string]bool{"vector_type": false, "vector_index": true},
			wantFact:     embedplan.Unsupported,
			wantBlocker: "vector_type is required and the target database does not have it: a " +
				"generation stores vectors, and this build has no way to store them elsewhere",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			inputs := resolved()
			inputs.Capabilities = test.capabilities

			plan := embedplan.Build(inputs)

			fact, found := plan.Facts.Lookup("target.capability.vector_type")
			c.Assert(found, qt.IsTrue)
			c.Assert(fact.Provenance, qt.Equals, test.wantFact)
			c.Assert(plan.Runnable(), qt.IsFalse)
			c.Assert(plan.Blockers, qt.Contains, test.wantBlocker)
		})
	}
}

// TestBuild_AMutableSourceWithoutAModeIsUncertainAndStillPlannable is the
// distinction between a blocker and a warning.
//
// Nothing here stops the plan being written. What it stops is the plan claiming
// the backfill covers the source, and the cutover decision is where that
// becomes a refusal.
func TestBuild_AMutableSourceWithoutAModeIsUncertainAndStillPlannable(t *testing.T) {
	c := qt.New(t)
	inputs := resolved()
	inputs.ConsistencyMode = ""

	plan := embedplan.Build(inputs)

	c.Assert(plan.Runnable(), qt.IsTrue, qt.Commentf("%v", plan.Blockers))
	c.Assert(strings.Join(plan.Uncertain, "\n"), qt.Contains,
		"run.consistency_mode = unknown (unknown: the source can change during the run and no "+
			"mode was selected")
}

// TestBuild_AnImmutableSourceNeedsNoModeAndSaysWhy is the inference, with its
// premise attached.
//
// An operator who disagrees with "none" needs to be able to find the claim that
// produced it, and "none" on its own is indistinguishable from a mode nobody
// set.
func TestBuild_AnImmutableSourceNeedsNoModeAndSaysWhy(t *testing.T) {
	c := qt.New(t)
	inputs := resolved()
	inputs.SourceMutable = false
	inputs.ConsistencyMode = ""

	plan := embedplan.Build(inputs)

	fact, found := plan.Facts.Lookup("run.consistency_mode")
	c.Assert(found, qt.IsTrue)
	c.Assert(fact.Provenance, qt.Equals, embedplan.Inferred)
	c.Assert(fact.Value, qt.Equals, "none")
	c.Assert(fact.Detail, qt.Equals,
		"the source is declared immutable, so there is nothing for a consistency mode to catch")
	c.Assert(fact.Established(), qt.IsFalse)
	// Neither half of the consistency machinery is planned: there is nothing to
	// capture a boundary against and nothing to catch up to.
	phases := make([]string, 0, len(plan.Steps))
	for _, step := range plan.Steps {
		phases = append(phases, step.Phase)
	}
	c.Assert(phases, qt.Not(qt.Contains), "capture")
	c.Assert(phases, qt.Not(qt.Contains), "catch-up")
}

// TestBuild_ThePlannerDoesNotOverwriteWhatWasResolved keeps Phase A's answers.
//
// The layer that read a fact is the only one that knows whether it read or was
// told, and a planner reclassifying its own inputs would be guessing about
// them.
func TestBuild_ThePlannerDoesNotOverwriteWhatWasResolved(t *testing.T) {
	c := qt.New(t)

	plan := embedplan.Build(resolved())

	c.Assert(factNames(plan), qt.Contains, "source.table")
	table, _ := plan.Facts.Lookup("source.table")
	c.Assert(table.Provenance, qt.Equals, embedplan.Measured)
	model, _ := plan.Facts.Lookup("model.identifier")
	c.Assert(model.Provenance, qt.Equals, embedplan.Configured)
	c.Assert(model.Detail, qt.Equals, "the migration specification")
}

// TestBuild_TheStepsFollowTheLifecycle pins what a plan would do and in what
// order.
func TestBuild_TheStepsFollowTheLifecycle(t *testing.T) {
	c := qt.New(t)

	plan := embedplan.Build(resolved())

	phases := make([]string, 0, len(plan.Steps))
	for _, step := range plan.Steps {
		phases = append(phases, step.Phase)
	}
	c.Assert(phases, qt.DeepEquals, []string{
		"prepare", "capture", "backfill", "catch-up", "index", "verify", "cutover", "retire",
	})
}

// TestBuild_AnExistingTargetIsNotCreatedTwice keeps a resumed run from planning
// work that is already done.
func TestBuild_AnExistingTargetIsNotCreatedTwice(t *testing.T) {
	c := qt.New(t)
	inputs := resolved()
	inputs.TargetExists = true

	plan := embedplan.Build(inputs)

	c.Assert(stepSummaries(plan), qt.Not(qt.Contains),
		"create the vector column and metadata for generation gen-new")
	target, _ := plan.Facts.Lookup("target.exists")
	c.Assert(target.Value, qt.Equals, "true")
}

// TestBuild_IrreversibleStepsAreSeparated is the epic's "irreversible or
// uncertain operations" line.
//
// A plan that listed dropping a column and adding one as the same kind of line
// makes the operator find the difference themselves, on the one reading where
// getting it wrong cannot be undone.
func TestBuild_IrreversibleStepsAreSeparated(t *testing.T) {
	c := qt.New(t)

	plan := embedplan.Build(resolved())

	c.Assert(phasesWhere(plan, func(step embedplan.Step) bool { return step.Irreversible }),
		qt.DeepEquals, []string{"retire"})
	// Verification is the step that reads and changes nothing, and it is what
	// separates "every step" from "the mutating ones".
	mutating := phasesWhere(plan, func(step embedplan.Step) bool { return step.Mutating })
	c.Assert(mutating, qt.DeepEquals, []string{
		"prepare", "capture", "backfill", "catch-up", "index", "cutover", "retire",
	})
	c.Assert(mutating, qt.Not(qt.Contains), "verify")
}

// TestBuild_AFirstGenerationRetiresNothing is the control for the row above.
func TestBuild_AFirstGenerationRetiresNothing(t *testing.T) {
	c := qt.New(t)
	inputs := resolved()
	inputs.Current = ""

	plan := embedplan.Build(inputs)

	c.Assert(phasesWhere(plan, func(step embedplan.Step) bool { return step.Irreversible }),
		qt.HasLen, 0)
	c.Assert(stepSummaries(plan), qt.Contains,
		"point queries at generation gen-new, which nothing currently reads")
}

// TestBuild_ThePlannerNeedsPermissionToPlan keeps a read-only caller from
// producing a plan that reads as authorization.
func TestBuild_ThePlannerNeedsPermissionToPlan(t *testing.T) {
	c := qt.New(t)
	inputs := resolved()
	inputs.Permissions = nil

	plan := embedplan.Build(inputs)

	c.Assert(plan.Runnable(), qt.IsFalse)
	c.Assert(plan.Blockers, qt.Contains, "the caller does not hold inference:plan")
}

// TestBuild_TheUncertainListReadsWorstLast orders what an operator has to
// decide about.
//
// An inference is a claim with a premise behind it; an unknown is a gap; an
// unsupported is a gap in the product. Sorting them puts the ones that will
// stop the migration where the eye stops.
func TestBuild_TheUncertainListReadsWorstLast(t *testing.T) {
	c := qt.New(t)
	inputs := resolved()
	inputs.SourceMutable = false
	inputs.ConsistencyMode = ""
	inputs.EstimatedRows = -1
	inputs.Capabilities = map[string]bool{"vector_type": true, "vector_index": false}

	plan := embedplan.Build(inputs)

	c.Assert(plan.Uncertain, qt.HasLen, 3)
	c.Assert(plan.Uncertain[0], qt.Contains, "(inferred:")
	c.Assert(plan.Uncertain[1], qt.Contains, "(unknown:")
	c.Assert(plan.Uncertain[2], qt.Contains, "(unsupported:")
}

// TestPlan_StringNamesEveryFactAndStep keeps the rendered plan from dropping
// something the structure carries.
func TestPlan_StringNamesEveryFactAndStep(t *testing.T) {
	c := qt.New(t)
	inputs := resolved()
	inputs.Capabilities = map[string]bool{"vector_type": true}

	rendered := embedplan.Build(inputs).String()

	c.Assert(rendered, qt.Contains, "generation gen-new, replacing gen-old")
	c.Assert(rendered, qt.Contains, "source.table = public.articles (measured)")
	c.Assert(rendered, qt.Contains,
		"model.identifier = text-embedding-3-large (configured: the migration specification)")
	c.Assert(rendered, qt.Contains, "[backfill] embed 120000 in-scope source rows")
	c.Assert(rendered, qt.Contains, "blocked: vector_index is required and was never established")
}

// TestBuild_AnEmptySourceIsAMeasurementAndNotAGap is the boundary between a
// count of nothing and no count.
//
// A filter matching no rows is a real answer, and a plan calling it unknown
// sends the operator looking for a measurement that was already taken. The two
// are one comparison apart and read identically in a report.
func TestBuild_AnEmptySourceIsAMeasurementAndNotAGap(t *testing.T) {
	c := qt.New(t)
	inputs := resolved()
	inputs.EstimatedRows = 0

	plan := embedplan.Build(inputs)

	fact, found := plan.Facts.Lookup("source.estimated_rows")
	c.Assert(found, qt.IsTrue)
	c.Assert(fact.Value, qt.Equals, "0")
	c.Assert(fact.Provenance, qt.Equals, embedplan.Measured)
	c.Assert(fact.Established(), qt.IsTrue)
	c.Assert(stepSummaries(plan), qt.Contains, "embed 0 in-scope source rows")
	c.Assert(plan.Uncertain, qt.HasLen, 0, qt.Commentf("%v", plan.Uncertain))
}

// phasesWhere names the phases of the steps a predicate accepts.
//
// The plan carries Mutating and Irreversible per step and no longer carries
// filters over them: two methods that read a field a renderer already reads
// were a second way to ask one question, and nothing outside a test asked it
// (stokaro/ptah#2474).
func phasesWhere(plan embedplan.Plan, accepts func(embedplan.Step) bool) []string {
	phases := make([]string, 0, len(plan.Steps))
	for _, step := range plan.Steps {
		if accepts(step) {
			phases = append(phases, step.Phase)
		}
	}
	return phases
}

// TestBuild_AMissingSourceTableIsABlocker covers the flagship case of
// stokaro/ptah#2648 finding 1.
//
// A specification naming a table that is not there planned green: no blocker,
// exit 0, and the absence never named. The only trace was
// `source.estimated_rows = unknown`, whose stated reason is about cost and
// duration — an uncertainty where a refusal belonged, so a CI job gating on the
// plan passed and `prepare` then failed with a raw SQLSTATE 42P01.
func TestBuild_AMissingSourceTableIsABlocker(t *testing.T) {
	c := qt.New(t)
	inputs := resolved()
	inputs.SourceExists = false

	plan := embedplan.Build(inputs)

	c.Assert(plan.Runnable(), qt.IsFalse)
	c.Assert(plan.Blockers, qt.Contains,
		"the source table is not there, so there is nothing to read from")
}

// TestBuild_AMissingTargetTableIsABlocker covers the case the issue's own list
// reached only in its verification: with the source present and the target
// table absent, the plan was completely clean — `source.estimated_rows = 2
// (measured)` and not one blocker — and the run died at `prepare`.
func TestBuild_AMissingTargetTableIsABlocker(t *testing.T) {
	c := qt.New(t)
	inputs := resolved()
	inputs.TargetTableExists = false

	plan := embedplan.Build(inputs)

	c.Assert(plan.Runnable(), qt.IsFalse)
	c.Assert(plan.Blockers, qt.Contains,
		"the target table is not there, so the generation's column has nowhere to go")
}

// TestBuild_AnAbsentTargetColumnIsNotABlocker is the control that keeps the two
// target checks apart. Creating the generation's column is what `prepare` is
// for, so a plan refusing because the column is missing would refuse every
// first run there is.
func TestBuild_AnAbsentTargetColumnIsNotABlocker(t *testing.T) {
	c := qt.New(t)
	inputs := resolved()
	inputs.TargetExists = false

	plan := embedplan.Build(inputs)

	c.Assert(plan.Runnable(), qt.IsTrue, qt.Commentf("%v", plan.Blockers))
}

// TestBuild_AnUnbuildableVectorIndexIsABlocker covers the third case the
// promise names, and the costliest one.
//
// `vector_index` answering true says the server builds vector indexes; it says
// nothing about whether it builds THIS one. Measured on pgvector 0.8.1,
// `ivfflat` with `sparsevec` was reported as `target.capability.vector_index =
// true (measured)` with no blocker, and the run completed prepare, backfill and
// catchup before dying at index — the whole provider bill for the corpus paid
// before the plan's promise was found to be false.
func TestBuild_AnUnbuildableVectorIndexIsABlocker(t *testing.T) {
	c := qt.New(t)
	inputs := resolved()
	inputs.VectorIndexBuildable = false

	plan := embedplan.Build(inputs)

	c.Assert(plan.Runnable(), qt.IsFalse)
	c.Assert(plan.Blockers, qt.Contains,
		"the target database has no operator class for this representation and metric "+
			"under the index method the specification names, so the index cannot be built")
}

// TestBuild_TheVectorIndexCapabilityDoesNotStandInForBuildability keeps the two
// apart. A plan satisfied by the capability alone is the plan this case was
// hidden behind.
func TestBuild_TheVectorIndexCapabilityDoesNotStandInForBuildability(t *testing.T) {
	c := qt.New(t)
	inputs := resolved()
	inputs.Capabilities = map[string]bool{"vector_type": true, "vector_index": true}
	inputs.VectorIndexBuildable = false

	plan := embedplan.Build(inputs)

	c.Assert(plan.Runnable(), qt.IsFalse)
	c.Assert(factNames(plan), qt.Contains, "target.capability.vector_index")
}
