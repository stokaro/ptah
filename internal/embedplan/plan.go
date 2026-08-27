package embedplan

import (
	"fmt"
	"sort"
	"strings"
)

// Inputs are everything Phase A resolved, as facts.
//
// They arrive already carrying their provenance, because the layer that read
// them is the only one that knows whether it read or was told. A planner that
// classified them itself would be guessing about its own inputs.
type Inputs struct {
	// Current is the generation queries read now, empty when there is none.
	Current string
	// Desired is the generation this plan would build.
	Desired string
	// Facts are the resolved facts, in the order Phase A produced them.
	Facts Facts
	// TargetExists reports whether the desired generation's column is already
	// there.
	TargetExists bool
	// SourceMutable reports whether the source can change under the run.
	SourceMutable bool
	// ConsistencyMode is the mode the operator selected, empty for none.
	ConsistencyMode string
	// EstimatedRows is how many source rows are in scope, negative when nobody
	// counted.
	EstimatedRows int64
	// Capabilities are what the target database can do, by name.
	Capabilities map[string]bool
	// Permissions are what the caller holds.
	Permissions map[string]bool
}

// Step is one thing the plan would do.
type Step struct {
	// Phase names which lifecycle phase it belongs to.
	Phase string
	// Summary says what it does.
	Summary string
	// Mutating reports whether it changes anything.
	Mutating bool
	// Irreversible reports whether it cannot be undone. A plan that did not
	// separate these from the rest would present dropping a column and adding
	// one as the same kind of line.
	Irreversible bool
}

// Plan is what Phase B produces.
type Plan struct {
	// Current and Desired are the generations.
	Current string
	Desired string
	// Facts are every fact the plan rests on, with its provenance.
	Facts Facts
	// Steps are what would happen, in order.
	Steps []Step
	// Blockers are the reasons this plan cannot run at all.
	Blockers []string
	// Uncertain names the facts the plan needed and does not have. It is not
	// the same as a blocker: a migration can run with an unknown row count,
	// and the operator should know they are agreeing to something nobody has
	// sized.
	Uncertain []string
}

// Runnable reports whether the plan can be executed.
func (p Plan) Runnable() bool {
	return len(p.Blockers) == 0
}

// Mutations lists the steps that change something.
func (p Plan) Mutations() []Step {
	var mutating []Step
	for _, step := range p.Steps {
		if step.Mutating {
			mutating = append(mutating, step)
		}
	}
	return mutating
}

// Irreversible lists the steps that cannot be undone.
func (p Plan) Irreversible() []Step {
	var irreversible []Step
	for _, step := range p.Steps {
		if step.Irreversible {
			irreversible = append(irreversible, step)
		}
	}
	return irreversible
}

// requiredCapabilities are what a generation needs from the target database.
var requiredCapabilities = []struct {
	key    string
	reason string
}{
	{key: "vector_type", reason: "a generation stores vectors, and this build has no way to store them elsewhere"},
	{key: "vector_index", reason: "without an index every query is a sequential scan over the whole corpus"},
}

// Build produces the plan.
//
// It never turns an absent answer into a confident one. Where something could
// not be established the plan says so, in the place the answer would have gone,
// and an operator reading it can tell a migration that was sized from one that
// was not.
func Build(inputs Inputs) Plan {
	plan := Plan{
		Current: inputs.Current,
		Desired: inputs.Desired,
		Facts:   append(Facts(nil), inputs.Facts...),
	}
	planFacts(&plan, inputs)
	planCapabilities(&plan, inputs)
	planSteps(&plan, inputs)

	for _, fact := range plan.Facts.Unestablished() {
		plan.Uncertain = append(plan.Uncertain, fact.String())
	}
	sort.Strings(plan.Blockers)
	return plan
}

// planFacts records what the planner itself worked out.
func planFacts(plan *Plan, inputs Inputs) {
	if inputs.EstimatedRows < 0 {
		plan.Facts.Add(UnknownFact("source.estimated_rows",
			"the source was not counted, so the cost and duration of the backfill are not known"))
	} else {
		plan.Facts.Add(MeasuredFact("source.estimated_rows", fmt.Sprintf("%d", inputs.EstimatedRows)))
	}

	switch {
	case !inputs.SourceMutable:
		plan.Facts.Add(InferredFact("run.consistency_mode", "none",
			"the source is declared immutable, so there is nothing for a consistency mode to catch"))
	case inputs.ConsistencyMode != "":
		plan.Facts.Add(ConfiguredFact("run.consistency_mode", inputs.ConsistencyMode, "the migration specification"))
	default:
		plan.Facts.Add(UnknownFact("run.consistency_mode",
			"the source can change during the run and no mode was selected, so nothing would "+
				"establish that the backfill covers the source as it is now"))
	}

	if inputs.TargetExists {
		plan.Facts.Add(MeasuredFact("target.exists", "true"))
		return
	}
	plan.Facts.Add(MeasuredFact("target.exists", "false"))
}

// planCapabilities holds the target database to what a generation needs.
func planCapabilities(plan *Plan, inputs Inputs) {
	for _, required := range requiredCapabilities {
		supported, known := inputs.Capabilities[required.key]
		switch {
		case !known:
			// Not asked is not the same as not supported. A planner treating
			// them alike either refuses a database that would have worked or
			// promises one that will not.
			plan.Facts.Add(UnknownFact("target.capability."+required.key,
				"the target database was not asked whether it supports this"))
			plan.Blockers = append(plan.Blockers,
				fmt.Sprintf("%s is required and was never established: %s", required.key, required.reason))
		case !supported:
			plan.Facts.Add(UnsupportedFact("target.capability."+required.key, required.reason))
			plan.Blockers = append(plan.Blockers,
				fmt.Sprintf("%s is required and the target database does not have it: %s",
					required.key, required.reason))
		default:
			plan.Facts.Add(MeasuredFact("target.capability."+required.key, "true"))
		}
	}
	if !inputs.Permissions["inference:plan"] {
		plan.Blockers = append(plan.Blockers, "the caller does not hold inference:plan")
	}
}

// planSteps lays out what would happen.
func planSteps(plan *Plan, inputs Inputs) {
	if !inputs.TargetExists {
		plan.Steps = append(plan.Steps, Step{
			Phase: "prepare", Mutating: true,
			Summary: fmt.Sprintf("create the vector column and metadata for generation %s", inputs.Desired),
		})
	}
	if inputs.SourceMutable && inputs.ConsistencyMode != "" {
		plan.Steps = append(plan.Steps, Step{
			Phase: "capture", Mutating: true,
			Summary: fmt.Sprintf("install the %s mechanism and record its starting position",
				inputs.ConsistencyMode),
		})
	}
	plan.Steps = append(plan.Steps,
		Step{Phase: "backfill", Mutating: true, Summary: backfillSummary(inputs)},
	)
	if inputs.SourceMutable && inputs.ConsistencyMode != "" {
		plan.Steps = append(plan.Steps, Step{
			Phase: "catch-up", Mutating: true,
			Summary: "process every source change recorded after the snapshot began",
		})
	}
	plan.Steps = append(plan.Steps,
		Step{Phase: "index", Mutating: true, Summary: "build the vector index and wait for it to be valid"},
		Step{Phase: "verify", Summary: "run every verification layer against the exact target generation"},
		Step{Phase: "cutover", Mutating: true, Summary: cutoverSummary(inputs)},
	)
	if inputs.Current != "" {
		plan.Steps = append(plan.Steps, Step{
			Phase: "retire", Mutating: true, Irreversible: true,
			Summary: fmt.Sprintf(
				"retire generation %s, which is a separate decision and is not part of this plan",
				inputs.Current),
		})
	}
}

// backfillSummary says what the backfill would cover, without inventing a size.
func backfillSummary(inputs Inputs) string {
	if inputs.EstimatedRows < 0 {
		return "embed every in-scope source row; the source was not counted, so this is unsized"
	}
	return fmt.Sprintf("embed %d in-scope source rows", inputs.EstimatedRows)
}

// cutoverSummary says what the pointer move replaces.
func cutoverSummary(inputs Inputs) string {
	if inputs.Current == "" {
		return fmt.Sprintf("point queries at generation %s, which nothing currently reads", inputs.Desired)
	}
	return fmt.Sprintf("point queries at generation %s, replacing %s", inputs.Desired, inputs.Current)
}

// String renders the plan for a person, worst-known facts last.
func (p Plan) String() string {
	var b strings.Builder
	fmt.Fprintf(&b, "generation %s", p.Desired)
	if p.Current != "" {
		fmt.Fprintf(&b, ", replacing %s", p.Current)
	}
	b.WriteByte('\n')
	for _, fact := range p.Facts {
		fmt.Fprintf(&b, "  %s\n", fact)
	}
	for _, step := range p.Steps {
		fmt.Fprintf(&b, "  [%s] %s\n", step.Phase, step.Summary)
	}
	for _, blocker := range p.Blockers {
		fmt.Fprintf(&b, "  blocked: %s\n", blocker)
	}
	return b.String()
}
