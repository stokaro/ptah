// Package embedreport answers what a generation change would do and how far it
// has got, without answering anything about the content it moves.
//
// The two questions have separate audiences and one implementation. An operator
// reads them at a terminal; an agent driving the lifecycle over the Model
// Context Protocol reads the same answers as data. Building them twice is how
// the two come to disagree, and the surface that disagrees quietly is the one
// nobody is watching.
//
// What is deliberately absent is the whole point of the package boundary.
// Nothing here carries a source row, a rendered model input, or a vector. The
// fields describing what leaves the system are the endpoint, the model and the
// NAMES of the columns whose text is sent -- which is what somebody needs to
// decide whether to authorize it, and is not the text itself.
package embedreport

import (
	"context"
	"database/sql"
	"fmt"

	"go.5x5.cz/ptah/internal/embedcatchup"
	"go.5x5.cz/ptah/internal/embedpg"
	"go.5x5.cz/ptah/internal/embedplan"
	"go.5x5.cz/ptah/internal/embedrun"
	"go.5x5.cz/ptah/internal/embedspec"
)

// Disclosure is what leaves the database if this plan runs.
//
// It is separate from the plan's facts because it answers a different question.
// The facts say what would change here; this says what would be sent
// elsewhere, and an operator authorizing a run is deciding about both.
type Disclosure struct {
	// Provider and Endpoint are who receives the text.
	Provider string `json:"provider"`
	Endpoint string `json:"endpoint"`
	// EndpointClass is what the specification declares that endpoint to be --
	// a local process, a self-hosted service, or a third party. Declared, not
	// measured: Ptah cannot tell from an address who operates it.
	EndpointClass string `json:"endpoint_class"`
	// Model and Revision identify what computes the vectors.
	Model    string `json:"model"`
	Revision string `json:"revision,omitempty"`
	// Fields are the NAMES of the source columns whose text is sent. The text
	// is not here and does not pass through this package.
	Fields []string `json:"fields"`
	// KeyFields are the columns identifying a row, which travel no further
	// than the database.
	KeyFields []string `json:"key_fields"`
	// Credential is how the credential is found, never what it is.
	Credential string `json:"credential,omitempty"`
	// RowsInScope is how many rows would be sent, negative when nobody counted.
	// An uncounted source rendered as zero says the disclosure is empty.
	RowsInScope int64 `json:"rows_in_scope"`
}

// Fact is one resolved fact and where it came from.
type Fact struct {
	Name       string `json:"name"`
	Value      string `json:"value"`
	Provenance string `json:"provenance"`
	Detail     string `json:"detail,omitempty"`
}

// Step is one thing the plan would do.
type Step struct {
	Phase  string `json:"phase"`
	Detail string `json:"detail"`
	// Mutating and Irreversible separate the lines a reader must not skim. A
	// plan that did not carry them would present dropping a column and adding
	// one as the same kind of line.
	Mutating     bool `json:"mutating"`
	Irreversible bool `json:"irreversible"`
}

// Plan is what a generation change would do.
type Plan struct {
	// Current is the generation queries read now, empty when there is none.
	Current string `json:"current,omitempty"`
	// Desired is the generation this plan would build.
	Desired string `json:"desired"`
	// Facts carry their provenance, which is the reason to read them at all:
	// a source nobody counted, rendered as zero, says the backfill is free.
	Facts []Fact `json:"facts"`
	Steps []Step `json:"steps"`
	// Blockers are why this plan cannot run.
	Blockers []string `json:"blockers,omitempty"`
	// Uncertain names what the plan needed and does not have, which is not the
	// same as a blocker.
	Uncertain []string `json:"uncertain,omitempty"`
	// Disclosure is what would leave the database.
	Disclosure Disclosure `json:"disclosure"`
	// ConsistencyMode is what would account for source changes during the run,
	// and Consistency says what that mode can establish.
	ConsistencyMode string `json:"consistency_mode"`
	Consistency     string `json:"consistency"`
}

// Progress is how much of the source a run has been through.
type Progress struct {
	RowsScanned      int64 `json:"rows_scanned"`
	RowsEmbedded     int64 `json:"rows_embedded"`
	RowsSkipped      int64 `json:"rows_skipped"`
	RowsDeleted      int64 `json:"rows_deleted"`
	BatchesCommitted int64 `json:"batches_committed"`
	RetryCount       int64 `json:"retry_count"`
	// The token counts are what the provider reported, which is what a bill is
	// read against.
	ProviderPromptTokens int64 `json:"provider_prompt_tokens"`
	ProviderTotalTokens  int64 `json:"provider_total_tokens"`
}

// Status is what a run has done and what it is waiting for.
type Status struct {
	RunID      string `json:"run_id"`
	Generation string `json:"generation"`
	Phase      string `json:"phase"`
	State      string `json:"state"`
	Progress   Progress
	// SnapshotWatermark and CatchUpWatermark are where the backfill's snapshot
	// began and how far the catch-up has read, rendered rather than raw: they
	// are positions in a log, and a position is not content.
	SnapshotWatermark string `json:"snapshot_watermark,omitempty"`
	CatchUpWatermark  string `json:"catch_up_watermark,omitempty"`
	// FencingToken is which holder may still commit. It is reported because a
	// run that appears stalled is usually one whose lease moved.
	FencingToken int64 `json:"fencing_token"`
	// LeaseHolder and LeaseUntil say who holds it and until when, empty when
	// nobody does.
	LeaseHolder string `json:"lease_holder,omitempty"`
	LeaseUntil  string `json:"lease_until,omitempty"`
	// FailureClass and FailureDetail are why it stopped, empty when it did not.
	FailureClass  string `json:"failure_class,omitempty"`
	FailureDetail string `json:"failure_detail,omitempty"`
	// ActivePointer is the generation queries read, and RollbackEligible
	// whether the previous one is still a place to go back to. Both are why a
	// cutover or a rollback is refused, which is the question an agent driving
	// this asks most often.
	ActivePointer    string `json:"active_pointer,omitempty"`
	RollbackEligible bool   `json:"rollback_eligible"`
	// VerificationRef, CutoverPlanRef and ApprovalRef are pointers to evidence
	// rather than the evidence: a digest to fetch, not a report to trust from
	// here.
	VerificationRef string `json:"verification_ref,omitempty"`
	CutoverPlanRef  string `json:"cutover_plan_ref,omitempty"`
	ApprovalRef     string `json:"approval_ref,omitempty"`
	// SpecDigest identifies the transformation, and Environment which
	// deployment the run belongs to.
	SpecDigest  string `json:"spec_digest"`
	Environment string `json:"environment,omitempty"`
	// UpdatedAt is when the run last moved, which is how a stall is told from
	// a run that is merely slow.
	UpdatedAt string `json:"updated_at,omitempty"`
}

// BuildPlan resolves a specification against a live database.
//
// It measures what it can and says so when it cannot. Nothing is created and
// nothing is written.
func BuildPlan(
	ctx context.Context, db *sql.DB, loaded embedspec.Loaded, current string,
) (Plan, error) {
	inputs, err := planInputs(ctx, db, loaded, current)
	if err != nil {
		return Plan{}, err
	}
	built := embedplan.Build(inputs)

	plan := Plan{
		Current:         built.Current,
		Desired:         built.Desired,
		Blockers:        built.Blockers,
		Uncertain:       built.Uncertain,
		Disclosure:      disclosureOf(loaded, inputs.EstimatedRows),
		ConsistencyMode: modeName(loaded.Mode),
		Consistency:     consistencyOf(loaded),
	}
	for _, fact := range built.Facts {
		plan.Facts = append(plan.Facts, Fact{
			Name: fact.Name, Value: fact.Value,
			Provenance: string(fact.Provenance), Detail: fact.Detail,
		})
	}
	for _, step := range built.Steps {
		plan.Steps = append(plan.Steps, Step{
			Phase: step.Phase, Detail: step.Summary,
			Mutating: step.Mutating, Irreversible: step.Irreversible,
		})
	}
	return plan, nil
}

// planInputs measures what it can and reports what it cannot.
func planInputs(
	ctx context.Context, db *sql.DB, loaded embedspec.Loaded, current string,
) (embedplan.Inputs, error) {
	spec := loaded.Spec
	facts := embedplan.Facts{
		embedplan.ConfiguredFact("source.table", spec.Source.Table, spec.Name),
		embedplan.ConfiguredFact("model.identifier", spec.Model.Identifier, "the specification"),
		embedplan.ConfiguredFact("model.revision", spec.Model.Revision, "the specification"),
		embedplan.ConfiguredFact("provider.credential", loaded.Credential,
			"the specification, as a reference rather than a value"),
	}

	targetExists, err := embedpg.ColumnExists(ctx, db, spec.Target.Table, spec.Target.Column)
	if err != nil {
		return embedplan.Inputs{}, err
	}
	rows, err := embedpg.CountRows(ctx, db, spec)
	if err != nil {
		return embedplan.Inputs{}, err
	}
	capabilities, err := embedpg.VectorCapabilities(ctx, db)
	if err != nil {
		return embedplan.Inputs{}, err
	}

	return embedplan.Inputs{
		Current:         current,
		Desired:         spec.Identity().Digest,
		Facts:           facts,
		TargetExists:    targetExists,
		SourceMutable:   loaded.Source.Mutable,
		ConsistencyMode: string(loaded.Mode),
		EstimatedRows:   rows,
		Capabilities:    capabilities,
		// Planning reads. The permission a plan needs is the one it has by
		// being able to open the database at all, and pretending otherwise
		// would be a check with nothing behind it.
		Permissions: map[string]bool{"inference:plan": true},
	}, nil
}

// disclosureOf says what would leave the database.
func disclosureOf(loaded embedspec.Loaded, rows int64) Disclosure {
	spec := loaded.Spec
	return Disclosure{
		Provider:      spec.Model.Provider,
		Endpoint:      loaded.Endpoint,
		EndpointClass: string(spec.Model.EndpointClass),
		Model:         spec.Model.Identifier,
		Revision:      spec.Model.Revision,
		Fields:        append([]string(nil), spec.Source.InputFields...),
		KeyFields:     append([]string(nil), spec.Source.KeyFields...),
		Credential:    loaded.Credential,
		RowsInScope:   rows,
	}
}

// consistencyOf says what the selected mode can establish.
//
// It belongs in the plan rather than at the cutover because that is when an
// operator can still change it: a dual-write migration whose writer has no
// evidence contract is a decision to take now, not a refusal to meet in an
// hour.
func consistencyOf(loaded embedspec.Loaded) string {
	if !loaded.Source.Mutable {
		return "the source is declared immutable, so no changes have to be accounted for"
	}
	switch loaded.Mode {
	case embedcatchup.ModeOutbox:
		return "the outbox event and the source change are one transaction, so a change that " +
			"committed has an event"
	case embedcatchup.ModeDualWrite:
		return "completeness will rest on what the writer reports; Ptah observes the reports " +
			"and not the writes"
	case embedcatchup.ModeImmutable:
		return "this requires writes to be paused for the duration, and the run refuses to " +
			"declare itself ready if they are not"
	case embedcatchup.ModeNone:
		return "nothing will establish that the backfill covers the source as it is now, and " +
			"the cutover will refuse"
	}
	return ""
}

// ModeName renders a consistency mode, including the absence of one.
func ModeName(mode embedcatchup.Mode) string { return modeName(mode) }

func modeName(mode embedcatchup.Mode) string {
	if mode == embedcatchup.ModeNone {
		return "none selected"
	}
	return string(mode)
}

// ReadStatus reports what a run has done.
func ReadStatus(ctx context.Context, store *embedpg.Store, runID string) (Status, error) {
	run, err := store.Run(ctx, runID)
	if err != nil {
		return Status{}, err
	}
	return StatusOf(run), nil
}

// StatusOf renders a stored run.
func StatusOf(run embedrun.Run) Status {
	status := Status{
		RunID:      run.ID,
		Generation: run.GenerationIdentity,
		Phase:      string(run.Phase),
		State:      string(run.Status),
		Progress: Progress{
			RowsScanned: run.Progress.RowsScanned, RowsEmbedded: run.Progress.RowsEmbedded,
			RowsSkipped: run.Progress.RowsSkipped, RowsDeleted: run.Progress.RowsDeleted,
			BatchesCommitted:     run.Progress.BatchesCommitted,
			RetryCount:           int64(run.Progress.RetryCount),
			ProviderPromptTokens: run.Progress.ProviderPromptTokens,
			ProviderTotalTokens:  run.Progress.ProviderTotalTokens,
		},
		SnapshotWatermark: BoundaryText(run.SnapshotWatermark),
		CatchUpWatermark:  BoundaryText(run.CatchUpWatermark),
		FencingToken:      run.FencingToken,
		FailureClass:      run.FailureClass,
		FailureDetail:     run.FailureDetail,
		ActivePointer:     run.ActivePointer,
		RollbackEligible:  run.RollbackEligible,
		VerificationRef:   run.VerificationRef,
		CutoverPlanRef:    run.CutoverPlanRef,
		ApprovalRef:       run.ApprovalRef,
		SpecDigest:        run.SpecDigest,
		Environment:       run.Environment,
	}
	if !run.UpdatedAt.IsZero() {
		status.UpdatedAt = run.UpdatedAt.UTC().Format(timeLayout)
	}
	if run.LeaseOwner != "" {
		status.LeaseHolder = run.LeaseOwner
		status.LeaseUntil = run.LeaseExpires.UTC().Format(timeLayout)
	}
	return status
}

// timeLayout is how a moment is rendered everywhere in this package.
const timeLayout = "2006-01-02T15:04:05Z07:00"

// BoundaryText renders a watermark, including its absence.
//
// An absent watermark is a fact worth a sentence rather than an empty string,
// and the sentence says why it is absent: a consistency mode that records no
// boundary is a choice the operator made, not a phase that failed to run.
func BoundaryText(watermark string) string {
	if watermark == "" {
		return "none, because the selected consistency mode records no boundary"
	}
	return watermark
}

// Describe renders a plan for a person.
func Describe(plan Plan) []string {
	lines := []string{fmt.Sprintf("generation %s", plan.Desired)}
	for _, fact := range plan.Facts {
		lines = append(lines, fmt.Sprintf("  %s = %s (%s)", fact.Name, fact.Value, fact.Provenance))
	}
	return lines
}
