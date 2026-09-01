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

	"go.5x5.cz/ptah/internal/embedcatchup"
	"go.5x5.cz/ptah/internal/embedgen"
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

// Runnable reports whether the plan can be executed.
//
// It asks [embedplan.Plan], rather than reading len(Blockers) here, so there is
// one rule. That rule had no production caller at all: it was computed, tested
// five times, and read by nothing, so `ptah inference plan` printed `blocked:`
// lines and exited 0 -- a CI job gating on the plan passed against a
// specification that could not run (stokaro/ptah#2648 finding 1).
//
// internal/embedguard exists to report exactly that shape and did not, because
// it matches a declaration by bare name and cobra's own Command.Runnable is
// called in cmd/atlas. The guard's doc comment names that false negative as the
// direction it accepts; this is one it cost.
func (p Plan) Runnable() bool {
	return embedplan.Plan{Blockers: p.Blockers}.Runnable()
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
	facts := configuredFacts(loaded)

	targetExists, err := embedpg.ColumnExists(ctx, db, spec.Target.Table, spec.Target.Column)
	if err != nil {
		return embedplan.Inputs{}, err
	}
	sourceExists, err := embedpg.SourceTableExists(ctx, db, spec)
	if err != nil {
		return embedplan.Inputs{}, err
	}
	targetTableExists, err := embedpg.TargetTableExists(ctx, db, spec)
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
	indexBuildable, err := embedpg.VectorIndexBuildable(ctx, db, spec)
	if err != nil {
		return embedplan.Inputs{}, err
	}

	return embedplan.Inputs{
		Current:              current,
		Desired:              spec.Identity().Digest,
		Facts:                facts,
		SourceExists:         sourceExists,
		TargetTableExists:    targetTableExists,
		VectorIndexBuildable: indexBuildable,
		TargetExists:         targetExists,
		SourceMutable:        loaded.Source.Mutable,
		ConsistencyMode:      string(loaded.Mode),
		EstimatedRows:        rows,
		Capabilities:         capabilities,
		// Planning reads. The permission a plan needs is the one it has by
		// being able to open the database at all, and pretending otherwise
		// would be a check with nothing behind it.
		Permissions: map[string]bool{"inference:plan": true},
	}, nil
}

// configuredFacts are the answers the specification gives on its own.
//
// One function because the plan is assembled from literals, and a literal is
// the only way to build a fact that owes an explanation and gives none.
// `Facts.Undetailed` is the check for that, and it can only be run against a
// list something produces.
//
// The detail on source.table used to be the specification's NAME, which read as
// `source.table = articles (configured: articles)` whenever the two matched and
// as nothing at all when the specification carried no name -- a configured fact
// with no source, which is the shape Undetailed exists to report
// (stokaro/ptah#2474).
func configuredFacts(loaded embedspec.Loaded) embedplan.Facts {
	spec := loaded.Spec
	identity := spec.Identity()
	facts := embedplan.Facts{
		embedplan.ConfiguredFact("source.table", spec.Source.Table, "the specification"),
		embedplan.ConfiguredFact("model.identifier", spec.Model.Identifier, "the specification"),
		embedplan.ConfiguredFact("model.revision", spec.Model.Revision, "the specification"),
		embedplan.ConfiguredFact("provider.credential", loaded.Credential,
			"the specification, as a reference rather than a value"),
	}
	// Derived rather than configured, so it carries what it was derived FROM.
	//
	// Three pages said `plan` reports this and it reported nothing -- and the
	// sample one of them prints is in the plan's own fact vocabulary, so the
	// promise was coherent and only the fact was missing (stokaro/ptah#2648
	// finding 2). `DescribeSpecification` says the same thing and its comment
	// already said why: "Rendered as the plan renders it, so the two agree on
	// the words."
	//
	// It is reported on every run rather than only where it is partial, which
	// is more than the pages promise, and more than `describe` says: that
	// verb's `reproducibility_reason` is a complaint and stays absent for a
	// `full` answer. The two agree wherever there is something to complain
	// about, because both read the identity's own sentence. A fact that appears when the answer is
	// bad and is absent when it is good is one a reader cannot tell from a fact
	// nobody computed.
	//
	// So a `full` answer also appears under "What is not established", and that
	// is right rather than a wart: an inference is not a measurement, and this
	// one reads a revision string out of the specification without asking the
	// provider whether it honors it. `run.consistency_mode` is inferred and
	// listed the same way for the same reason.
	facts.Add(embedplan.InferredFact(
		"generation.reproducibility", string(identity.Reproducibility),
		reproducibilityFrom(identity)))
	return facts
}

// reproducibilityFrom is what the answer was derived from.
//
// A full reproducibility carries no reason of its own, and a fact with an empty
// detail is the shape `Facts.Undetailed` reports: an inferred answer owes its
// premise. So the premise is stated for both answers rather than only for the
// one that has a complaint attached.
func reproducibilityFrom(identity embedgen.Identity) string {
	if identity.ReproducibilityReason != "" {
		return identity.ReproducibilityReason
	}
	return "the specification pins an immutable model revision"
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
//
// The mode is the specification's, and it is a parameter because a run record
// does not carry one: an absent watermark means "catch-up has not run yet"
// under an outbox and "this mode records none" under the other two, and only
// the specification can tell them apart (stokaro/ptah#2646).
func ReadStatus(
	ctx context.Context, store *embedpg.Store, runID string, mode embedcatchup.Mode,
) (Status, error) {
	run, err := store.Run(ctx, runID)
	if err != nil {
		return Status{}, err
	}
	return StatusOf(run, mode), nil
}

// StatusOf renders a stored run under the mode its specification declares.
func StatusOf(run embedrun.Run, mode embedcatchup.Mode) Status {
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
		SnapshotWatermark: BoundaryText(run.SnapshotWatermark, mode),
		CatchUpWatermark:  BoundaryText(run.CatchUpWatermark, mode),
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
// An absent watermark is a fact worth a sentence rather than an empty string.
// What the sentence must not do is invent a reason: it said "the selected
// consistency mode records no boundary" for every empty watermark, which is
// true of `immutable` and `dual_write` and false of an outbox run between
// `prepare` and its first `catchup` -- the state every outbox run passes
// through, including the one the quick start walks a reader through. The
// operator was told something untrue about their own specification, and pointed
// away from the reason the same output stated a few lines further down
// (stokaro/ptah#2646).
//
// The mode is a parameter because it is the only thing that can tell the two
// apart, and a renderer that does not have it cannot answer.
func BoundaryText(watermark string, mode embedcatchup.Mode) string {
	if watermark != "" {
		return watermark
	}
	switch {
	case mode == "":
		// The caller does not have the specification, so it does not know which
		// of the two answers below is true and says neither. `inference_status`
		// on the agent surface is the case: it takes a run id and a target, and
		// a reason invented there would be the defect this replaces.
		return "none recorded"
	case recordsABoundary(mode):
		return "none yet, because catch-up has not run"
	default:
		return "none, because the selected consistency mode records no boundary"
	}
}

// recordsABoundary reports whether a mode establishes a watermark at all.
//
// Only the outbox does. The immutable mode has nothing to catch up on by
// definition, and dual write is the application's own business -- Ptah reads
// the writer's reports rather than a change log.
func recordsABoundary(mode embedcatchup.Mode) bool {
	return mode == embedcatchup.ModeOutbox
}

// Specification is what a specification file says on its own.
//
// Everything here comes from the file. Nothing is measured, and that is the
// point: every other answer this package produces needs a live database, so a
// specification could not be checked at all without one. An author writing one
// and a CI job asking "does this edit change the corpus" both need the file's
// own answer, and neither has a PostgreSQL to hand.
type Specification struct {
	Name string `json:"name"`
	// Generation is the identity this specification addresses, and Short is the
	// prefix an operator types.
	Generation string `json:"generation"`
	Short      string `json:"generation_short"`
	// Reproducibility says whether the generation can be rebuilt, and Reason
	// why not where it cannot. A file that says neither reads as "yes".
	//
	// Reason is a complaint, never a premise: it is absent for a `full` answer
	// on purpose. `plan` states a premise for both answers because its fact
	// vocabulary requires one, and the two are not in disagreement -- for
	// `partial` they render the identity's same sentence.
	Reproducibility string `json:"reproducibility"`
	Reason          string `json:"reproducibility_reason,omitempty"`
	// Disclosure is what running it would send out of the database. RowsInScope
	// is negative here: nobody counted, because counting needs the database.
	Disclosure Disclosure `json:"disclosure"`
	// ConsistencyMode is what would account for source changes, and Consistency
	// says what that mode can establish.
	ConsistencyMode string `json:"consistency_mode"`
	Consistency     string `json:"consistency"`
	// Target names the column a generation would write and the index over it,
	// which is what tells an author whether two specifications collide.
	TargetTable  string `json:"target_table"`
	TargetColumn string `json:"target_column"`
	TargetType   string `json:"target_type"`
	IndexName    string `json:"index_name,omitempty"`
	IndexMethod  string `json:"index_method,omitempty"`
}

// DescribeSpecification reads a loaded specification into what it says.
func DescribeSpecification(loaded embedspec.Loaded) (Specification, error) {
	objects, err := loaded.Spec.TargetObjects()
	if err != nil {
		return Specification{}, err
	}
	identity := loaded.Spec.Identity()

	described := Specification{
		Name:       loaded.Spec.Name,
		Generation: identity.Digest,
		Short:      identity.Short(),
		// The value, and the complaint where there is one, are the identity's
		// own -- so `describe` and `plan` cannot word a `partial` answer
		// differently: both read `ReproducibilityReason`.
		//
		// They differ on `full`, deliberately. The plan renders every fact as
		// `name = value (provenance: detail)` and `Facts.Undetailed` refuses an
		// inferred fact carrying no detail, so it states the premise it
		// inferred from. This field means "why not", and a specification that
		// pins a revision has no why not; `omitempty` keeps it out of the JSON
		// rather than filling it with a sentence that is not a complaint.
		Reproducibility: string(identity.Reproducibility),
		Reason:          identity.ReproducibilityReason,
		// Negative rather than zero: an uncounted source rendered as zero says
		// the disclosure is empty, which is the one answer it must not give.
		Disclosure:      disclosureOf(loaded, -1),
		ConsistencyMode: modeName(loaded.Mode),
		Consistency:     consistencyOf(loaded),
		TargetTable:     loaded.Spec.Target.Table,
		TargetColumn:    objects.Column.Name,
		TargetType:      objects.Column.Type,
	}
	if objects.HasIndex {
		described.IndexName = objects.Index.Name
		described.IndexMethod = objects.Index.Type
	}
	return described, nil
}
