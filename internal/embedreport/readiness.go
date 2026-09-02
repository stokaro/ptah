package embedreport

import (
	"context"
	"database/sql"
	"fmt"
	"slices"
	"strconv"
	"time"

	"go.5x5.cz/ptah/internal/embedcatchup"
	"go.5x5.cz/ptah/internal/embedcutover"
	"go.5x5.cz/ptah/internal/embedgen"
	"go.5x5.cz/ptah/internal/embedpg"
	"go.5x5.cz/ptah/internal/embedrelease"
	"go.5x5.cz/ptah/internal/embedrun"
	"go.5x5.cz/ptah/internal/embedspec"
	"go.5x5.cz/ptah/internal/embedverify"
)

// Readiness is what is true of a generation now: whether the deterministic
// checks pass, and whether a cutover would be allowed to proceed.
//
// It is the answer a rollout gate waits on. A deployment of a new model has to
// know that the persistent state it will read has been built and measured, and
// the two conditions it waits for are these.
//
// Measured rather than remembered. A verification recorded an hour ago is not a
// statement about the source as it is now, and a gate reading a stored verdict
// would let a deployment proceed against a corpus that has drifted since.
type Readiness struct {
	// Verified reports whether every deterministic layer passes right now.
	Verified bool `json:"verified"`
	// CutoverReady reports whether a cutover would proceed on the state alone.
	//
	// It excludes the approval, which is not a defect in the state: a gate that
	// waited for an unsigned approval would wait forever under the policy most
	// production environments run. ApprovalRequired says whether one is still
	// owed, and PlanDigest is what it would bind to.
	CutoverReady     bool   `json:"cutover_ready"`
	ApprovalRequired bool   `json:"approval_required"`
	PlanDigest       string `json:"plan_digest,omitempty"`
	// Findings are what the verification said, and Blockers what a cutover
	// would refuse for. They are separate lists because they are separate
	// questions: a generation can verify cleanly and still be uncutoverable
	// because somebody else moved the pointer.
	Findings []string `json:"findings,omitempty"`
	Blockers []string `json:"blockers,omitempty"`
	// Unmeasured names the checks that did not run.
	//
	// A report listing only what it found reads as though it looked at
	// everything, and "verified: false" with no reason is the answer a gate
	// operator cannot act on.
	Unmeasured []string `json:"unmeasured,omitempty"`
	// SourceRows and TargetRows are the shape the answer was taken on.
	SourceRows int `json:"source_rows"`
	TargetRows int `json:"target_rows"`
	// MeasuredAt is when, so a reader can tell this answer from a cached one.
	MeasuredAt string `json:"measured_at"`
}

// ReadReadiness measures a generation and decides what a cutover would do.
//
// Nothing is written. This is the read-only half of what `cutover` does before
// it moves a pointer, and it is the same code: a gate that agreed with the
// cutover verb only by coincidence is one that will eventually let a deployment
// proceed against a generation the cutover then refuses.
func ReadReadiness(
	ctx context.Context, db *sql.DB, store *embedpg.Store,
	loaded embedspec.Loaded, runID string, now time.Time,
) (Readiness, error) {
	run, err := store.Run(ctx, runID)
	if err != nil {
		return Readiness{}, err
	}
	readiness := Readiness{MeasuredAt: now.UTC().Format(timeLayout)}

	// A generation whose column is not there yet has nothing to measure, and
	// asking anyway means selecting a column that does not exist -- an error
	// about SQL, where the fact is that `prepare` has not run. A gate polling
	// from the start of a run meets this case first.
	exists, err := embedpg.ColumnExists(ctx, db, loaded.Spec.Target.Table, loaded.Spec.Target.Column)
	if err != nil {
		return Readiness{}, err
	}
	if !exists {
		readiness.Blockers = []string{"the target column " +
			loaded.Spec.Target.Table + "." + loaded.Spec.Target.Column +
			" does not exist, so nothing has been built to measure"}
		readiness.Unmeasured = []string{"every deterministic layer, because there is no target"}
		return readiness, nil
	}

	report, err := VerifyGeneration(ctx, db, store, loaded, run)
	if err != nil {
		return Readiness{}, err
	}
	readiness.Verified = report.Passed()
	readiness.SourceRows, readiness.TargetRows = report.SourceRows, report.TargetRows
	readiness.Unmeasured = report.Unmeasured
	for _, finding := range report.Findings {
		readiness.Findings = append(readiness.Findings,
			"["+string(finding.Layer)+"/"+string(finding.Severity)+"] "+finding.Summary)
	}

	plan, observed, err := BuildCutoverPlan(ctx, db, store, loaded, run, report, nil, now)
	if err != nil {
		return Readiness{}, err
	}
	assessed := embedcutover.AssessReadiness(plan, loaded.Policy, observed)
	readiness.CutoverReady = assessed.Ready
	readiness.Blockers = assessed.Blockers
	readiness.PlanDigest = assessed.PlanDigest
	readiness.ApprovalRequired = assessed.ApprovalRequired
	return readiness, nil
}

// VerifyGeneration runs every deterministic layer against the live generation.
//
// It lives here rather than in the command that prints it because three callers
// need the same answer: the verify verb, the cutover it gates, and the
// readiness a rollout waits on. Two of those existed before this one, and the
// third is the reason the answer had to stop being assembled at the CLI.
func VerifyGeneration(
	ctx context.Context, db *sql.DB, store *embedpg.Store,
	loaded embedspec.Loaded, run embedrun.Run,
) (embedverify.Report, error) {
	spec := loaded.Spec
	structure, err := embedpg.ReadStructure(ctx, db, spec, ActivePointer(ctx, store, spec.Target.Schema, spec.Target.Table))
	if err != nil {
		return embedverify.Report{}, err
	}
	corpus, err := embedpg.VerificationCorpus(ctx, db, spec)
	if err != nil {
		return embedverify.Report{}, err
	}
	guarantee, err := AssessConsistency(ctx, db, loaded, run)
	if err != nil {
		return embedverify.Report{}, err
	}
	objects, err := spec.TargetObjects()
	if err != nil {
		return embedverify.Report{}, err
	}
	return embedverify.Verify(
		embedverify.Expectation{
			Generation:    spec.Identity().Digest,
			ColumnType:    objects.Column.Type,
			Dimension:     spec.Model.ReportedDimension,
			IndexMethod:   objects.Index.Type,
			OperatorClass: objects.Index.Operator,
			RequireIndex:  objects.HasIndex && run.Reached(embedrun.PhaseBackfilled),
		},
		structure, corpus,
		embedverify.RunState{
			// The fact the backfill recorded when its walk ran off the end
			// of the source, rather than a phase standing in for it. Two
			// phase readings stood here and both were wrong in ways nothing
			// noticed: `Phase != PhaseBackfilling` was true for every phase
			// BEFORE the backfill as well as after it, and
			// `Reached(PhaseBackfilled)` is a high-water mark, so a run whose
			// backfill once finished and was then given more to do still read
			// as complete -- and the whole consistency layer went quiet for a
			// run whose status was `failed` (stokaro/ptah#2649 finding 3).
			SnapshotComplete: run.SnapshotDone,
			CatchUpReached:   guarantee.Complete,
			ConsistencyMode:  string(loaded.Mode),
			SourceMutable:    loaded.Source.Mutable,
			// The lease, which had no producer at all: the field the consistency
			// layer read was set nowhere outside its own tests, so a run whose
			// lease was live reported `every deterministic layer passed` while
			// `SELECT lease_owner, lease_expires > now()` answered otherwise
			// (stokaro/ptah#2738). What the layer does with it is
			// [embedverify.reportLease]'s decision, not this one's.
			LeaseHolder:  run.LeaseOwner,
			LeaseExpires: run.LeaseExpires,
			Now:          time.Now().UTC(),
		})
}

// splitBlocking sorts this report's blocking findings into the ones an operator
// accepted and the ones they did not.
//
// An acceptance names a finding by its summary, and a summary that matches
// nothing is refused rather than ignored. That is the direction that matters:
// an acceptance outlives the finding it was written for -- copied into a
// runbook, a pipeline, a shell history -- and one that silently applies to
// nothing is an operator believing they have looked at something they have not.
func splitBlocking(
	report embedverify.Report, accepting []string,
) (accepted, unaccepted []string, err error) {
	blocking := make(map[string]bool)
	for _, finding := range report.Blocking() {
		blocking[finding.Summary] = true
	}

	// A set, so naming one finding twice is one acceptance. The flag is
	// repeatable and an operator assembling it from a loop or a runbook can
	// easily pass the same summary twice; carrying the repeat into
	// AcceptedFindings would put it in the plan digest, and two invocations
	// that accept exactly the same thing would produce two plans -- so the
	// digest a refusal published would not match the plan the approval is
	// offered against, and the two-step approve flow could not complete.
	seen := make(map[string]bool, len(accepting))
	for _, summary := range accepting {
		if !blocking[summary] {
			return nil, nil, fmt.Errorf(
				"no blocking finding says %q, so there is nothing there to accept",
				summary,
			)
		}
		if seen[summary] {
			continue
		}
		seen[summary] = true
		accepted = append(accepted, summary)
	}

	for summary := range blocking {
		if seen[summary] {
			continue
		}
		unaccepted = append(unaccepted, summary)
	}
	slices.Sort(unaccepted)
	return accepted, unaccepted, nil
}

// BuildCutoverPlan assembles the plan and what is true now.
//
// The moment is a parameter rather than a clock read here, so that a caller
// deciding and then acting -- or asking twice, as a readiness check and a
// cutover do -- measures one instant rather than two that drift apart between
// the plan's staleness check and the record it writes.
func BuildCutoverPlan(
	ctx context.Context, db *sql.DB, store *embedpg.Store,
	loaded embedspec.Loaded, run embedrun.Run, report embedverify.Report,
	accepting []string, now time.Time,
) (embedcutover.Plan, embedcutover.Observed, error) {
	spec := loaded.Spec
	active := ActivePointer(ctx, store, spec.Target.Schema, spec.Target.Table)
	structure, err := embedpg.ReadStructure(ctx, db, spec, active)
	if err != nil {
		return embedcutover.Plan{}, embedcutover.Observed{}, err
	}
	guarantee, err := AssessConsistency(ctx, db, loaded, run)
	if err != nil {
		return embedcutover.Plan{}, embedcutover.Observed{}, err
	}

	objects, err := spec.TargetObjects()
	if err != nil {
		return embedcutover.Plan{}, embedcutover.Observed{}, err
	}
	ready := indexReady(objects, structure)
	accepted, unaccepted, err := splitBlocking(report, accepting)
	if err != nil {
		return embedcutover.Plan{}, embedcutover.Observed{}, err
	}
	plan := embedcutover.Plan{
		Generation: spec.Identity().Digest, Previous: active,
		Schema: spec.Target.Schema, Table: spec.Target.Table, Column: spec.Target.Column,
		Evidence: embedcutover.Evidence{
			// The MEASUREMENT, not the generation identity. This field held
			// report.Generation, which is spec.Identity().Digest -- the same
			// value plan.Generation already carries -- so the plan file an
			// approver signs showed the same sixty-four characters twice under
			// two labels, the plan digest did not move when the report changed,
			// and the refusal "the plan cites no verification report" could
			// never fire (stokaro/ptah#2643). The measurement digest moves with
			// the findings, the counts and the unmeasured layers, so an
			// approval given for this plan stops applying when any of them do.
			VerificationDigest: embedrelease.VerificationOf(
				spec.Identity().Digest, report, nil, time.Time{}).MeasurementDigest(),
			VerificationPassed:   report.Passed(),
			AcceptedFindings:     accepted,
			UnacceptedFindings:   unaccepted,
			ConsistencyMode:      string(loaded.Mode),
			ConsistencyWatermark: run.CatchUpWatermark,
			IndexReady:           ready,
			SourceMutable:        loaded.Source.Mutable,
		},
		// When the EVIDENCE was last established, not when this process
		// started. Two things follow, and both are the point.
		//
		// The digest is stable across invocations, so the operator who runs
		// this to see the plan and runs it again with an approval is approving
		// the plan they read. A wall-clock timestamp made every run a
		// different plan and the approval impossible to give -- which the
		// end-to-end test found the moment it tried.
		//
		// And the age a policy bounds becomes the age of the evidence rather
		// than of the printout. A plan built on a run that last moved two days
		// ago is stale however recently it was rendered.
		PreparedAt: run.UpdatedAt.UTC(),
	}
	// The mode stays, and the reasons it has not been reached travel beside it.
	// Blanking the mode made "you declared none" the diagnostic for "yours has
	// not caught up yet" (stokaro/ptah#2646).
	plan.Evidence.ConsistencyBlockers = guarantee.Blockers
	// The plan's expected previous generation and the observed pointer come
	// from ONE read, because the caller that executes builds and executes in
	// the same process: there is no interval between them for the pointer to
	// move in. What protects a cutover from somebody else's here is the
	// approval, which is bound to a plan built from the pointer as it was.
	//
	// The domain's drift check is for a caller that persists a plan and
	// executes it later. Supplying two separate reads here would be a second
	// answer to a question with one.
	return plan, embedcutover.Observed{
		ActivePointer: active, ConsistencyWatermark: run.CatchUpWatermark,
		IndexReady:  ready,
		Permissions: []embedcutover.Permission{embedcutover.PermissionCutover},
		Now:         now.UTC(),
	}, nil
}

// indexReady reports whether the index this generation needs is there.
//
// It takes the objects the specification derives rather than a flag, because
// "does this generation want an index" and "is that index there" are one
// question about one generation and splitting them into a boolean and a struct
// invites a caller to answer half of it.
//
// A generation that declares no index method needs none, and is ready by
// definition. Reporting the absent index as "not ready" made every such
// generation permanently uncutoverable -- and the refusal named an index the
// specification never asked for, which is the worst kind of diagnostic: it
// sends the operator to configure something they deliberately left out.
func indexReady(objects embedgen.TargetObjects, structure embedverify.Structure) bool {
	if !objects.HasIndex {
		return true
	}
	return structure.IndexExists && structure.IndexValid
}

// AssessConsistency asks the selected mode whether it proved its condition.
func AssessConsistency(
	ctx context.Context, db *sql.DB, loaded embedspec.Loaded, run embedrun.Run,
) (embedcatchup.Guarantee, error) {
	barrier, err := readBarrier(ctx, db, loaded, run)
	if err != nil {
		return embedcatchup.Guarantee{}, err
	}
	return embedcatchup.Assess(loaded.Mode, loaded.Source, barrier,
		embedcatchup.DualWriteEvidence{}, time.Now().UTC()), nil
}

// readBarrier measures the outbox's completion condition.
func readBarrier(
	ctx context.Context, db *sql.DB, loaded embedspec.Loaded, run embedrun.Run,
) (embedcatchup.Barrier, error) {
	if loaded.Mode != embedcatchup.ModeOutbox {
		return embedcatchup.Barrier{}, nil
	}
	outbox, err := embedpg.NewOutbox(db, loaded.Spec)
	if err != nil {
		return embedcatchup.Barrier{}, err
	}
	installed, err := outbox.Installed(ctx)
	if err != nil {
		return embedcatchup.Barrier{}, err
	}
	processed, err := processedCursor(run.CatchUpWatermark)
	if err != nil {
		return embedcatchup.Barrier{}, err
	}
	unprocessed, err := outbox.Unprocessed(ctx, processed)
	if err != nil {
		return embedcatchup.Barrier{}, err
	}
	horizon, err := outbox.Horizon(ctx)
	if err != nil {
		return embedcatchup.Barrier{}, err
	}
	return embedcatchup.Barrier{
		Installed: installed, Snapshot: parseWatermark(run.SnapshotWatermark),
		Processed: processed.Transaction, Horizon: horizon, Unprocessed: unprocessed,
	}, nil
}

// processedCursor reads how far catch-up has got.
//
// An absent watermark is a run whose catch-up has not started, and its cursor
// owes everything. A PRESENT but unreadable one is refused rather than folded
// into the same answer: those are different states, and reporting the second as
// the first counts the whole outbox as unprocessed -- a finished catch-up
// reported as owing every change ever recorded.
func processedCursor(raw string) (embedcatchup.Cursor, error) {
	if raw == "" {
		return embedcatchup.Cursor{}, nil
	}
	return embedcatchup.ParseCursor(raw, "catch-up watermark")
}

// parseWatermark reads a recorded transaction identity, or zero.
//
// Zero for the snapshot boundary is a value Barrier.Reached already reports on
// -- "no snapshot boundary was recorded" -- so an unreadable one arrives there
// as a blocker rather than as a silent default.
func parseWatermark(raw string) uint64 {
	value, err := strconv.ParseUint(raw, 10, 64)
	if err != nil {
		return 0
	}
	return value
}

// ActivePointer reads which generation queries currently read, or nothing.
func ActivePointer(ctx context.Context, store *embedpg.Store, schema, table string) string {
	pointer, err := store.Pointer(ctx, schema, table)
	if err != nil {
		return ""
	}
	return pointer.Active
}
