package inference

import (
	"context"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"go.5x5.cz/ptah/internal/embedcatchup"
	"go.5x5.cz/ptah/internal/embedcutover"
	"go.5x5.cz/ptah/internal/embedgen"
	"go.5x5.cz/ptah/internal/embedpg"
	"go.5x5.cz/ptah/internal/embedrelease"
	"go.5x5.cz/ptah/internal/embedrun"
	"go.5x5.cz/ptah/internal/embedstore"
	"go.5x5.cz/ptah/internal/embedverify"
)

// newVerifyCommand returns "ptah inference verify".
func newVerifyCommand() *cobra.Command {
	var options commonOptions
	var runID string
	var evidence evidenceOptions

	cmd := &cobra.Command{
		Use:   "verify",
		Short: "Run the deterministic checks a cutover rests on",
		Long: `Check a generation against the source it was built from.

Five layers, and each one is a different way a finished run can be wrong: the
column and its index, whether every in-scope row is covered, whether each vector
was computed from the source as it is NOW, whether the vectors are usable
numbers, and whether the run finished what it started.

Coverage is answered key by key. A source count matching a target count is
satisfied by a corpus that missed a thousand rows and invented a thousand
others.`,
		SilenceUsage: true,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runVerify(cmd.Context(), cmd.OutOrStdout(), options, runID, evidence)
		},
	}
	addCommonFlags(cmd, &options)
	cmd.Flags().StringVar(&runID, "run-id", "", "Identifier of the run (required)")
	addEvidenceFlags(cmd.Flags(), &evidence)
	addSubjectFlag(cmd, &evidence)
	return cmd
}

// runVerify reads both sides, reports, and records a pass.
//
// Recording it is not bookkeeping. A rollback rests on a generation having been
// verified, and nothing else in the lifecycle can say that it was: a generation
// somebody asserts is fine is not a generation anybody measured. The record is
// written only on a pass, so a failing verification leaves the last passing one
// standing rather than replacing it with a newer lie.
func runVerify(
	ctx context.Context, out io.Writer, options commonOptions, runID string, evidence evidenceOptions,
) error {
	report, _, err := verify(ctx, options, runID)
	if err != nil {
		return err
	}
	if err := printReport(out, report); err != nil {
		return err
	}
	if err := publishVerification(ctx, out, options, report, evidence); err != nil {
		return err
	}
	if !report.Passed() {
		// Published either way, and that is the point: a verification that
		// found something is the evidence somebody will want, and a registry
		// holding only the passes is a record of nothing.
		return fmt.Errorf("verification found %d blocking findings", len(report.Blocking()))
	}
	if err := reachPhase(ctx, options, runID, embedrun.PhaseVerified); err != nil {
		return err
	}
	return recordVerification(ctx, options, report.Generation)
}

// publishVerification writes the report to a registry where one was named.
func publishVerification(
	ctx context.Context, out io.Writer, options commonOptions,
	report embedverify.Report, evidence evidenceOptions,
) error {
	if !evidence.destinationNamed() {
		return nil
	}
	opened, err := open(ctx, options)
	if err != nil {
		return err
	}
	defer opened.close()
	record, buildErr := embedrelease.NewVerificationRecord(
		verificationRecord(opened.loaded.Spec, report, nil, time.Now().UTC()))
	return publishRecord(ctx, out, options.spec.plainHTTP, evidence, record, buildErr)
}

// recordVerification writes the pass onto the generation.
func recordVerification(ctx context.Context, options commonOptions, generation string) error {
	opened, err := open(ctx, options)
	if err != nil {
		return err
	}
	defer opened.close()
	return opened.store.RecordVerification(ctx, generation, time.Now().UTC())
}

// verify runs every deterministic layer against the live generation.
func verify(
	ctx context.Context, options commonOptions, runID string,
) (embedverify.Report, embedrun.Run, error) {
	if runID == "" {
		return embedverify.Report{}, embedrun.Run{}, fmt.Errorf("--run-id is required")
	}
	opened, err := open(ctx, options)
	if err != nil {
		return embedverify.Report{}, embedrun.Run{}, err
	}
	defer opened.close()

	run, err := opened.store.Run(ctx, runID)
	if err != nil {
		return embedverify.Report{}, embedrun.Run{}, err
	}
	spec := opened.loaded.Spec
	active := activePointer(ctx, opened, spec.Target.Table)
	structure, err := embedpg.ReadStructure(ctx, opened.db, spec, active)
	if err != nil {
		return embedverify.Report{}, run, err
	}
	source, target, err := embedpg.ReadVerificationRows(ctx, opened.db, spec)
	if err != nil {
		return embedverify.Report{}, run, err
	}
	guarantee, err := assessConsistency(ctx, opened, run)
	if err != nil {
		return embedverify.Report{}, run, err
	}

	objects, err := spec.TargetObjects()
	if err != nil {
		return embedverify.Report{}, run, err
	}
	report := embedverify.Verify(
		embedverify.Expectation{
			Generation:    spec.Identity().Digest,
			ColumnType:    objects.Column.Type,
			Dimension:     spec.Model.ReportedDimension,
			IndexMethod:   objects.Index.Type,
			OperatorClass: objects.Index.Operator,
			RequireIndex:  objects.HasIndex && run.Phase != embedrun.PhaseBackfilling,
		},
		structure, source, target,
		embedverify.RunState{
			SnapshotComplete:    run.Phase != embedrun.PhaseBackfilling,
			CatchUpReached:      guarantee.Complete,
			ConsistencyMode:     string(opened.loaded.Mode),
			SourceMutable:       opened.loaded.Source.Mutable,
			UnreconciledBatches: 0,
		})
	return report, run, nil
}

// assessConsistency asks the selected mode whether it proved its condition.
func assessConsistency(
	ctx context.Context, opened *session, run embedrun.Run,
) (embedcatchup.Guarantee, error) {
	barrier, err := readBarrier(ctx, opened, run)
	if err != nil {
		return embedcatchup.Guarantee{}, err
	}
	return embedcatchup.Assess(opened.loaded.Mode, opened.loaded.Source, barrier,
		embedcatchup.DualWriteEvidence{}, time.Now().UTC()), nil
}

// readBarrier measures the outbox's completion condition.
func readBarrier(
	ctx context.Context, opened *session, run embedrun.Run,
) (embedcatchup.Barrier, error) {
	if opened.loaded.Mode != embedcatchup.ModeOutbox {
		return embedcatchup.Barrier{}, nil
	}
	outbox, err := embedpg.NewOutbox(opened.db, opened.loaded.Spec)
	if err != nil {
		return embedcatchup.Barrier{}, err
	}
	installed, err := outbox.Installed(ctx)
	if err != nil {
		return embedcatchup.Barrier{}, err
	}
	processed := parseWatermark(run.CatchUpWatermark)
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
		Processed: processed, Horizon: horizon, Unprocessed: unprocessed,
	}, nil
}

// parseWatermark reads a recorded transaction identity, or zero.
func parseWatermark(raw string) uint64 {
	value, err := strconv.ParseUint(raw, 10, 64)
	if err != nil {
		return 0
	}
	return value
}

// activePointer reads which generation queries currently read, or nothing.
func activePointer(ctx context.Context, opened *session, table string) string {
	pointer, err := opened.store.Pointer(ctx, table)
	if err != nil {
		return ""
	}
	return pointer.Active
}

// printReport renders a verification report.
func printReport(out io.Writer, report embedverify.Report) error {
	lines := []string{fmt.Sprintf("generation %s: %d source rows, %d target rows",
		report.Generation, report.SourceRows, report.TargetRows)}
	for _, finding := range report.Findings {
		lines = append(lines, bullet(fmt.Sprintf("[%s/%s] %s",
			finding.Layer, finding.Severity, finding.Summary)))
		if len(finding.Keys) > 0 {
			lines = append(lines, "      keys: "+joinKeys(finding.Keys))
		}
	}
	if len(report.Findings) == 0 {
		lines = append(lines, bullet("every deterministic layer passed"))
	}
	return writeLines(out, lines...)
}

// joinKeys renders the bounded key list a finding carries.
func joinKeys(keys []string) string {
	return strings.Join(keys, ", ")
}

// newCutoverCommand returns "ptah inference cutover".
func newCutoverCommand() *cobra.Command {
	var options commonOptions
	var runID string
	var approvalDigest string
	var approver string
	var stabilizeFor time.Duration
	var evidence evidenceOptions

	cmd := &cobra.Command{
		Use:   "cutover",
		Short: "Make the new generation the one queries read",
		Long: `Move the active pointer to a verified generation.

The plan is built from evidence and checked against what is true NOW: a
verification report by digest, the consistency mode's own completion condition,
whether the index is ready, and where the pointer actually points. Somebody else
cutting over in the meantime is refused rather than overwritten.

Where the specification requires an approval, it binds to the plan's exact
digest. Any change to the evidence produces a different plan and the approval
stops applying -- which is the point of it.`,
		SilenceUsage: true,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runCutover(cmd.Context(), cmd.OutOrStdout(), options,
				runID, approvalDigest, approver, stabilizeFor, evidence)
		},
	}
	addCommonFlags(cmd, &options)
	cmd.Flags().StringVar(&runID, "run-id", "", "Identifier of the run (required)")
	cmd.Flags().StringVar(&approvalDigest, "approve", "",
		"Plan digest this cutover is approved for; run without it to see the digest")
	cmd.Flags().StringVar(&approver, "approver", "", "Who approved it")
	cmd.Flags().DurationVar(&stabilizeFor, "stabilize-for", 0,
		"How long the previous generation stays a way back; zero leaves no rollback")
	addEvidenceFlags(cmd.Flags(), &evidence)
	addSubjectFlag(cmd, &evidence)
	return cmd
}

// runCutover builds a plan, decides, and moves the pointer.
func runCutover(
	ctx context.Context, out io.Writer, options commonOptions,
	runID, approvalDigest, approver string, stabilizeFor time.Duration, evidence evidenceOptions,
) error {
	report, run, err := verify(ctx, options, runID)
	if err != nil {
		return err
	}
	opened, err := open(ctx, options)
	if err != nil {
		return err
	}
	defer opened.close()

	plan, observed, err := buildCutoverPlan(ctx, opened, run, report)
	if err != nil {
		return err
	}
	approval := approvalFrom(plan, approvalDigest, approver)
	decision := embedcutover.Decide(plan, opened.loaded.Policy, observed, approval)
	if !decision.Allowed {
		_ = writeLines(out, fmt.Sprintf("plan %s", plan.Short()))
		return refusal(out, "cutover refused", decision.Blockers)
	}

	now := time.Now().UTC()
	if err := opened.store.MovePointer(ctx, embedstore.Pointer{
		TargetTable: opened.loaded.Spec.Target.Table, Active: plan.Generation,
		Previous: plan.Previous, CutOverAt: now,
		CutOverBy: approver, PlanDigest: plan.Digest(),
	}, plan.Previous); err != nil {
		return err
	}
	if err := openStabilization(ctx, out, opened, plan, now, stabilizeFor); err != nil {
		return err
	}
	if err := reachPhase(ctx, options, runID, embedrun.PhaseCutOver); err != nil {
		return err
	}
	return publishCutover(ctx, out, options.spec.plainHTTP, opened, plan, report,
		approver, now, stabilizeFor, evidence)
}

// publishCutover records what was done, where a registry was named.
//
// After the pointer has moved, and reported rather than fatal for the same
// reason the window is: the cutover happened, and a registry being unreachable
// is not a fact about it.
func publishCutover(
	ctx context.Context, out io.Writer, plainHTTP bool, opened *session, plan embedcutover.Plan,
	report embedverify.Report, approver string, at time.Time,
	stabilizeFor time.Duration, evidence evidenceOptions,
) error {
	if !evidence.destinationNamed() {
		return nil
	}
	cutover := embedrelease.Cutover{
		Generation: plan.Generation, Replaced: plan.Previous,
		Target:     opened.loaded.Spec.Target.Table,
		PlanDigest: plan.Digest(), Approver: approver,
		VerificationDigest: verificationRecord(
			opened.loaded.Spec, report, nil, at).Digest(),
		CutOverAt: at,
	}
	if plan.Previous != "" && stabilizeFor > 0 {
		cutover.StabilizeUntil = at.Add(stabilizeFor)
	}
	record, buildErr := embedrelease.NewCutoverRecord(cutover)
	return publishRecord(ctx, out, plainHTTP, evidence, record, buildErr)
}

// openStabilization starts the window in which the previous generation is still
// a way back.
//
// Phase K. The old generation stops receiving changes the moment queries stop
// reading it, so what makes it a rollback target is somebody keeping it
// current -- and the window is a promise to do that, recorded where the
// eligibility check reads it. A zero window records nothing, which is the
// honest answer for an operator who did not ask for one: the previous
// generation is immediately not a way back, and rollback says so.
func openStabilization(
	ctx context.Context, out io.Writer, opened *session,
	plan embedcutover.Plan, now time.Time, window time.Duration,
) error {
	lines := []string{fmt.Sprintf("queries now read generation %s (plan %s)",
		plan.Generation, plan.Short())}
	if plan.Previous == "" || window <= 0 {
		return writeLines(out, append(lines, bullet(
			"no stabilization window was asked for, so nothing is keeping the previous "+
				"generation current and there is no rollback to it"))...)
	}
	until := now.Add(window)
	err := opened.store.Maintain(ctx, plan.Previous, until)
	if errorsIs(err, embedstore.ErrNotFound) || errorsIs(err, embedstore.ErrRetired) {
		// The pointer names a generation the registry does not have, or has as
		// destroyed. The cutover itself already happened -- failing here would
		// leave an operator with a moved pointer and an error, which is the
		// worst of both. Say what could not be recorded and why it matters.
		return writeLines(out, append(lines, bullet(fmt.Sprintf(
			"no window was opened over %s: %v, so there is no rollback to it",
			plan.Previous, err)))...)
	}
	if err != nil {
		return err
	}
	return writeLines(out, append(lines, bullet(fmt.Sprintf(
		"generation %s stays a way back until %s, for as long as catch-up keeps feeding it",
		plan.Previous, until.Format(time.RFC3339))))...)
}

// buildCutoverPlan assembles the plan and what is true now.
func buildCutoverPlan(
	ctx context.Context, opened *session, run embedrun.Run, report embedverify.Report,
) (embedcutover.Plan, embedcutover.Observed, error) {
	spec := opened.loaded.Spec
	active := activePointer(ctx, opened, spec.Target.Table)
	structure, err := embedpg.ReadStructure(ctx, opened.db, spec, active)
	if err != nil {
		return embedcutover.Plan{}, embedcutover.Observed{}, err
	}
	guarantee, err := assessConsistency(ctx, opened, run)
	if err != nil {
		return embedcutover.Plan{}, embedcutover.Observed{}, err
	}

	objects, err := spec.TargetObjects()
	if err != nil {
		return embedcutover.Plan{}, embedcutover.Observed{}, err
	}
	ready := indexReady(objects, structure)
	plan := embedcutover.Plan{
		Generation: spec.Identity().Digest, Previous: active,
		Schema: spec.Target.Schema, Table: spec.Target.Table, Column: spec.Target.Column,
		Evidence: embedcutover.Evidence{
			VerificationDigest:   report.Generation,
			VerificationPassed:   report.Passed(),
			ConsistencyMode:      string(opened.loaded.Mode),
			ConsistencyWatermark: run.CatchUpWatermark,
			IndexReady:           ready,
			SourceMutable:        opened.loaded.Source.Mutable,
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
	if !guarantee.Complete {
		plan.Evidence.ConsistencyMode = ""
	}
	// The plan's expected previous generation and the observed pointer come
	// from ONE read, because this caller builds and executes in the same
	// process: there is no interval between them for the pointer to move in.
	// What protects a cutover from somebody else's here is the approval, which
	// is bound to a plan built from the pointer as it was.
	//
	// The domain's drift check is for a caller that persists a plan and
	// executes it later. Supplying two separate reads here would be a second
	// answer to a question with one.
	return plan, embedcutover.Observed{
		ActivePointer: active, ConsistencyWatermark: run.CatchUpWatermark,
		IndexReady:  ready,
		Permissions: []embedcutover.Permission{embedcutover.PermissionCutover},
		Now:         time.Now().UTC(),
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

// approvalFrom builds the approval a caller supplied, or none.
func approvalFrom(plan embedcutover.Plan, digest, approver string) *embedcutover.Approval {
	if digest == "" {
		return nil
	}
	return &embedcutover.Approval{
		PlanDigest: expandDigest(plan, digest), Approver: approver, GrantedAt: time.Now().UTC(),
	}
}

// expandDigest accepts the short digest a person reads off the terminal.
//
// A full digest is sixty-four characters and nobody retypes one correctly.
// Matching the short form against THIS plan is safe because it is only ever
// compared to this plan: a short form that does not match leaves the caller's
// own string, which then fails the comparison and names both.
func expandDigest(plan embedcutover.Plan, digest string) string {
	if digest == plan.Short() {
		return plan.Digest()
	}
	return digest
}

// newRetireCommand returns "ptah inference retire".
func newRetireCommand() *cobra.Command {
	var options commonOptions
	var generation string
	var approvalDigest string
	var approver string
	var dropColumn bool

	cmd := &cobra.Command{
		Use:   "retire",
		Short: "Destroy a generation, which cannot be undone",
		Long: `Remove a generation's vectors and the objects that hold them.

This is the one operation here that cannot be undone. A cutover that was wrong
is a cutover back; a retirement that was wrong is a backfill that has to run
again from nothing.

It is refused while queries read the generation, and while a live generation can
still be rolled back to it. Where the specification requires an approval, the
approval binds to what is DESTROYED rather than to what is named: approving the
removal of an index does not authorize the removal of the column.`,
		SilenceUsage: true,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runRetire(cmd.Context(), cmd.OutOrStdout(), retireOptions{
				commonOptions: options, generation: generation,
				approvalDigest: approvalDigest, approver: approver, dropColumn: dropColumn,
			})
		},
	}
	addCommonFlags(cmd, &options)
	cmd.Flags().StringVar(&generation, "generation", "", "Identity of the generation to destroy (required)")
	cmd.Flags().StringVar(&approvalDigest, "approve", "",
		"Plan digest this retirement is approved for; run without it to see the digest")
	cmd.Flags().StringVar(&approver, "approver", "", "Who approved it")
	cmd.Flags().BoolVar(&dropColumn, "drop-column", true,
		"Drop the vector column as well as the index")
	return cmd
}

// retireOptions are what the retire verb takes.
type retireOptions struct {
	commonOptions
	generation     string
	approvalDigest string
	approver       string
	dropColumn     bool
}

// reachPhase records how far a verb got, on its own connection.
//
// Its own, because the verbs call it after the work is done, when the session
// they did it through may already be closed. It is a second connection on a
// path that has just written to the database, and it is the alternative to
// threading a session through every return.
func reachPhase(
	ctx context.Context, options commonOptions, runID string, to embedrun.Phase,
) error {
	opened, err := open(ctx, options)
	if err != nil {
		return err
	}
	defer opened.close()
	return opened.store.ReachPhase(ctx, runID, to)
}

// runRetire decides and destroys.
func runRetire(ctx context.Context, out io.Writer, options retireOptions) error {
	if options.generation == "" {
		return fmt.Errorf("--generation is required")
	}
	opened, err := open(ctx, options.commonOptions)
	if err != nil {
		return err
	}
	defer opened.close()

	registered, err := opened.store.Generation(ctx, options.generation)
	if err != nil {
		return err
	}
	rows, err := embedpg.CountGenerationRows(ctx, opened.db, registered)
	if err != nil {
		return err
	}
	plan := embedcutover.RetirementPlan{
		Generation: options.generation, Schema: opened.loaded.Spec.Target.Schema,
		Table: registered.TargetTable, Column: registered.TargetColumn,
		DropsIndex: true, DropsColumn: options.dropColumn, RowCount: rows,
	}
	state, observed, err := retirementFacts(ctx, opened, registered, options.generation)
	if err != nil {
		return err
	}
	decision := embedcutover.DecideRetirement(plan, state, observed,
		retirementApproval(plan, options), opened.loaded.Policy)
	if !decision.Allowed {
		_ = writeLines(out, retirementContext(plan, state, rows)...)
		return refusal(out, "retirement refused", decision.Blockers)
	}

	if err := embedpg.RetireIndex(ctx, opened.db, opened.loaded.Spec, registered); err != nil {
		return err
	}
	if plan.DropsColumn {
		if err := embedpg.RetireColumns(ctx, opened.db, registered); err != nil {
			return err
		}
	}
	if err := opened.store.RetireGeneration(ctx, options.generation, time.Now().UTC()); err != nil {
		return err
	}
	return writeLines(out, fmt.Sprintf("generation %s is gone, with %d vectors",
		options.generation, rows))
}

// retirementContext says what was measured, beside what was decided.
//
// The rollback dependency is the line worth printing even when it does not
// block. A generation the pointer records as somebody's way back, whose way
// back is not currently eligible, is a fact an operator is about to act on:
// destroying it is fine today and would not have been last week, and the
// difference is not in the refusal.
func retirementContext(
	plan embedcutover.RetirementPlan, state embedcutover.RetirementState, rows int,
) []string {
	lines := []string{fmt.Sprintf("plan %s (%d rows)", plan.Short(), rows)}
	if state.IsRollbackTargetFor == "" {
		return lines
	}
	if state.RollbackEligible {
		return append(lines, bullet(fmt.Sprintf(
			"generation %q records this as its way back, and that way back is still eligible",
			state.IsRollbackTargetFor)))
	}
	return append(lines, bullet(fmt.Sprintf(
		"generation %q records this as its way back, and that way back is no longer eligible, "+
			"so nothing here preserves it", state.IsRollbackTargetFor)))
}

// retirementFacts reads what is true about a generation somebody wants gone.
//
// IsActive is deliberately left alone and the pointer answers instead. Both
// fields say whether queries read this generation, and this caller has one
// source for that -- filling in two would be two answers to one question, and
// whichever the decision happened to read would be the only one anybody could
// test.
//
// The rollback dependency is the field that matters here. A generation the
// pointer records as the one before the active generation is somebody's way
// back, and destroying it while that is still true removes a rollback the
// operator believes they have.
func retirementFacts(
	ctx context.Context, opened *session, registered embedstore.Generation, generation string,
) (embedcutover.RetirementState, embedcutover.Observed, error) {
	pointer, err := opened.store.Pointer(ctx, registered.TargetTable)
	if err != nil && !errorsIs(err, embedstore.ErrNotFound) {
		return embedcutover.RetirementState{}, embedcutover.Observed{}, err
	}

	state := embedcutover.RetirementState{}
	if pointer.Previous == generation {
		state.IsRollbackTargetFor = pointer.Active
		eligibility, err := rollbackEligibility(ctx, opened, generation, pointer)
		if err != nil {
			return embedcutover.RetirementState{}, embedcutover.Observed{}, err
		}
		state.RollbackEligible = eligibility.Eligible
	}
	return state, embedcutover.Observed{
		ActivePointer: pointer.Active,
		Permissions:   []embedcutover.Permission{embedcutover.PermissionRetire},
		Now:           time.Now().UTC(),
	}, nil
}

// rollbackEligibility measures whether a previous generation is still a place
// to go back to.
func rollbackEligibility(
	ctx context.Context, opened *session, generation string, pointer embedstore.Pointer,
) (embedcutover.RollbackEligibility, error) {
	state, err := embedpg.RollbackState(ctx, opened.db, opened.loaded.Spec, generation, pointer)
	if err != nil {
		return embedcutover.RollbackEligibility{}, err
	}
	policy, err := rollbackPolicy(opened, 0)
	if err != nil {
		return embedcutover.RollbackEligibility{}, err
	}
	return embedcutover.EvaluateRollback(policy, state,
		embedcutover.Observed{Now: time.Now().UTC()}), nil
}

// retirementApproval builds the approval a caller supplied, or none.
func retirementApproval(
	plan embedcutover.RetirementPlan, options retireOptions,
) *embedcutover.Approval {
	if options.approvalDigest == "" {
		return nil
	}
	digest := options.approvalDigest
	if digest == plan.Short() {
		digest = plan.Digest()
	}
	return &embedcutover.Approval{
		PlanDigest: digest, Approver: options.approver, GrantedAt: time.Now().UTC(),
	}
}
