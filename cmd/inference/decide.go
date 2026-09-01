package inference

import (
	"context"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"go.5x5.cz/ptah/cmd/internal/exitcode"
	"go.5x5.cz/ptah/internal/embedcatchup"
	"go.5x5.cz/ptah/internal/embedcutover"
	"go.5x5.cz/ptah/internal/embedpg"
	"go.5x5.cz/ptah/internal/embedrelease"
	"go.5x5.cz/ptah/internal/embedreport"
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
		// Exit 1 rather than 2, for the same reason `evaluate` does: blocking
		// findings are the verb's ANSWER, and the exit-code reference says so.
		// Two means it could not run, and a rollout gate reading 2 for both
		// cannot tell a generation that failed verification from a database it
		// could not reach (stokaro/ptah#2639).
		return exitcode.New(1, fmt.Errorf(
			"verification found %d blocking findings", len(report.Blocking())))
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
		embedrelease.VerificationOf(
			opened.loaded.Spec.Identity().Digest, report, nil, time.Now().UTC()))
	return publishRecord(ctx, out, options, evidence, record, buildErr, swallowed)
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
	// What the verification did NOT do, which is the half a passing report
	// otherwise hides. Every run carries at least one entry, `status` prints
	// them and the published record carries them, so the verb an operator runs
	// was the only surface that dropped them -- and a run reporting only what
	// passed reads as though every layer was checked, which is exactly what
	// Report.Unmeasured was added to prevent (stokaro/ptah#2649 finding 4).
	for _, unmeasured := range report.Unmeasured {
		lines = append(lines, bullet("not measured: "+unmeasured))
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
	var approval approvalOptions
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
stops applying -- which is the point of it.

--approve takes that digest and --approver the name to record beside it. Where
who approved something has to be evidence rather than a claim, --plan-file
writes the refused plan, "ptah schema approve" signs it with an SSH key, and
--approval verifies the signature and records the principal it belongs to. A
specification setting policy.require_signed_approval refuses the typed form.`,
		SilenceUsage: true,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runCutover(cmd.Context(), cmd.OutOrStdout(), options,
				runID, approval, stabilizeFor, evidence)
		},
	}
	addCommonFlags(cmd, &options)
	cmd.Flags().StringVar(&runID, "run-id", "", "Identifier of the run (required)")
	addApprovalFlags(cmd, &approval)
	cmd.Flags().DurationVar(&stabilizeFor, "stabilize-for", 0,
		"How long the previous generation stays a way back; zero leaves no rollback")
	addEvidenceFlags(cmd.Flags(), &evidence)
	addSubjectFlag(cmd, &evidence)
	return cmd
}

// runCutover builds a plan, decides, and moves the pointer.
func runCutover(
	ctx context.Context, out io.Writer, options commonOptions,
	runID string, authorization approvalOptions,
	stabilizeFor time.Duration, evidence evidenceOptions,
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

	now := time.Now().UTC()
	plan, observed, err := embedreport.BuildCutoverPlan(
		ctx, opened.db, opened.store, opened.loaded, run, report, now)
	if err != nil {
		return err
	}
	identity := cutoverPlanIdentity(plan)
	approval, err := approvalFor(ctx, authorization, identity)
	if err != nil {
		return err
	}
	decision := embedcutover.Decide(plan, opened.loaded.Policy, observed, approval)
	if !decision.Allowed {
		_ = writeLines(out, fmt.Sprintf("plan %s", plan.Short()))
		_ = writePlanFile(out, authorization.planFile, identity)
		return refusal(out, "cutover refused", decision.Blockers)
	}

	// The internal verification establishes what the `verify` verb does -- the
	// same layers over the same rows -- so it records the same phase, and the
	// run is at `verified` before anything moves.
	//
	// Without this, `cut_over` was reachable only after a SEPARATE `verify`
	// run, and nothing said so until after the pointer had already moved: the
	// lifecycle prepare/backfill/catchup/cutover moved the pointer, opened the
	// window, printed "queries now read generation ...", then exited 2 on
	// `caught_up cannot move to cut_over` and published no evidence at all, for
	// a cutover that had happened (stokaro/ptah#2631).
	//
	// After the decision rather than before it, because a REFUSED cutover must
	// leave the run where it found it. Advancing there changes the plan the
	// next invocation builds, so the digest the refusal published stops
	// matching the plan the approval is offered against -- and the two-step
	// approve flow the whole policy rests on cannot complete.
	if err := recordVerified(ctx, options, runID, report); err != nil {
		return err
	}
	if err := opened.store.MovePointer(ctx, embedstore.Pointer{
		TargetSchema: opened.loaded.Spec.Target.Schema,
		TargetTable:  opened.loaded.Spec.Target.Table, Active: plan.Generation,
		Previous: plan.Previous, CutOverAt: now,
		CutOverBy: approverName(approval), PlanDigest: plan.Digest(),
	}, plan.Previous); err != nil {
		return err
	}
	if err := openStabilization(ctx, out, opened, plan, now, stabilizeFor); err != nil {
		return err
	}
	if err := reachPhase(ctx, options, runID, embedrun.PhaseCutOver); err != nil {
		// The pointer has moved; saying only that a phase could not be
		// recorded sends the reader looking for a cutover that did not happen.
		return fmt.Errorf(
			"queries now read generation %s and the run could not record it: %w", plan.Generation, err)
	}
	// The report is not passed: the record cites the value the plan already
	// carries, so there is nothing left here to recompute it from.
	return publishCutover(ctx, out, options, opened, plan,
		approverName(approval), approvalSigned(approval), now, stabilizeFor, evidence)
}

// recordVerified marks a run verified when the verification passed.
//
// A report that found something is left alone: the cutover decision refuses on
// it moments later, and a run recorded as verified because something looked at
// it is the state this whole phase machine exists to prevent.
func recordVerified(
	ctx context.Context, options commonOptions, runID string, report embedverify.Report,
) error {
	if !report.Passed() {
		return nil
	}
	if err := reachPhase(ctx, options, runID, embedrun.PhaseVerified); err != nil {
		return err
	}
	return recordVerification(ctx, options, report.Generation)
}

// publishCutover records what was done, where a registry was named.
//
// After the pointer has moved, and reported rather than fatal for the same
// reason the window is: the cutover happened, and a registry being unreachable
// is not a fact about it.
func publishCutover(
	ctx context.Context, out io.Writer, options commonOptions, opened *session, plan embedcutover.Plan,
	approver string, signed bool, at time.Time,
	stabilizeFor time.Duration, evidence evidenceOptions,
) error {
	if !evidence.destinationNamed() {
		return nil
	}
	cutover := embedrelease.Cutover{
		Generation: plan.Generation, Replaced: plan.Previous,
		Target:     opened.loaded.Spec.Target.Table,
		PlanDigest: plan.Digest(), Approver: approver,
		ApprovalSigned: signed,
		// The value the PLAN cites, not a second computation of it. The two
		// records disagreeing about what "verification digest" means is
		// stokaro/ptah#2643 finding 3, and one value cannot.
		VerificationDigest: plan.Evidence.VerificationDigest,
		Watermark:          plan.Evidence.ConsistencyWatermark,
		CutOverAt:          at,
	}
	if plan.Previous != "" && stabilizeFor > 0 {
		cutover.StabilizeUntil = at.Add(stabilizeFor)
	}
	record, buildErr := embedrelease.NewCutoverRecord(cutover)
	return publishRecord(ctx, out, options, evidence, record, buildErr, swallowed)
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
	// The two cases are separate sentences, because they are different facts
	// and one of them accused the operator of not asking for what they asked
	// for. A first cutover with `--stabilize-for 24h` was told "no
	// stabilization window was asked for" (stokaro/ptah#2647).
	if plan.Previous == "" {
		return writeLines(out, append(lines, bullet(
			"this is the first generation over this target, so there is no previous "+
				"one to keep current and nothing to roll back to"))...)
	}
	if window <= 0 {
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

// cutoverPlanIdentity is what an approver reads and signs.
//
// The facts, and not only the digest: a signature over sixty-four hex
// characters attests to a number nobody could have checked.
func cutoverPlanIdentity(plan embedcutover.Plan) planIdentity {
	return planIdentity{
		operation: "cutover",
		digest:    plan.Digest(),
		lines: []string{
			"generation: " + plan.Generation,
			"replaces: " + plan.Previous,
			"target: " + plan.Schema + "." + plan.Table + "." + plan.Column,
			"verification: " + plan.Evidence.VerificationDigest,
		},
	}
}

// newRetireCommand returns "ptah inference retire".
func newRetireCommand() *cobra.Command {
	var options commonOptions
	var generation string
	var approval approvalOptions
	var dropColumn bool
	var evidence evidenceOptions

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
removal of an index does not authorize the removal of the column.

--plan-file writes the refused plan, "ptah schema approve" signs it, and
--approval verifies the signature and records whose it is. For an operation
nothing can undo, who approved it is worth being evidence rather than a name in
a shell history.`,
		SilenceUsage: true,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runRetire(cmd.Context(), cmd.OutOrStdout(), retireOptions{
				commonOptions: options, generation: generation,
				approval: approval, dropColumn: dropColumn, evidence: evidence,
			})
		},
	}
	addCommonFlags(cmd, &options)
	cmd.Flags().StringVar(&generation, "generation", "", "Identity of the generation to destroy (required)")
	addApprovalFlags(cmd, &approval)
	cmd.Flags().BoolVar(&dropColumn, "drop-column", true,
		"Drop the vector column as well as the index")
	addEvidenceFlags(cmd.Flags(), &evidence)
	addSubjectFlag(cmd, &evidence)
	return cmd
}

// retireOptions are what the retire verb takes.
type retireOptions struct {
	commonOptions
	generation string
	approval   approvalOptions
	dropColumn bool
	evidence   evidenceOptions
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
	// Measured rather than asserted. `DropsIndex: true` promised the operator
	// an index would go whether or not one was there, and the record afterwards
	// said one had (stokaro/ptah#2642).
	hasIndex, err := embedpg.GenerationIndexExists(ctx, opened.db, registered)
	if err != nil {
		return err
	}
	plan := embedcutover.RetirementPlan{
		Generation: options.generation, Schema: opened.loaded.Spec.Target.Schema,
		Table: registered.TargetTable, Column: registered.TargetColumn,
		DropsIndex: hasIndex, DropsColumn: options.dropColumn, RowCount: rows,
	}
	state, observed, err := retirementFacts(ctx, opened, registered, options.generation)
	if err != nil {
		return err
	}
	identity := retirementPlanIdentity(plan)
	approval, err := approvalFor(ctx, options.approval, identity)
	if err != nil {
		return err
	}
	decision := embedcutover.DecideRetirement(plan, state, observed, approval, opened.loaded.Policy)
	if !decision.Allowed {
		_ = writeLines(out, retirementContext(plan, state, rows)...)
		_ = writePlanFile(out, options.approval.planFile, identity)
		return refusal(out, "retirement refused", decision.Blockers)
	}

	if err := embedpg.RetireIndex(ctx, opened.db, registered); err != nil {
		return err
	}
	if plan.DropsColumn {
		if err := embedpg.RetireColumns(ctx, opened.db, registered); err != nil {
			return err
		}
	}
	retiredAt := time.Now().UTC()
	if err := opened.store.RetireGeneration(ctx, options.generation, retiredAt); err != nil {
		return err
	}
	if err := reachTerminalPhase(
		ctx, opened, options.generation, embedrun.PhaseRetired); err != nil {
		return err
	}
	lines := []string{fmt.Sprintf("generation %s is gone, with %d vectors",
		options.generation, rows)}
	// After the registry says so, because the answer is "is this the last one"
	// and this one has to be retired before it can be counted out.
	outboxLine, err := removeOutboxIfLast(ctx, opened, registered)
	if err != nil {
		return err
	}
	if err := writeLines(out, append(lines, outboxLine...)...); err != nil {
		return err
	}
	return publishRetirement(ctx, out, options, plan, identity, approval, rows, retiredAt)
}

// removeOutboxIfLast takes the change capture off the source when the retired
// generation was the last one reading it.
//
// An outbox belongs to a SOURCE TABLE rather than to a generation -- two
// generations over one table share its changes -- so retirement can only remove
// it once nothing is left to feed. Until stokaro/ptah#2649 nothing removed it at
// all: both triggers went on firing on the operator's table for every write,
// and the event table grew with nothing that would ever read or trim it.
//
// It says what it did either way. "Retire removes the generation and its
// bookkeeping" is what the guide promises, and an operator who is told nothing
// cannot tell a removal from the silence that preceded this.
func removeOutboxIfLast(
	ctx context.Context, opened *session, registered embedstore.Generation,
) ([]string, error) {
	if opened.loaded.Mode != embedcatchup.ModeOutbox {
		return nil, nil
	}
	remaining, err := opened.store.LiveGenerationsOver(
		ctx, registered.TargetTable, registered.Identity)
	if err != nil {
		return nil, err
	}
	if remaining > 0 {
		return []string{bullet(fmt.Sprintf(
			"the outbox stays: %d other generation(s) still read %s",
			remaining, registered.TargetTable))}, nil
	}
	outbox, err := embedpg.NewOutbox(opened.db, opened.loaded.Spec)
	if err != nil {
		return nil, err
	}
	if err := outbox.Uninstall(ctx); err != nil {
		return nil, err
	}
	return []string{bullet(fmt.Sprintf(
		"the outbox is gone: its triggers, capture function and event table were "+
			"the last thing Ptah had on %s", registered.TargetTable))}, nil
}

// publishRetirement records what was destroyed, where a destination was named.
//
// This is the one record whose subject cannot be inspected afterwards. Every
// other one here describes something still in the database; this describes an
// absence, so it names the objects rather than counting them.
//
// Reported rather than fatal, for the reason the others are, and more so: the
// vectors are already gone and a failed publication cannot bring them back.
func publishRetirement(
	ctx context.Context, out io.Writer, options retireOptions,
	plan embedcutover.RetirementPlan, identity planIdentity,
	approval *embedcutover.Approval, rows int, at time.Time,
) error {
	if !options.evidence.destinationNamed() {
		return nil
	}
	record, buildErr := embedrelease.NewRetirementRecord(embedrelease.Retirement{
		Generation: plan.Generation,
		Target:     plan.Schema + "." + plan.Table + "." + plan.Column,
		Objects:    retiredObjects(plan),
		Rows:       int64(rows),
		PlanDigest: identity.digest, Approver: approverName(approval),
		ApprovalSigned: approvalSigned(approval),
		RetiredAt:      at,
	})
	return publishRecord(ctx, out, options.commonOptions, options.evidence, record, buildErr, swallowed)
}

// retiredObjects names what the retirement removed.
//
// Named rather than described as a count, because a reader of this record
// cannot go and look: the column either is in the list or it survived, and
// "two objects" answers neither.
func retiredObjects(plan embedcutover.RetirementPlan) []string {
	objects := make([]string, 0, 2)
	if plan.DropsIndex {
		objects = append(objects, "index over "+plan.Schema+"."+plan.Table+"."+plan.Column)
	}
	if plan.DropsColumn {
		objects = append(objects, "column "+plan.Schema+"."+plan.Table+"."+plan.Column)
	}
	return objects
}

// approverName is who authorized it, or nobody.
//
// Nobody is a real answer rather than an impossible one: a policy that requires
// no exact approval allows a cutover with none, and reading the approver off a
// nil approval panicked the process on exactly that path -- the one every test
// here misses, because every specification in the suite requires an approval
// (stokaro/ptah#2068).
func approverName(approval *embedcutover.Approval) string {
	if approval == nil {
		return ""
	}
	return approval.Approver
}

// approvalSigned reports whether the approval was established by a verified
// signature over the plan bytes.
//
// It travels beside approverName rather than inside it because the name and the
// basis for believing it are two facts, and the record needs both: dropping the
// second is what made a signed cutover and a typed one indistinguishable in
// every published field (stokaro/ptah#2643).
func approvalSigned(approval *embedcutover.Approval) bool {
	return approval != nil && approval.Signed
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
	pointer, err := opened.store.Pointer(ctx, registered.TargetSchema, registered.TargetTable)
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

// retirementPlanIdentity is what an approver reads and signs.
//
// It names what is DESTROYED rather than only the generation, because that is
// the difference the retirement digest binds: approving the removal of an index
// does not authorize the removal of the column.
func retirementPlanIdentity(plan embedcutover.RetirementPlan) planIdentity {
	return planIdentity{
		operation: "retire",
		digest:    plan.Digest(),
		lines: []string{
			"generation: " + plan.Generation,
			"target: " + plan.Schema + "." + plan.Table + "." + plan.Column,
			fmt.Sprintf("drops index: %t", plan.DropsIndex),
			fmt.Sprintf("drops column: %t", plan.DropsColumn),
			fmt.Sprintf("rows destroyed: %d", plan.RowCount),
		},
	}
}

// verify opens a session and measures the generation.
//
// The measurement itself is internal/embedreport, which the verify verb, the
// cutover it gates and the readiness a rollout waits on all consume. What is
// here is opening the connection and answering with the run beside the report,
// which is what the two callers in this file go on to use.
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
	// Verification reads the run's watermark and the specification's rows, so a
	// run for another generation measures one generation against another's
	// boundary and reports about neither (stokaro/ptah#2637).
	if err := run.DescribesGeneration(opened.loaded.Spec.Identity().Digest); err != nil {
		return embedverify.Report{}, embedrun.Run{}, err
	}
	report, err := embedreport.VerifyGeneration(ctx, opened.db, opened.store, opened.loaded, run)
	return report, run, err
}
