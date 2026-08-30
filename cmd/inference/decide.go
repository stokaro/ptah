package inference

import (
	"context"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/spf13/cobra"

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

	if err := opened.store.MovePointer(ctx, embedstore.Pointer{
		TargetTable: opened.loaded.Spec.Target.Table, Active: plan.Generation,
		Previous: plan.Previous, CutOverAt: now,
		CutOverBy: approval.Approver, PlanDigest: plan.Digest(),
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
		approval.Approver, now, stabilizeFor, evidence)
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
		Watermark: plan.Evidence.ConsistencyWatermark,
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
				approval: approval, dropColumn: dropColumn,
			})
		},
	}
	addCommonFlags(cmd, &options)
	cmd.Flags().StringVar(&generation, "generation", "", "Identity of the generation to destroy (required)")
	addApprovalFlags(cmd, &approval)
	cmd.Flags().BoolVar(&dropColumn, "drop-column", true,
		"Drop the vector column as well as the index")
	return cmd
}

// retireOptions are what the retire verb takes.
type retireOptions struct {
	commonOptions
	generation string
	approval   approvalOptions
	dropColumn bool
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
	report, err := embedreport.VerifyGeneration(ctx, opened.db, opened.store, opened.loaded, run)
	return report, run, err
}
