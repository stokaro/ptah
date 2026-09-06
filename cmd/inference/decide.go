package inference

import (
	"context"
	"fmt"
	"io"
	"path/filepath"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"ptah.run/cmd/internal/exitcode"
	"ptah.run/internal/embedcutover"
	"ptah.run/internal/embedpg"
	"ptah.run/internal/embedrelease"
	"ptah.run/internal/embedreport"
	"ptah.run/internal/embedrun"
	"ptah.run/internal/embedstore"
	"ptah.run/internal/embedverify"
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
	return publishVerificationFor(ctx, out, options, opened, report, evidence, time.Now().UTC())
}

// publishVerificationFor is publishVerification for a caller that already holds
// the session, and that knows when the measurement it is publishing was taken.
//
// `cutover` is that caller. It re-verifies before it moves the pointer and rests
// its decision on THAT report, which is a different measurement from the one
// `verify` published whenever the source moved between the two: measured in
// stokaro/ptah#2643, the only published report for a generation said 3 source
// rows and 3 target rows while the cutover's own re-verification saw 4 and 4,
// and the counts, verdict and findings it actually rested on were recorded
// nowhere (stokaro/ptah#2656).
//
// It publishes whenever a destination is named, rather than only when the
// measurement differs from the last published one, and that is a decision
// rather than the easier branch:
//
//   - the store records a verification's TIME, not its digest, so "differs from
//     what verify published" is not knowable here without new bookkeeping --
//     and bookkeeping that decides whether evidence exists is a worse thing to
//     get wrong than a duplicate record;
//   - the operator named a destination. Withholding a record they asked for
//     because we judged it redundant is the silent omission this codebase
//     refuses everywhere else;
//   - `verify` already settled the same question the same way: it publishes a
//     failing report too, because "a registry holding only the passes is a
//     record of nothing".
//
// The cost is real and bounded: when the two measurements are identical the
// registry holds two records that differ only in `measured_at`. They are the
// same measurement -- MeasurementDigest excludes the timestamp -- so the cutover
// record's citation resolves against either, which is exactly what
// stokaro/ptah#2655 made well-defined.
func publishVerificationFor(
	ctx context.Context, out io.Writer, options commonOptions, opened *session,
	report embedverify.Report, evidence evidenceOptions, measuredAt time.Time,
) error {
	if !evidence.destinationNamed() {
		return nil
	}
	record, buildErr := embedrelease.NewVerificationRecord(
		embedrelease.VerificationOf(
			opened.loaded.Spec.Identity().Digest, report, nil, measuredAt))
	return publishRecord(ctx, out, options, evidence, record, buildErr, swallowed)
}

// beside moves a record's FILE destination off the one the caller's other record
// will use, and leaves the registry destinations alone.
//
// It is applied at the CUTOVER call site rather than inside
// [publishVerificationFor], because only that verb writes two records. Putting
// it in the shared helper moved `verify --evidence-file x.json` to
// `x.verification.json` as well -- a verb that writes one record, to the path
// the operator named. CI caught it; the cutover test alone did not, because it
// never runs `verify`.
//
// `--evidence-file` overwrites, and until this change no verb wrote two records
// in one run, so nothing had to answer the question. A cutover writing its
// verification and then its cutover record to one path would have left only the
// second, and the existing evidence test -- which asserts the cutover record --
// would have passed against exactly that loss.
//
// A registry holds both without help: two pushes, two artifacts, and the cutover
// record's citation picks out the one it rested on. Only the single file needs
// somewhere else to go, and it says on standard output where that was, so the
// derived name is discoverable rather than a convention to know.
func beside(evidence evidenceOptions) evidenceOptions {
	if evidence.writeTo == "" {
		return evidence
	}
	extension := filepath.Ext(evidence.writeTo)
	evidence.writeTo = strings.TrimSuffix(evidence.writeTo, extension) + ".verification" + extension
	return evidence
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

// targetShapeText explains a target-row count that is not a vector count.
//
// The header printed the walk's target-row total on its own, and a reader took
// it for the number of vectors: `2 source rows, 3 target rows` sat beside a
// column holding two, after catch-up tombstoned a row through Ptah's own verbs
// (stokaro/ptah#2742). A tombstone occupies a position and holds nothing, and
// so does a skip.
//
// The total is kept rather than replaced. It is the shape the verification
// record stores, and a header that quietly reported a different number would
// disagree with the evidence file beside it. What was missing is why the two
// differ, so the breakdown is appended and nothing is taken away.
//
// It names the deliberate absences and is not a partition. A row nothing ever
// wrote holds no vector and carries neither flag, so it is part of the
// difference and not part of the breakdown -- the coverage layer reports it as
// a finding, which is where a reader should be sent for it.
//
// Silent when there is nothing to explain: on a healthy generation every target
// row holds a vector, and "(2 with a vector)" after "2 target rows" is noise.
func targetShapeText(report embedverify.Report) string {
	if report.TargetVectors == report.TargetRows {
		return ""
	}
	parts := []string{fmt.Sprintf("%d with a vector", report.TargetVectors)}
	if report.Tombstones > 0 {
		parts = append(parts, fmt.Sprintf("%d tombstoned", report.Tombstones))
	}
	if report.SkippedTargets > 0 {
		parts = append(parts, fmt.Sprintf("%d skipped", report.SkippedTargets))
	}
	return " (" + strings.Join(parts, ", ") + ")"
}

// printReport renders a verification report.
func printReport(out io.Writer, report embedverify.Report) error {
	lines := []string{fmt.Sprintf("generation %s: %d source rows, %d target rows%s",
		report.Generation, report.SourceRows, report.TargetRows, targetShapeText(report))}
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
//
// Each key goes through [embedverify.RenderKey] first. A key's components are
// joined internally with a control character, so a composite key printed as it
// is stored is one a terminal swallows: `(acme, 2)` and `(globex, 1)` came out
// as `acme2` and `globex1`, which is neither copy-pasteable nor unambiguous,
// on the only line telling an operator which rows to act on
// (stokaro/ptah#2649 finding 2).
func joinKeys(keys []string) string {
	rendered := make([]string, 0, len(keys))
	for _, key := range keys {
		rendered = append(rendered, embedverify.RenderKey(key))
	}
	return strings.Join(rendered, ", ")
}

// newCutoverCommand returns "ptah inference cutover".
func newCutoverCommand() *cobra.Command {
	var options commonOptions
	var runID string
	var approval approvalOptions
	var stabilizeFor time.Duration
	var accepting []string
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
				runID, approval, stabilizeFor, accepting, evidence)
		},
	}
	addCommonFlags(cmd, &options)
	cmd.Flags().StringVar(&runID, "run-id", "", "Identifier of the run (required)")
	addApprovalFlags(cmd, &approval)
	cmd.Flags().DurationVar(&stabilizeFor, "stabilize-for", 0,
		"How long the previous generation stays a way back; zero leaves no rollback")
	cmd.Flags().StringArrayVar(&accepting, "accept-finding", nil,
		"A blocking finding, by its exact summary, to cut over despite. Repeatable. "+
			"Refused unless policy.allow_accepted_findings is set, and refused when the "+
			"summary matches no blocking finding")
	addEvidenceFlags(cmd.Flags(), &evidence)
	addSubjectFlag(cmd, &evidence)
	return cmd
}

// runCutover builds a plan, decides, and moves the pointer.
func runCutover(
	ctx context.Context, out io.Writer, options commonOptions,
	runID string, authorization approvalOptions,
	stabilizeFor time.Duration, accepting []string, evidence evidenceOptions,
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
		ctx, opened.db, opened.store, opened.loaded, run, report, accepting, now)
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
	// run is at `verified` before anything moves. The store binds the pointer
	// move to this exact run and records `cut_over` in the same transaction.
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
	move, err := opened.store.MovePointerWithMaintenance(ctx, embedstore.Pointer{
		TargetSchema: opened.loaded.Spec.Target.Schema,
		TargetTable:  opened.loaded.Spec.Target.Table, Active: plan.Generation,
		Previous:  plan.Previous,
		CutOverBy: approverName(approval), PlanDigest: plan.Digest(),
	}, plan.Previous, runID, stabilizeFor)
	if err != nil {
		return err
	}
	if err := reportStabilization(out, plan, move.PreviousMaintainedUntil); err != nil {
		return err
	}
	// The measurement this cutover rested on, published before the record that
	// cites it, so a reader following the citation finds the report rather than
	// a digest of something the registry does not hold (stokaro/ptah#2656).
	//
	// After the pointer moved rather than before the decision: a REFUSED
	// cutover rested on nothing and publishes nothing, which is what the
	// refusal path above already does. A publication failure is swallowed here
	// for the reason publishCutover swallows one -- the pointer has moved, and
	// failing the verb now would report a cutover that did not happen.
	if err := publishVerificationFor(
		ctx, out, options, opened, report, beside(evidence), now); err != nil {
		return err
	}
	// The report is not passed: the record cites the value the plan already
	// carries, so there is nothing left here to recompute it from.
	return publishCutover(ctx, out, options, opened, plan,
		approverName(approval), approvalSigned(approval),
		move.CutOverAt, move.PreviousMaintainedUntil, evidence)
}

// recordVerified marks a run verified when the cutover gate was satisfied.
//
// Called only after the decision allowed the cutover, so reaching here means
// the gate was passed -- by a clean report, or by every blocking finding having
// been named in an acceptance the policy permits and the plan digest binds.
// Both are the run standing where the lifecycle calls verified; the difference
// between them is what the VERIFICATION record says, which is why that record
// is written for a clean report only.
//
// Keying the phase on report.Passed() alone leaves an accepted-findings cutover
// stuck at `caught_up`: the pointer moves, and then the phase it cannot reach
// fails the verb — the same shape stokaro/ptah#2631 fixed, reopened by the path
// that makes acceptance possible at all.
func recordVerified(
	ctx context.Context, options commonOptions, runID string, report embedverify.Report,
) error {
	if err := reachPhase(ctx, options, runID, embedrun.PhaseVerified); err != nil {
		return err
	}
	if !report.Passed() {
		// Accepted rather than clean. The plan carries what was accepted and
		// the pointer carries the plan digest, so the acceptance is recorded;
		// a verification record saying this generation verified would say
		// something no layer measured.
		return nil
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
	approver string, signed bool, at, stabilizeUntil time.Time, evidence evidenceOptions,
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
	if plan.Previous != "" && !stabilizeUntil.IsZero() {
		cutover.StabilizeUntil = stabilizeUntil
	}
	record, buildErr := embedrelease.NewCutoverRecord(cutover)
	return publishRecord(ctx, out, options, evidence, record, buildErr, swallowed)
}

// reportStabilization says which window the atomic pointer move opened over the
// previous generation.
//
// Phase K. The old generation stops receiving changes the moment queries stop
// reading it, so what makes it a rollback target is somebody keeping it
// current -- and the window is a promise to do that, recorded where the
// eligibility check reads it. A zero window records nothing, which is the
// honest answer for an operator who did not ask for one: the previous
// generation is immediately not a way back, and rollback says so.
func reportStabilization(
	out io.Writer, plan embedcutover.Plan, stabilizeUntil time.Time,
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
	if stabilizeUntil.IsZero() {
		return writeLines(out, append(lines, bullet(
			"no stabilization window was asked for, so nothing is keeping the previous "+
				"generation current and there is no rollback to it"))...)
	}
	return writeLines(out, append(lines, bullet(fmt.Sprintf(
		"generation %s stays a way back until %s, for as long as catch-up keeps feeding it",
		plan.Previous, stabilizeUntil.Format(time.RFC3339))))...)
}

// cutoverPlanIdentity is what an approver reads and signs.
//
// The facts, and not only the digest: a signature over sixty-four hex
// characters attests to a number nobody could have checked. Which facts is
// [embedcutover.Plan.IdentityLines]'s answer, bound to what the digest covers,
// because this command deciding it separately is how the file came to omit the
// acceptance an approval exists to authorize (stokaro/ptah#2739).
func cutoverPlanIdentity(plan embedcutover.Plan) planIdentity {
	return planIdentity{
		operation: "cutover",
		digest:    plan.Digest(),
		lines:     plan.IdentityLines(),
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
	// Default true, and measured rather than assumed (stokaro/ptah#2743).
	// Dropping the column is what destroys the vectors: RetireColumns runs only
	// under this flag, so `--drop-column=false` retires a generation whose
	// every vector is still in the table. Making it opt-in would turn the plain
	// verb into an index drop, and refuse outright for a specification that
	// declares no index method.
	cmd.Flags().BoolVar(&dropColumn, "drop-column", true,
		"Drop the storage the vectors are in as well as the index: the vector column, "+
			"or the whole table under the own_table layout")
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
	// Every part of the location comes from the registry, including the schema.
	// It was the one part read off the invocation's file, so the plan an
	// approval is bound to could name a schema other than the one
	// RetireColumns and RetireIndex actually destroy in.
	// Which storage the vectors are in is read from the generation's OWN
	// recorded specification rather than from whichever file this invocation
	// was handed. The recorded one is the only document that describes the
	// generation being destroyed; the invocation's may have been edited since,
	// and reading the layout from it would decide DROP TABLE against DROP
	// COLUMN from a file the generation was never built under -- the shape
	// stokaro/ptah#2630 already paid for on the rollback path.
	ownsTable, err := retirementOwnsTable(registered)
	if err != nil {
		return err
	}
	plan := embedcutover.RetirementPlan{
		Generation: options.generation, Schema: registered.TargetSchema,
		Table: registered.TargetTable, Column: registered.TargetColumn,
		DropsIndex:  hasIndex,
		DropsColumn: options.dropColumn && !ownsTable,
		DropsTable:  options.dropColumn && ownsTable,
		RowCount:    rows,
	}
	facts, err := retirementFacts(
		ctx, opened, registered, options.generation)
	if err != nil {
		return err
	}
	identity := retirementPlanIdentity(plan)
	approval, err := approvalFor(ctx, options.approval, identity)
	if err != nil {
		return err
	}
	decision := embedcutover.DecideRetirement(
		plan, facts.state, facts.observed, approval, opened.loaded.Policy)
	if !decision.Allowed {
		_ = writeLines(out, retirementContext(plan, facts.state, rows)...)
		_ = writePlanFile(out, options.approval.planFile, identity)
		return refusal(out, "retirement refused", decision.Blockers)
	}

	release, err := opened.store.RetireGenerationObjects(
		ctx, options.generation, facts.pointer, plan.RowCount,
		embedpg.RetirementDestruction{
			IndexExists: plan.DropsIndex,
			DropColumns: plan.DropsColumn,
			DropTable:   plan.DropsTable,
		})
	if err != nil {
		return err
	}
	retiredAt := release.RetiredAt
	lines := []string{retirementSummary(options.generation, rows, plan)}
	if sentence := release.Sentence(); sentence != "" {
		lines = append(lines, bullet(sentence))
	}
	if err := writeLines(out, lines...); err != nil {
		return err
	}
	return publishRetirement(ctx, out, options, plan, identity, approval, rows, retiredAt)
}

// retirementSummary says what the retirement actually destroyed.
//
// The vectors live in the target column, and RetireColumns is what removes it,
// so a retirement that keeps the column keeps every vector in it -- dropping
// the index is then the whole of the operation. Reporting "is gone, with N
// vectors" for that run named a destruction that did not happen, and it is the
// sentence an operator reads to decide whether storage was reclaimed
// (stokaro/ptah#2743).
func retirementSummary(generation string, rows int, plan embedcutover.RetirementPlan) string {
	if plan.DropsTable {
		return fmt.Sprintf("generation %s is gone, with %d vectors and the table %s.%s they were in",
			generation, rows, plan.Schema, plan.Table)
	}
	if plan.DropsColumn {
		return fmt.Sprintf("generation %s is gone, with %d vectors", generation, rows)
	}
	return fmt.Sprintf(
		"generation %s is retired, and its %d vectors are still in column %s: "+
			"the run kept the column, so it dropped the index and nothing else",
		generation, rows, plan.Column)
}

// retirementOwnsTable reads the layout out of the generation's recorded
// specification.
//
// A generation with no recorded specification is refused rather than assumed to
// be the common layout. The assumption would be right most of the time and
// wrong exactly where it costs the most: a LayoutOwnTable generation read as
// LayoutSourceColumns retires by dropping five columns out of Ptah's own table
// and leaving the relation, with every row still in it, credited to a
// generation the registry now calls retired.
func retirementOwnsTable(registered embedstore.Generation) (bool, error) {
	recorded, err := embedpg.RecordedSpec(registered, "stored in a table of its own")
	if err != nil {
		return false, err
	}
	return recorded.Spec.Target.Layout.OwnsTable(), nil
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
	if plan.DropsTable {
		objects = append(objects, "table "+plan.Schema+"."+plan.Table)
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
type retirementSnapshot struct {
	state    embedcutover.RetirementState
	observed embedcutover.Observed
	pointer  embedstore.Pointer
}

func retirementFacts(
	ctx context.Context, opened *session, registered embedstore.Generation, generation string,
) (retirementSnapshot, error) {
	pointer, err := opened.store.Pointer(ctx, registered.TargetSchema, registered.TargetTable)
	if err != nil && !errorsIs(err, embedstore.ErrNotFound) {
		return retirementSnapshot{}, err
	}

	state := embedcutover.RetirementState{}
	if pointer.Previous == generation {
		state.IsRollbackTargetFor = pointer.Active
		eligibility, err := rollbackEligibility(ctx, opened, generation, pointer)
		if err != nil {
			return retirementSnapshot{}, err
		}
		state.RollbackEligible = eligibility.Eligible
	}
	return retirementSnapshot{
		state: state,
		observed: embedcutover.Observed{
			ActivePointer: pointer.Active,
			Permissions:   []embedcutover.Permission{embedcutover.PermissionRetire},
			Now:           time.Now().UTC(),
		},
		pointer: pointer,
	}, nil
}

// rollbackEligibility measures whether a previous generation is still a place
// to go back to.
func rollbackEligibility(
	ctx context.Context, opened *session, generation string, pointer embedstore.Pointer,
) (embedcutover.RollbackEligibility, error) {
	state, err := embedpg.RollbackState(ctx, opened.db, generation, pointer)
	if err != nil {
		return embedcutover.RollbackEligibility{}, err
	}
	destination, err := opened.store.Generation(ctx, generation)
	if err != nil {
		return embedcutover.RollbackEligibility{}, err
	}
	policy, err := rollbackPolicy(destination, 0)
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
			fmt.Sprintf("drops table: %t", plan.DropsTable),
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
	if run.Terminal() {
		return embedverify.Report{}, embedrun.Run{}, fmt.Errorf(
			"%w: run %s is %s", embedrun.ErrTerminal, run.ID, run.Status)
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
