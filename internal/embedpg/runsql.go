package embedpg

import (
	"database/sql"
	"errors"
	"fmt"

	"go.5x5.cz/ptah/internal/embedrun"
	"go.5x5.cz/ptah/internal/embedstore"
)

// runColumns is the run table's columns in the order the statements below use
// them.
//
// One list, because the INSERT, the UPDATE and the SELECT all have to agree
// about it and three hand-written lists agree until somebody adds a column to
// two of them.
//
// A new column is appended rather than slotted in beside its relatives, so that
// nothing after it renumbers. Renumbering ten placeholders by hand to keep a
// thematic order is how a run comes to be written with its counters one column
// over, and no test reads a column it was never given.
const runColumns = `id, spec_digest, generation_identity, environment, source, target,
	provider_profile, resolved_model, ptah_version, policy_digest, phase, status,
	lease_owner, lease_expires, fencing_token, snapshot_watermark, catch_up_watermark, cursor,
	rows_scanned, rows_embedded, rows_skipped, rows_deleted, batches_committed,
	provider_prompt_tokens, provider_total_tokens, retry_count,
	verification_ref, cutover_plan_ref, approval_ref, active_pointer, rollback_eligible,
	failure_class, failure_detail, created_at, updated_at,
	provider_usage_batches`

// insertRunSQL creates a run, refusing to replace one.
const insertRunSQL = `INSERT INTO ` + embedstore.RunTable + ` (` + runColumns + `)
	VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,
		$21,$22,$23,$24,$25,$26,$27,$28,$29,$30,$31,$32,$33,$34,$35,$36)
	ON CONFLICT (id) DO NOTHING`

// updateRunSQL writes a run's state, refusing a stale fencing token.
//
// `fencing_token <= $15` rather than `=`: the writer that RAISES the token is
// the one taking the run over, and it has to be able to write the row that
// records the takeover.
const updateRunSQL = `UPDATE ` + embedstore.RunTable + ` SET
	spec_digest=$2, generation_identity=$3, environment=$4, source=$5, target=$6,
	provider_profile=$7, resolved_model=$8, ptah_version=$9, policy_digest=$10,
	phase=$11, status=$12, lease_owner=$13, lease_expires=$14, fencing_token=$15,
	snapshot_watermark=$16, catch_up_watermark=$17, cursor=$18,
	rows_scanned=$19, rows_embedded=$20, rows_skipped=$21, rows_deleted=$22, batches_committed=$23,
	provider_prompt_tokens=$24, provider_total_tokens=$25, retry_count=$26,
	verification_ref=$27, cutover_plan_ref=$28, approval_ref=$29, active_pointer=$30,
	rollback_eligible=$31, failure_class=$32, failure_detail=$33, created_at=$34, updated_at=$35,
	provider_usage_batches=$36
	WHERE id=$1 AND fencing_token <= $15`

// claimRunSQL takes the lease and nothing else.
//
// Three properties, and each of them is a defect this replaces
// (stokaro/ptah#2636):
//
//   - It names the lease columns only. A claim that wrote the whole row wrote
//     back the cursor and the counters it had read, erasing a checkpoint the
//     working worker committed in between.
//   - It computes the token from the stored value rather than from one the
//     caller read. Two claimers reading one token compute one successor, and
//     the second write passes a `fencing_token <= $n` guard.
//   - It RETURNS the row it wrote, so the caller's copy of the run is the
//     store's, including whatever the fenced worker committed before it was
//     fenced.
const claimRunSQL = `UPDATE ` + embedstore.RunTable + ` SET
	lease_owner=$2, lease_expires=$3, fencing_token=fencing_token+1, updated_at=$4
	WHERE id=$1
	RETURNING ` + runColumns

// selectRunsForGenerationSQL reads every run that built one generation.
//
// Ordered newest first, and by id after that, so a caller reading the answer
// gets a stable order rather than whatever the plan happened to produce -- two
// runs of one specification started in the same second are ordinary.
const selectRunsForGenerationSQL = `SELECT ` + runColumns +
	` FROM ` + embedstore.RunTable +
	` WHERE generation_identity = $1 ORDER BY created_at DESC, id`

// selectRunSQL reads one back.
const selectRunSQL = `SELECT ` + runColumns + ` FROM ` + embedstore.RunTable + ` WHERE id = $1`

// runArguments binds a run to the statements above, in the same order.
func runArguments(run embedrun.Run, cursor any) []any {
	return []any{
		run.ID, run.SpecDigest, run.GenerationIdentity, run.Environment, run.Source, run.Target,
		run.ProviderProfile, nullable(run.ResolvedModel), run.PtahVersion, run.PolicyDigest,
		string(run.Phase), string(run.Status),
		nullable(run.LeaseOwner), nullableTime(run.LeaseExpires), run.FencingToken,
		nullable(run.SnapshotWatermark), nullable(run.CatchUpWatermark), cursor,
		run.Progress.RowsScanned, run.Progress.RowsEmbedded, run.Progress.RowsSkipped,
		run.Progress.RowsDeleted, run.Progress.BatchesCommitted,
		run.Progress.ProviderPromptTokens, run.Progress.ProviderTotalTokens, run.Progress.RetryCount,
		nullable(run.VerificationRef), nullable(run.CutoverPlanRef), nullable(run.ApprovalRef),
		nullable(run.ActivePointer), run.RollbackEligible,
		nullable(run.FailureClass), nullable(run.FailureDetail),
		run.CreatedAt.UTC(), run.UpdatedAt.UTC(),
		run.Progress.ProviderUsageBatches,
	}
}

// row is what a scan reads from, so one function serves QueryRow and Query.
type row interface {
	Scan(destination ...any) error
}

// scanRun reads a run's columns in the order selectRunSQL names them.
func scanRun(source row, id string) (embedrun.Run, error) {
	var run embedrun.Run
	var phase, status string
	var resolvedModel, leaseOwner, snapshot, catchUp, cursor sql.NullString
	var verification, plan, approval, pointer, failureClass, failureDetail sql.NullString
	var leaseExpires sql.NullTime

	err := source.Scan(
		&run.ID, &run.SpecDigest, &run.GenerationIdentity, &run.Environment, &run.Source, &run.Target,
		&run.ProviderProfile, &resolvedModel, &run.PtahVersion, &run.PolicyDigest, &phase, &status,
		&leaseOwner, &leaseExpires, &run.FencingToken, &snapshot, &catchUp, &cursor,
		&run.Progress.RowsScanned, &run.Progress.RowsEmbedded, &run.Progress.RowsSkipped,
		&run.Progress.RowsDeleted, &run.Progress.BatchesCommitted,
		&run.Progress.ProviderPromptTokens, &run.Progress.ProviderTotalTokens, &run.Progress.RetryCount,
		&verification, &plan, &approval, &pointer, &run.RollbackEligible,
		&failureClass, &failureDetail, &run.CreatedAt, &run.UpdatedAt,
		&run.Progress.ProviderUsageBatches)
	if errors.Is(err, sql.ErrNoRows) {
		return embedrun.Run{}, fmt.Errorf("%w: run %s", embedstore.ErrNotFound, id)
	}
	if err != nil {
		return embedrun.Run{}, fmt.Errorf("read run %s: %w", id, err)
	}

	run.Phase = embedrun.Phase(phase)
	run.Status = embedrun.Status(status)
	run.ResolvedModel = resolvedModel.String
	run.LeaseOwner = leaseOwner.String
	run.SnapshotWatermark = snapshot.String
	run.CatchUpWatermark = catchUp.String
	run.VerificationRef = verification.String
	run.CutoverPlanRef = plan.String
	run.ApprovalRef = approval.String
	run.ActivePointer = pointer.String
	run.FailureClass = failureClass.String
	run.FailureDetail = failureDetail.String
	if leaseExpires.Valid {
		run.LeaseExpires = leaseExpires.Time.UTC()
	}
	run.CreatedAt = run.CreatedAt.UTC()
	run.UpdatedAt = run.UpdatedAt.UTC()

	decoded, err := decodeCursor(cursor)
	if err != nil {
		return embedrun.Run{}, err
	}
	run.Cursor = decoded
	return run, nil
}

// scanEvent reads one audit row.
func scanEvent(source row) (embedrun.Event, error) {
	var event embedrun.Event
	var kind, from, to string
	if err := source.Scan(
		&event.RunID, &kind, &event.At, &event.Actor, &event.FencingToken,
		&from, &to, &event.Detail,
		&event.Counts.RowsScanned, &event.Counts.RowsEmbedded, &event.Counts.RowsSkipped,
		&event.Counts.RowsDeleted, &event.Counts.BatchesCommitted); err != nil {
		return embedrun.Event{}, fmt.Errorf("read event: %w", err)
	}
	event.Kind = embedrun.EventKind(kind)
	event.FromPhase = embedrun.Phase(from)
	event.ToPhase = embedrun.Phase(to)
	event.At = event.At.UTC()
	return event, nil
}
