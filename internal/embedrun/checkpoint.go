package embedrun

import (
	"fmt"
	"time"
)

// BatchOutcome is what one batch did, handed to [Run.Checkpoint] once every
// effect it describes is committed.
//
// It is a value rather than a set of arguments because the checkpoint's whole
// contract is that these move together: a cursor that advanced without its row
// counts, or counts without the cursor, is a run whose resume point disagrees
// with its own history.
type BatchOutcome struct {
	// Cursor is the last completed key, in the specification's key order. It is
	// what the next batch resumes after.
	Cursor []string
	// RowsScanned, RowsEmbedded, RowsSkipped and RowsDeleted are this batch's
	// counts.
	RowsScanned  int64
	RowsEmbedded int64
	RowsSkipped  int64
	RowsDeleted  int64
	// PromptTokens and TotalTokens are what the provider reported for it.
	PromptTokens int64
	TotalTokens  int64
	// UsageReported says the answer carried a usage object. A batch that
	// reported nothing contributes no counts AND no claim that its counts are
	// the provider's, which is the difference between a zero measured and a
	// zero nobody asked for (stokaro/ptah#2648).
	UsageReported bool
	// CatchUpWatermark is how far catch-up has processed, set during catch-up
	// and empty during the backfill.
	CatchUpWatermark string
	// TargetCommitted and DeletesCommitted are the caller's statement that
	// every target write and every tombstone this batch produced is durable.
	//
	// They are asserted rather than inferred because this package cannot see
	// the database: the checkpoint's meaning is "the work behind this cursor is
	// on disk", and only the caller that wrote it knows.
	TargetCommitted  bool
	DeletesCommitted bool
	// Unreconciled reports a result the caller could not decide about -- a
	// provider request whose answer never arrived, a write whose outcome is
	// unknown. A batch carrying one is not complete.
	Unreconciled bool
}

// Checkpoint records one completed batch and moves the resume point.
//
// A batch is complete only when every target write is committed, every
// tombstone is committed, and nothing is left unreconciled. The order matters
// and it is the reason this refuses rather than trusts: a checkpoint written
// before its writes would move the cursor past rows that were never embedded,
// and the run would resume after them -- leaving a permanent hole that only a
// full verification could find, long after the provider was paid
// (stokaro/ptah#2068).
func (r *Run) Checkpoint(token int64, outcome BatchOutcome) error {
	if err := r.Fence(token); err != nil {
		return err
	}
	if err := checkpointReady(outcome); err != nil {
		return err
	}

	// Each resume point is overwritten only by a batch that names one. The two
	// halves of a run resume from different things -- the backfill from a key,
	// catch-up from a transaction -- and a catch-up batch that cleared the
	// keyset cursor would send a resumed backfill to the start of the table.
	if len(outcome.Cursor) > 0 {
		r.Cursor = append([]string(nil), outcome.Cursor...)
	}
	if outcome.CatchUpWatermark != "" {
		r.CatchUpWatermark = outcome.CatchUpWatermark
	}
	r.Progress.RowsScanned += outcome.RowsScanned
	r.Progress.RowsEmbedded += outcome.RowsEmbedded
	r.Progress.RowsSkipped += outcome.RowsSkipped
	r.Progress.RowsDeleted += outcome.RowsDeleted
	r.Progress.ProviderPromptTokens += outcome.PromptTokens
	r.Progress.ProviderTotalTokens += outcome.TotalTokens
	if outcome.UsageReported {
		r.Progress.ProviderUsageBatches++
	}
	r.Progress.BatchesCommitted++
	// The retry count is per-batch: a batch that finally committed says nothing
	// about the next one, and carrying its retries forward would make a run
	// look stuck the moment one batch was slow.
	r.Progress.RetryCount = 0
	r.UpdatedAt = time.Now().UTC()
	return nil
}

// checkpointReady states the three preconditions.
func checkpointReady(outcome BatchOutcome) error {
	switch {
	case !outcome.TargetCommitted:
		return fmt.Errorf("%w: the batch's target writes are not committed", ErrCheckpoint)
	case !outcome.DeletesCommitted:
		return fmt.Errorf("%w: the batch's tombstones are not committed", ErrCheckpoint)
	case outcome.Unreconciled:
		return fmt.Errorf("%w: the batch carries a result that was never reconciled", ErrCheckpoint)
	case len(outcome.Cursor) == 0 && outcome.CatchUpWatermark == "":
		// One or the other, not both: a backfill batch names a key and a
		// catch-up batch names a transaction, and a batch that names neither
		// has nothing to resume after.
		return fmt.Errorf(
			"%w: the batch names neither a cursor nor a catch-up watermark, so there is nothing "+
				"to resume after", ErrCheckpoint)
	default:
		return nil
	}
}

// Pause stops the run at a safe boundary, keeping everything a resume needs.
//
// The reason is required: a paused run whose reason is empty is one nobody can
// act on, and "why did this stop" is the first question its operator asks.
func (r *Run) Pause(token int64, reason string) error {
	if err := r.Fence(token); err != nil {
		return err
	}
	if reason == "" {
		return fmt.Errorf("%w: a pause without a reason cannot be acted on", ErrCheckpoint)
	}
	r.Status = StatusPaused
	r.FailureDetail = reason
	r.UpdatedAt = time.Now().UTC()
	return nil
}

// Fail stops the run with a classification.
//
// The class is what a later reader groups by -- provider, source, target,
// policy -- and the detail is what they read. Both are required for the same
// reason a pause needs a reason.
func (r *Run) Fail(token int64, class, detail string) error {
	if err := r.Fence(token); err != nil {
		return err
	}
	if class == "" || detail == "" {
		return fmt.Errorf("%w: a failure needs a class and a detail", ErrCheckpoint)
	}
	r.Status = StatusFailed
	r.FailureClass = class
	r.FailureDetail = detail
	r.UpdatedAt = time.Now().UTC()
	return nil
}

// Resume returns a paused run to running, clearing the pause reason.
func (r *Run) Resume(token int64) error {
	if err := r.Fence(token); err != nil {
		return err
	}
	if r.Status != StatusPaused {
		return fmt.Errorf("%w: only a paused run resumes, and this one is %s", ErrCheckpoint, r.Status)
	}
	r.Status = StatusRunning
	r.FailureDetail = ""
	r.UpdatedAt = time.Now().UTC()
	return nil
}
