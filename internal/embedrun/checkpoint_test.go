package embedrun_test

import (
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/embedrun"
)

// TestCheckpoint_RefusesABatchWhoseWorkIsNotDurable is the ordering the whole
// resume story rests on.
//
// A checkpoint written before its writes moves the cursor past rows that were
// never embedded, and the run resumes AFTER them -- a permanent hole only a full
// verification could find, long after the provider was paid for answers that
// went nowhere (stokaro/ptah#2068).
func TestCheckpoint_RefusesABatchWhoseWorkIsNotDurable(t *testing.T) {
	tests := []struct {
		name    string
		outcome embedrun.BatchOutcome
		want    string
	}{
		{
			name:    "the target writes are not committed",
			outcome: embedrun.BatchOutcome{Cursor: []string{"42"}, DeletesCommitted: true},
			want:    `.*target writes are not committed.*`,
		},
		{
			name:    "the tombstones are not committed",
			outcome: embedrun.BatchOutcome{Cursor: []string{"42"}, TargetCommitted: true},
			want:    `.*tombstones are not committed.*`,
		},
		{
			name: "something was never reconciled",
			outcome: embedrun.BatchOutcome{
				Cursor: []string{"42"}, TargetCommitted: true, DeletesCommitted: true, Unreconciled: true,
			},
			want: `.*never reconciled.*`,
		},
		{
			name:    "no cursor, so nothing to resume after",
			outcome: embedrun.BatchOutcome{TargetCommitted: true, DeletesCommitted: true},
			want:    `.*names neither a cursor nor a catch-up watermark.*`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			run := running(embedrun.PhaseBackfilling)

			err := run.Checkpoint(run.FencingToken, test.outcome)

			c.Assert(err, qt.ErrorIs, embedrun.ErrCheckpoint)
			c.Assert(err, qt.ErrorMatches, test.want)
			// The refusal leaves the resume point where it was, which is the
			// point: a rejected checkpoint must not half-apply.
			c.Assert(run.Cursor, qt.HasLen, 0)
			c.Assert(run.Progress.BatchesCommitted, qt.Equals, int64(0))
		})
	}
}

// TestCheckpoint_MovesTheResumePointAndTheCounts is the accepted case, and the
// counts move together with the cursor on purpose.
//
// A cursor that advanced without its counts, or counts without the cursor, is a
// run whose resume point disagrees with its own history.
func TestCheckpoint_MovesTheResumePointAndTheCounts(t *testing.T) {
	c := qt.New(t)
	run := running(embedrun.PhaseBackfilling)

	c.Assert(run.Checkpoint(run.FencingToken, committed("42")), qt.IsNil)
	c.Assert(run.Checkpoint(run.FencingToken, committed("99")), qt.IsNil)

	c.Assert(run.Cursor, qt.DeepEquals, []string{"99"})
	c.Assert(run.Progress.BatchesCommitted, qt.Equals, int64(2))
	c.Assert(run.Progress.RowsScanned, qt.Equals, int64(20))
	c.Assert(run.Progress.RowsEmbedded, qt.Equals, int64(18))
	c.Assert(run.Progress.RowsSkipped, qt.Equals, int64(2))
	c.Assert(run.Progress.ProviderTotalTokens, qt.Equals, int64(240))
}

// TestCheckpoint_TheCursorIsCopied is a small thing with a large failure.
//
// A cursor kept by reference moves when the caller reuses its slice for the
// next batch -- so the run's resume point would silently follow the scan
// forward, and a crash would resume from a row that was never committed.
func TestCheckpoint_TheCursorIsCopied(t *testing.T) {
	c := qt.New(t)
	run := running(embedrun.PhaseBackfilling)
	cursor := []string{"42"}
	outcome := committed(cursor...)

	c.Assert(run.Checkpoint(run.FencingToken, outcome), qt.IsNil)
	cursor[0] = "9999"

	c.Assert(run.Cursor, qt.DeepEquals, []string{"42"})
}

// TestCheckpoint_TheRetryCountIsPerBatch keeps a slow batch from reading as a
// stuck run.
//
// A batch that finally committed says nothing about the next one, so its
// retries do not carry forward.
func TestCheckpoint_TheRetryCountIsPerBatch(t *testing.T) {
	c := qt.New(t)
	run := running(embedrun.PhaseBackfilling)
	run.Progress.RetryCount = 4

	c.Assert(run.Checkpoint(run.FencingToken, committed("42")), qt.IsNil)

	c.Assert(run.Progress.RetryCount, qt.Equals, 0)
}

// TestCheckpoint_CatchUpMovesItsOwnWatermark separates the two boundaries.
//
// The snapshot watermark is where the backfill's world ends; the catch-up
// watermark is how far past it the run has processed. A backfill batch carries
// no catch-up watermark and must not clear the one already recorded.
func TestCheckpoint_CatchUpMovesItsOwnWatermark(t *testing.T) {
	c := qt.New(t)
	run := running(embedrun.PhaseCaughtUp)
	run.SnapshotWatermark = "1000"
	run.CatchUpWatermark = "1200"

	outcome := committed("42")
	c.Assert(run.Checkpoint(run.FencingToken, outcome), qt.IsNil)
	c.Assert(run.CatchUpWatermark, qt.Equals, "1200")

	outcome.CatchUpWatermark = "1500"
	c.Assert(run.Checkpoint(run.FencingToken, outcome), qt.IsNil)

	c.Assert(run.CatchUpWatermark, qt.Equals, "1500")
	c.Assert(run.SnapshotWatermark, qt.Equals, "1000")
}

// TestPauseAndResume_PreserveEverythingAResumeNeeds is the epic's requirement
// that a pause stop at a safe boundary without losing committed work.
func TestPauseAndResume_PreserveEverythingAResumeNeeds(t *testing.T) {
	c := qt.New(t)
	run := running(embedrun.PhaseBackfilling)
	c.Assert(run.Checkpoint(run.FencingToken, committed("42")), qt.IsNil)

	c.Assert(run.Pause(run.FencingToken, "the operator asked"), qt.IsNil)

	c.Assert(run.Status, qt.Equals, embedrun.StatusPaused)
	c.Assert(run.FailureDetail, qt.Equals, "the operator asked")
	c.Assert(run.Cursor, qt.DeepEquals, []string{"42"})
	c.Assert(run.Progress.BatchesCommitted, qt.Equals, int64(1))
	c.Assert(run.LeaseOwner, qt.Equals, "worker-a")

	c.Assert(run.Resume(run.FencingToken), qt.IsNil)

	c.Assert(run.Status, qt.Equals, embedrun.StatusRunning)
	c.Assert(run.FailureDetail, qt.Equals, "")
	c.Assert(run.Cursor, qt.DeepEquals, []string{"42"})
}

// TestPauseAndFail_RequireSomethingToActOn is why both take words.
//
// "It stopped" is the answer nobody can do anything with, and it is the answer
// a run gives when the reason was optional.
func TestPauseAndFail_RequireSomethingToActOn(t *testing.T) {
	c := qt.New(t)
	run := running(embedrun.PhaseBackfilling)

	c.Assert(run.Pause(run.FencingToken, ""), qt.ErrorIs, embedrun.ErrCheckpoint)
	c.Assert(run.Fail(run.FencingToken, "", "detail"), qt.ErrorIs, embedrun.ErrCheckpoint)
	c.Assert(run.Fail(run.FencingToken, "class", ""), qt.ErrorIs, embedrun.ErrCheckpoint)
	c.Assert(run.Status, qt.Equals, embedrun.StatusRunning)
}

// TestResume_OnlyAPausedRunResumes stops a failed run being restarted as though
// nothing had happened: a failure is classified, and clearing it silently would
// lose the classification an operator needs.
func TestResume_OnlyAPausedRunResumes(t *testing.T) {
	c := qt.New(t)
	run := running(embedrun.PhaseBackfilling)
	c.Assert(run.Fail(run.FencingToken, "provider", "the endpoint refused the credential"), qt.IsNil)

	err := run.Resume(run.FencingToken)

	c.Assert(err, qt.ErrorIs, embedrun.ErrCheckpoint)
	c.Assert(run.Status, qt.Equals, embedrun.StatusFailed)
	c.Assert(run.FailureClass, qt.Equals, "provider")
}

// TestAbandon_KeepsTheCheckpointAndReleasesTheLease is the non-destructive
// distinction from retirement. The run ends, but everything already built is
// still described by the row.
func TestAbandon_KeepsTheCheckpointAndReleasesTheLease(t *testing.T) {
	c := qt.New(t)
	run := running(embedrun.PhaseBackfilling)
	c.Assert(run.Checkpoint(run.FencingToken, committed("42")), qt.IsNil)

	err := run.Abandon(run.FencingToken, "the migration was superseded")

	c.Assert(err, qt.IsNil)
	c.Assert(run.Status, qt.Equals, embedrun.StatusAbandoned)
	c.Assert(run.Terminal(), qt.IsTrue)
	c.Assert(run.FailureDetail, qt.Equals, "the migration was superseded")
	c.Assert(run.Cursor, qt.DeepEquals, []string{"42"})
	c.Assert(run.Progress.BatchesCommitted, qt.Equals, int64(1))
	c.Assert(run.LeaseOwner, qt.Equals, "")
	c.Assert(run.LeaseExpires.IsZero(), qt.IsTrue)
	c.Assert(run.Checkpoint(run.FencingToken, committed("43")), qt.ErrorIs, embedrun.ErrTerminal)
	c.Assert(run.Reach(run.FencingToken, embedrun.PhaseBackfilled), qt.ErrorIs, embedrun.ErrTerminal)
}

// TestAbandon_RequiresAReasonAndTheCurrentToken keeps the terminal state from
// becoming an unowned status write.
func TestAbandon_RequiresAReasonAndTheCurrentToken(t *testing.T) {
	c := qt.New(t)
	run := running(embedrun.PhaseBackfilling)
	stale := run.FencingToken
	run.Claim("worker-b", time.Minute)

	c.Assert(run.Abandon(stale, "the migration was superseded"), qt.ErrorIs, embedrun.ErrFenced)
	c.Assert(run.Abandon(run.FencingToken, ""), qt.ErrorIs, embedrun.ErrCheckpoint)
	c.Assert(run.Status, qt.Equals, embedrun.StatusRunning)
}

// TestCheckpoint_ACatchUpBatchResumesFromAWatermarkRatherThanAKey is the other
// half of a run.
//
// The backfill resumes from a key and catch-up resumes from a transaction. A
// catch-up batch names no keyset cursor because there is no key it got to, and
// requiring one would make catch-up unable to checkpoint at all.
func TestCheckpoint_ACatchUpBatchResumesFromAWatermarkRatherThanAKey(t *testing.T) {
	c := qt.New(t)
	run := embedrun.Run{Phase: embedrun.PhaseCaughtUp, Status: embedrun.StatusRunning, FencingToken: 1}

	err := run.Checkpoint(1, embedrun.BatchOutcome{
		CatchUpWatermark: "500", TargetCommitted: true, DeletesCommitted: true,
	})

	c.Assert(err, qt.IsNil)
	c.Assert(run.CatchUpWatermark, qt.Equals, "500")
	c.Assert(run.Cursor, qt.HasLen, 0)
}

// TestCheckpoint_ACatchUpBatchDoesNotEraseTheBackfillsCursor is what makes the
// two resume points independent.
//
// A catch-up batch carries no key, and a checkpoint that copied its empty
// cursor over the backfill's would send a resumed backfill to the start of the
// table -- over a target that already holds most of a corpus.
func TestCheckpoint_ACatchUpBatchDoesNotEraseTheBackfillsCursor(t *testing.T) {
	c := qt.New(t)
	run := embedrun.Run{
		Phase: embedrun.PhaseCaughtUp, Status: embedrun.StatusRunning, FencingToken: 1,
		Cursor: []string{"tenant-b", "4000"},
	}

	err := run.Checkpoint(1, embedrun.BatchOutcome{
		CatchUpWatermark: "500", TargetCommitted: true, DeletesCommitted: true,
	})

	c.Assert(err, qt.IsNil)
	c.Assert(run.Cursor, qt.DeepEquals, []string{"tenant-b", "4000"})
}
