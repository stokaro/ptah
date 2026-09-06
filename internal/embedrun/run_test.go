package embedrun_test

import (
	"reflect"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/embedrun"
)

// running is a run a worker holds, at the given phase.
func running(phase embedrun.Phase) *embedrun.Run {
	run := &embedrun.Run{ID: "run-1", Phase: phase, Status: embedrun.StatusRunning}
	run.Claim("worker-a", time.Minute)
	return run
}

// committed is a batch whose every effect is durable.
func committed(cursor ...string) embedrun.BatchOutcome {
	return embedrun.BatchOutcome{
		Cursor: cursor, RowsScanned: 10, RowsEmbedded: 9, RowsSkipped: 1,
		PromptTokens: 100, TotalTokens: 120,
		TargetCommitted: true, DeletesCommitted: true,
	}
}

// TestFence_AStaleWorkerCannotCommit is the guarantee a lease alone does not
// give.
//
// A worker paused past its lease and then resumed still believes it holds the
// run, and the only thing that can stop it is the state refusing its token. The
// second claim is what a supervisor does when the first worker stops answering,
// and from that moment the first must be unable to move anything
// (stokaro/ptah#2068).
func TestFence_AStaleWorkerCannotCommit(t *testing.T) {
	c := qt.New(t)
	run := running(embedrun.PhaseBackfilling)
	stale := run.FencingToken

	fresh := run.Claim("worker-b", time.Minute)

	c.Assert(fresh > stale, qt.IsTrue)
	c.Assert(run.Fence(stale), qt.ErrorIs, embedrun.ErrFenced)
	c.Assert(run.Checkpoint(stale, committed("42")), qt.ErrorIs, embedrun.ErrFenced)
	c.Assert(run.Reach(stale, embedrun.PhaseCaughtUp), qt.ErrorIs, embedrun.ErrFenced)
	c.Assert(run.Pause(stale, "stopping"), qt.ErrorIs, embedrun.ErrFenced)
	c.Assert(run.Fail(stale, "provider", "refused"), qt.ErrorIs, embedrun.ErrFenced)
	// And the run is untouched by any of it.
	c.Assert(run.Cursor, qt.HasLen, 0)
	c.Assert(run.Phase, qt.Equals, embedrun.PhaseBackfilling)
	c.Assert(run.Status, qt.Equals, embedrun.StatusRunning)
}

// TestFence_TheCurrentWorkerCommits is the control: a fence that refused
// everyone would satisfy the test above and stop the product working.
func TestFence_TheCurrentWorkerCommits(t *testing.T) {
	c := qt.New(t)
	run := running(embedrun.PhaseBackfilling)

	c.Assert(run.Checkpoint(run.FencingToken, committed("42")), qt.IsNil)
	c.Assert(run.Cursor, qt.DeepEquals, []string{"42"})
}

// TestReach_NothingReachesCutoverExceptThroughVerification is why the phases
// are a machine rather than a label.
//
// Each row is a jump somebody could make by setting a field, and each one skips
// a step whose absence is invisible afterwards: a corpus cut over without
// verification looks exactly like one that passed.
func TestReach_NothingReachesCutoverExceptThroughVerification(t *testing.T) {
	tests := []struct {
		name string
		from embedrun.Phase
		to   embedrun.Phase
	}{
		{name: "backfilling straight to cutover", from: embedrun.PhaseBackfilling, to: embedrun.PhaseCutOver},
		{name: "prepared straight to backfilling", from: embedrun.PhasePrepared, to: embedrun.PhaseBackfilling},
		{name: "indexed straight to cutover", from: embedrun.PhaseIndexed, to: embedrun.PhaseCutOver},
		{name: "caught up straight to cutover", from: embedrun.PhaseCaughtUp, to: embedrun.PhaseCutOver},
		{name: "verified straight to retired", from: embedrun.PhaseVerified, to: embedrun.PhaseRetired},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			run := running(test.from)

			err := run.Reach(run.FencingToken, test.to)

			c.Assert(err, qt.ErrorIs, embedrun.ErrPhase)
			c.Assert(run.Phase, qt.Equals, test.from)
		})
	}
}

// TestReach_VerificationFollowsTheCatchUpWhenThereIsNoIndex is the edge a
// specification without an index method needs.
//
// Such a generation has no indexing step, so requiring the phase would leave a
// run unable to reach one it legitimately completed. The edge is in the table
// rather than decided per run, because a static table cannot read a
// specification -- and it costs nothing: a DECLARED index that is missing is
// refused by verification, and cut_over is still reachable only from verified,
// which the row above asserts.
func TestReach_VerificationFollowsTheCatchUpWhenThereIsNoIndex(t *testing.T) {
	c := qt.New(t)
	run := running(embedrun.PhaseCaughtUp)

	c.Assert(run.Reach(run.FencingToken, embedrun.PhaseVerified), qt.IsNil)
	c.Assert(run.Phase, qt.Equals, embedrun.PhaseVerified)
}

// TestReach_APhaseAlreadyPassedIsLeftAlone is what makes the verbs usable.
//
// The phase is a high-water mark, not a cursor. A catch-up run after a
// verification is ordinary -- the source keeps moving -- and it asks to reach a
// phase the run went past. Reporting that as an error would make re-running an
// earlier verb a failure; the work happened, it told the run nothing new.
//
// Silence rather than an error, and the phase unmoved: those are two different
// claims and both are asserted, because an implementation that answered nil and
// dragged the run backwards would satisfy the first alone.
func TestReach_APhaseAlreadyPassedIsLeftAlone(t *testing.T) {
	tests := []struct {
		name string
		from embedrun.Phase
		to   embedrun.Phase
	}{
		{name: "catching up again after verification",
			from: embedrun.PhaseVerified, to: embedrun.PhaseCaughtUp},
		{name: "the same phase twice",
			from: embedrun.PhaseBackfilling, to: embedrun.PhaseBackfilling},
		{name: "one step behind",
			from: embedrun.PhaseIndexed, to: embedrun.PhaseCaughtUp},
		{name: "a terminal run told to cut over",
			from: embedrun.PhaseRetired, to: embedrun.PhaseCutOver},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			run := running(test.from)

			c.Assert(run.Reach(run.FencingToken, test.to), qt.IsNil)
			c.Assert(run.Phase, qt.Equals, test.from)
		})
	}
}

// TestReach_RefusesAPhaseTheLifecycleDoesNotHave is the input guard.
//
// A phase nothing declares is neither ahead nor behind, and answering nil
// would let a typo pass for a move that happened.
func TestReach_RefusesAPhaseTheLifecycleDoesNotHave(t *testing.T) {
	c := qt.New(t)
	run := running(embedrun.PhaseBackfilling)

	err := run.Reach(run.FencingToken, embedrun.Phase("indexing"))

	c.Assert(err, qt.ErrorIs, embedrun.ErrPhase)
	c.Assert(run.Phase, qt.Equals, embedrun.PhaseBackfilling)
}

// TestReach_WalksTheWholeLifecycle is the other side: the path the epic
// describes has to be walkable end to end, or the machine is a wall.
func TestReach_WalksTheWholeLifecycle(t *testing.T) {
	c := qt.New(t)
	run := running(embedrun.PhaseResolved)

	for _, phase := range []embedrun.Phase{
		embedrun.PhasePlanned, embedrun.PhasePrepared, embedrun.PhaseBoundaryCaptured,
		embedrun.PhaseBackfilling, embedrun.PhaseBackfilled, embedrun.PhaseCaughtUp,
		embedrun.PhaseIndexed,
		embedrun.PhaseVerified, embedrun.PhaseCutOver, embedrun.PhaseRetired,
	} {
		c.Assert(run.Reach(run.FencingToken, phase), qt.IsNil, qt.Commentf("moving to %s", phase))
		c.Assert(run.Phase, qt.Equals, phase)
	}
}

// TestReach_RollbackIsReachableFromCutover pins the other branch: a run that
// cut over must be able to go back, which is what makes the rollback window
// real rather than a promise.
func TestReach_RollbackIsReachableFromCutover(t *testing.T) {
	c := qt.New(t)
	run := running(embedrun.PhaseCutOver)

	c.Assert(run.Reach(run.FencingToken, embedrun.PhaseRolledBack), qt.IsNil)
	c.Assert(run.Phase, qt.Equals, embedrun.PhaseRolledBack)
}

// TestReach_ARollbackIsReversible is what the guide promises and what the
// first attempt at these phases took away.
//
// A rollback is documented as reversible: cutting the generation over again is
// how it is reversed. Recorded as a forward-only move, the run went on saying
// `rolled_back` while the pointer named its generation as the one queries read,
// and nothing could ever bring the two back into agreement
// (stokaro/ptah#2649 finding 6).
func TestReach_ARollbackIsReversible(t *testing.T) {
	c := qt.New(t)
	run := running(embedrun.PhaseCutOver)

	c.Assert(run.Reach(run.FencingToken, embedrun.PhaseRolledBack), qt.IsNil)
	c.Assert(run.Reach(run.FencingToken, embedrun.PhaseCutOver), qt.IsNil)
	c.Assert(run.Phase, qt.Equals, embedrun.PhaseCutOver)
	// And still reversible after that, because an operator may go back and
	// forth for as long as the window lasts.
	c.Assert(run.Reach(run.FencingToken, embedrun.PhaseRolledBack), qt.IsNil)
	c.Assert(run.Phase, qt.Equals, embedrun.PhaseRolledBack)
}

// TestReach_ARolledBackGenerationCanStillBeRetired is the other move the
// forward-only table refused.
//
// Rolling a generation off the pointer and then retiring it is the ordinary end
// of one nobody wants back. With `rolled_back` declared as leading nowhere, the
// retirement destroyed the vectors and the phase change was refused, so the row
// stood describing a corpus that no longer existed.
func TestReach_ARolledBackGenerationCanStillBeRetired(t *testing.T) {
	c := qt.New(t)
	run := running(embedrun.PhaseRolledBack)

	c.Assert(run.Reach(run.FencingToken, embedrun.PhaseRetired), qt.IsNil)
	c.Assert(run.Phase, qt.Equals, embedrun.PhaseRetired)
}

// TestReach_RetirementCompletesTheRunAndReleasesItsLease gives
// [embedrun.StatusComplete] the producer it never had.
//
// The constant and its doc comment were the only two lines in the tree naming
// it, so every run ever built reported `running` for the rest of the registry's
// life -- including runs whose generation had been destroyed, which still
// carried a lease naming a worker that had exited.
func TestReach_RetirementCompletesTheRunAndReleasesItsLease(t *testing.T) {
	c := qt.New(t)
	run := running(embedrun.PhaseCutOver)

	c.Assert(run.Reach(run.FencingToken, embedrun.PhaseRetired), qt.IsNil)
	c.Assert(run.Status, qt.Equals, embedrun.StatusComplete)
	c.Assert(run.LeaseOwner, qt.Equals, "")
	c.Assert(run.LeaseExpires.IsZero(), qt.IsTrue)
}

// TestReach_ARollbackDoesNotCompleteTheRun is the control for the test above.
//
// A rolled-back run can be cut over again, so calling it complete would be the
// same false statement in the status column that the phase column was making.
// Without this, a fix that completed every run leaving `cut_over` would pass.
func TestReach_ARollbackDoesNotCompleteTheRun(t *testing.T) {
	c := qt.New(t)
	run := running(embedrun.PhaseCutOver)

	c.Assert(run.Reach(run.FencingToken, embedrun.PhaseRolledBack), qt.IsNil)
	c.Assert(run.Status, qt.Equals, embedrun.StatusRunning)
	c.Assert(run.LeaseOwner, qt.Equals, "worker-a")
}

// TestLeadsTo_AnswersWhichRunsAVerbAdvances covers the predicate `retire` and
// `rollback` ask instead of naming a phase themselves.
func TestLeadsTo_AnswersWhichRunsAVerbAdvances(t *testing.T) {
	tests := []struct {
		name string
		from embedrun.Phase
		to   embedrun.Phase
		want bool
	}{
		{name: "cutover to retired", from: embedrun.PhaseCutOver, to: embedrun.PhaseRetired, want: true},
		{name: "cutover to rolled back", from: embedrun.PhaseCutOver, to: embedrun.PhaseRolledBack, want: true},
		{name: "rolled back to retired", from: embedrun.PhaseRolledBack, to: embedrun.PhaseRetired, want: true},
		{name: "rolled back to rolled back", from: embedrun.PhaseRolledBack, to: embedrun.PhaseRolledBack, want: false},
		{name: "verified to retired", from: embedrun.PhaseVerified, to: embedrun.PhaseRetired, want: false},
		{name: "retired to anything", from: embedrun.PhaseRetired, to: embedrun.PhaseRolledBack, want: false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(test.from.LeadsTo(test.to), qt.Equals, test.want)
		})
	}
}

// TestEvent_CarriesNoRowContent is the privacy rule made structural.
//
// The check is on the TYPE rather than on a value, because a rule about what
// callers put in a field is a rule somebody eventually breaks. An audit trail
// able to carry the corpus would be a second copy of it, outside every control
// the corpus has.
func TestEvent_CarriesNoRowContent(t *testing.T) {
	c := qt.New(t)

	fields := reflect.TypeFor[embedrun.Event]()

	for field := range fields.Fields() {
		name := strings.ToLower(field.Name)
		for _, forbidden := range []string{"vector", "embedding", "content", "row", "input", "text", "payload"} {
			c.Assert(name, qt.Not(qt.Contains), forbidden,
				qt.Commentf("Event.%s could carry corpus content into the audit log", field.Name))
		}
	}
}

// TestNewEvent_RecordsWhoHeldWhatAndWhere is what makes a refused commit
// reconstructable: without the token, two workers' events are indistinguishable.
func TestNewEvent_RecordsWhoHeldWhatAndWhere(t *testing.T) {
	c := qt.New(t)
	run := running(embedrun.PhaseBackfilling)
	c.Assert(run.Checkpoint(run.FencingToken, committed("42")), qt.IsNil)

	event := embedrun.NewEvent(run, embedrun.EventCheckpoint, "worker-a", "batch 1")

	c.Assert(event.RunID, qt.Equals, "run-1")
	c.Assert(event.Kind, qt.Equals, embedrun.EventCheckpoint)
	c.Assert(event.Actor, qt.Equals, "worker-a")
	c.Assert(event.FencingToken, qt.Equals, run.FencingToken)
	c.Assert(event.ToPhase, qt.Equals, embedrun.PhaseBackfilling)
	c.Assert(event.Counts.RowsEmbedded, qt.Equals, int64(9))
}

// TestReached_AnswersAtOrPast is the direction test, and it exists because I
// got the direction wrong writing it.
//
// `reaches(from, to)` asks whether `to` is reachable FROM `from`, so
// `Reached(phase)` has to ask `reaches(phase, r.Phase)` -- is the run's phase
// reachable from the one being asked about. The transposition compiles, type
// checks, and answers the exact opposite: it reports a run as having reached
// every phase still ahead of it.
//
// Both directions are asserted for that reason. A test that only checked the
// phases at or past the target passes just as happily with the arguments
// swapped, because a run at `boundary_captured` can reach `backfilled` too.
func TestReached_AnswersAtOrPast(t *testing.T) {
	tests := []struct {
		name  string
		phase embedrun.Phase
		want  bool
	}{
		{name: "the phase itself", phase: embedrun.PhaseBackfilled, want: true},
		{name: "one past it", phase: embedrun.PhaseCaughtUp, want: true},
		{name: "far past it", phase: embedrun.PhaseVerified, want: true},
		{name: "the phase before it", phase: embedrun.PhaseBackfilling, want: false},
		{name: "two before it", phase: embedrun.PhaseBoundaryCaptured, want: false},
		{name: "the beginning", phase: embedrun.PhaseResolved, want: false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			run := embedrun.Run{Phase: test.phase}

			c.Assert(run.Reached(embedrun.PhaseBackfilled), qt.Equals, test.want)
		})
	}
}
