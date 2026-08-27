package embedrun_test

import (
	"reflect"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/embedrun"
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
	c.Assert(run.Advance(stale, embedrun.PhaseCaughtUp), qt.ErrorIs, embedrun.ErrFenced)
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

// TestAdvance_NothingReachesCutoverExceptThroughVerification is why the phases
// are a machine rather than a label.
//
// Each row is a jump somebody could make by setting a field, and each one skips
// a step whose absence is invisible afterwards: a corpus cut over without
// verification looks exactly like one that passed.
func TestAdvance_NothingReachesCutoverExceptThroughVerification(t *testing.T) {
	tests := []struct {
		name string
		from embedrun.Phase
		to   embedrun.Phase
	}{
		{name: "backfilling straight to cutover", from: embedrun.PhaseBackfilling, to: embedrun.PhaseCutOver},
		{name: "prepared straight to backfilling", from: embedrun.PhasePrepared, to: embedrun.PhaseBackfilling},
		{name: "indexed straight to cutover", from: embedrun.PhaseIndexed, to: embedrun.PhaseCutOver},
		{name: "verified straight to retired", from: embedrun.PhaseVerified, to: embedrun.PhaseRetired},
		{name: "backwards", from: embedrun.PhaseVerified, to: embedrun.PhaseBackfilling},
		{name: "past a terminal phase", from: embedrun.PhaseRetired, to: embedrun.PhaseCutOver},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			run := running(test.from)

			err := run.Advance(run.FencingToken, test.to)

			c.Assert(err, qt.ErrorIs, embedrun.ErrPhase)
			c.Assert(run.Phase, qt.Equals, test.from)
		})
	}
}

// TestAdvance_WalksTheWholeLifecycle is the other side: the path the epic
// describes has to be walkable end to end, or the machine is a wall.
func TestAdvance_WalksTheWholeLifecycle(t *testing.T) {
	c := qt.New(t)
	run := running(embedrun.PhaseResolved)

	for _, phase := range []embedrun.Phase{
		embedrun.PhasePlanned, embedrun.PhasePrepared, embedrun.PhaseBoundaryCaptured,
		embedrun.PhaseBackfilling, embedrun.PhaseCaughtUp, embedrun.PhaseIndexed,
		embedrun.PhaseVerified, embedrun.PhaseCutOver, embedrun.PhaseRetired,
	} {
		c.Assert(run.Advance(run.FencingToken, phase), qt.IsNil, qt.Commentf("moving to %s", phase))
		c.Assert(run.Phase, qt.Equals, phase)
	}
}

// TestAdvance_RollbackIsReachableFromCutover pins the other branch: a run that
// cut over must be able to go back, which is what makes the rollback window
// real rather than a promise.
func TestAdvance_RollbackIsReachableFromCutover(t *testing.T) {
	c := qt.New(t)
	run := running(embedrun.PhaseCutOver)

	c.Assert(run.Advance(run.FencingToken, embedrun.PhaseRolledBack), qt.IsNil)
	c.Assert(run.Phase, qt.Equals, embedrun.PhaseRolledBack)
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
