package embedcatchup_test

import (
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/embedcatchup"
)

// now is a fixed instant.
var now = time.Date(2026, 8, 27, 12, 0, 0, 0, time.UTC)

// reachedBarrier is an outbox that has caught up.
func reachedBarrier() embedcatchup.Barrier {
	return embedcatchup.Barrier{
		Installed: true, Snapshot: 100, Processed: 220, Horizon: 220, Unprocessed: 0,
	}
}

// healthyWriter is a dual-write application reporting everything it owes.
func healthyWriter() embedcatchup.DualWriteEvidence {
	return embedcatchup.DualWriteEvidence{
		SupportsGeneration: "gen-new", Heartbeat: now.Add(-time.Second),
		AcknowledgedSourceVersion: "7", AcknowledgedTargetVersion: "7",
		Errors: 0, CutoverAcknowledged: true,
	}
}

// mutable is a source that can change.
func mutable() embedcatchup.SourceState {
	return embedcatchup.SourceState{Mutable: true}
}

// TestAssess_AnImmutableSourceNeedsNoMode is the case where the whole question
// does not arise.
//
// Requiring a mode here would make every migration over a static corpus
// configure machinery for changes that cannot happen.
func TestAssess_AnImmutableSourceNeedsNoMode(t *testing.T) {
	c := qt.New(t)

	guarantee := embedcatchup.Assess(embedcatchup.ModeNone,
		embedcatchup.SourceState{Mutable: false},
		embedcatchup.Barrier{}, embedcatchup.DualWriteEvidence{}, now)

	c.Assert(guarantee.Complete, qt.IsTrue)
	c.Assert(guarantee.Blockers, qt.HasLen, 0)
}

// TestAssess_AMutableSourceWithNoModeIsUnprovable is the epic's cutover rule.
func TestAssess_AMutableSourceWithNoModeIsUnprovable(t *testing.T) {
	c := qt.New(t)

	guarantee := embedcatchup.Assess(embedcatchup.ModeNone, mutable(),
		reachedBarrier(), healthyWriter(), now)

	c.Assert(guarantee.Complete, qt.IsFalse)
	c.Assert(guarantee.Blockers, qt.Contains,
		"the source can change and no consistency mode was selected, so nothing establishes "+
			"that the backfill covers the source as it is now")
}

// TestAssess_TheImmutableModeRequiresTheSourceToActuallyBePaused separates a
// mode nobody selected from a mode whose premise is false.
//
// An operator told the generic "no mode selected" message would go looking for
// a configuration they already wrote.
func TestAssess_TheImmutableModeRequiresTheSourceToActuallyBePaused(t *testing.T) {
	tests := []struct {
		name     string
		paused   bool
		complete bool
	}{
		{name: "writes are paused", paused: true, complete: true},
		{name: "writes are not", paused: false, complete: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			guarantee := embedcatchup.Assess(embedcatchup.ModeImmutable,
				embedcatchup.SourceState{Mutable: true, Paused: test.paused},
				embedcatchup.Barrier{}, embedcatchup.DualWriteEvidence{}, now)

			c.Assert(guarantee.Complete, qt.Equals, test.complete,
				qt.Commentf("%v", guarantee.Blockers))
		})
	}
}

// TestAssess_AReachedOutboxBarrierIsComplete is the control for the outbox
// mode.
func TestAssess_AReachedOutboxBarrierIsComplete(t *testing.T) {
	c := qt.New(t)

	guarantee := embedcatchup.Assess(embedcatchup.ModeOutbox, mutable(),
		reachedBarrier(), embedcatchup.DualWriteEvidence{}, now)

	c.Assert(guarantee.Complete, qt.IsTrue, qt.Commentf("%v", guarantee.Blockers))
	c.Assert(guarantee.Partial, qt.HasLen, 0)
}

// TestBarrier_RefusesEveryWayCatchUpIsNotDone walks the completion condition.
func TestBarrier_RefusesEveryWayCatchUpIsNotDone(t *testing.T) {
	tests := []struct {
		name   string
		change func(*embedcatchup.Barrier)
		want   string
	}{
		{
			name:   "the outbox was never installed",
			change: func(b *embedcatchup.Barrier) { b.Installed = false },
			want:   "the outbox is not installed on the source, so no change was ever captured",
		},
		{
			name:   "no snapshot boundary was recorded",
			change: func(b *embedcatchup.Barrier) { b.Snapshot = 0 },
			want: "no snapshot boundary was recorded, so there is nothing to say which changes " +
				"catch-up owes",
		},
		{
			name:   "catch-up has not reached the boundary",
			change: func(b *embedcatchup.Barrier) { b.Processed = 50 },
			want: "catch-up has reached transaction 50 and the backfill's boundary is 100, so " +
				"changes between them are unprocessed",
		},
		{
			name:   "events are unprocessed",
			change: func(b *embedcatchup.Barrier) { b.Unprocessed = 3 },
			want:   "3 source changes are unprocessed",
		},
		{
			name:   "nothing to process and the boundary did not move",
			change: func(b *embedcatchup.Barrier) { b.Horizon = 400 },
			want: "catch-up found nothing between transaction 220 and 400 and did not advance " +
				"past it",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			barrier := reachedBarrier()
			test.change(&barrier)

			reached, blockers := barrier.Reached()

			c.Assert(reached, qt.IsFalse)
			c.Assert(blockers, qt.Contains, test.want)
		})
	}
}

// TestBarrier_AnUninstalledOutboxSaysOnlyThat keeps a report about an absent
// mechanism from listing everything that is also unknown about it.
func TestBarrier_AnUninstalledOutboxSaysOnlyThat(t *testing.T) {
	c := qt.New(t)

	reached, blockers := embedcatchup.Barrier{}.Reached()

	c.Assert(reached, qt.IsFalse)
	c.Assert(blockers, qt.DeepEquals, []string{
		"the outbox is not installed on the source, so no change was ever captured",
	})
}

// TestAssess_DualWriteIsAlwaysPartialEvenWhenItPasses is the mode's defining
// property.
//
// An outbox row that committed is evidence. A writer saying it wrote is
// testimony, and a report that presented them alike would hide the difference
// an operator is entitled to know they are accepting.
func TestAssess_DualWriteIsAlwaysPartialEvenWhenItPasses(t *testing.T) {
	c := qt.New(t)

	guarantee := embedcatchup.Assess(embedcatchup.ModeDualWrite, mutable(),
		embedcatchup.Barrier{}, healthyWriter(), now)

	c.Assert(guarantee.Complete, qt.IsTrue, qt.Commentf("%v", guarantee.Blockers))
	c.Assert(guarantee.Partial, qt.DeepEquals, []string{
		"dual-write completeness rests on what the writer reports; Ptah observed the reports and " +
			"not the writes",
	})
}

// TestAssess_DualWriteRefusesTheEvidenceItDoesNotHave walks the six things the
// epic says a dual-write contract must define.
func TestAssess_DualWriteRefusesTheEvidenceItDoesNotHave(t *testing.T) {
	tests := []struct {
		name   string
		change func(*embedcatchup.DualWriteEvidence)
		want   string
	}{
		{
			name:   "the writer has never reported anything",
			change: func(e *embedcatchup.DualWriteEvidence) { e.Heartbeat = time.Time{} },
			want:   "the dual-write mode was selected and the writer has never reported anything",
		},
		{
			name:   "the writer names no generation",
			change: func(e *embedcatchup.DualWriteEvidence) { e.SupportsGeneration = "" },
			want:   "the writer names no generation, so nothing says it is writing the one being built",
		},
		{
			name:   "the writer has written nothing to the new generation",
			change: func(e *embedcatchup.DualWriteEvidence) { e.AcknowledgedTargetVersion = "" },
			want: "the writer has acknowledged no target version, so nothing says it has written " +
				"to the new generation at all",
		},
		{
			name:   "the writer has not said it is ready",
			change: func(e *embedcatchup.DualWriteEvidence) { e.CutoverAcknowledged = false },
			want:   "the writer has not acknowledged that it is ready to cut over",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			evidence := healthyWriter()
			test.change(&evidence)

			guarantee := embedcatchup.Assess(embedcatchup.ModeDualWrite, mutable(),
				embedcatchup.Barrier{}, evidence, now)

			c.Assert(guarantee.Complete, qt.IsFalse)
			c.Assert(guarantee.Blockers, qt.Contains, test.want)
		})
	}
}

// TestAssess_ASilentWriterSaysOnlyThat keeps a writer that was never wired up
// from reading as four separate configuration mistakes.
func TestAssess_ASilentWriterSaysOnlyThat(t *testing.T) {
	c := qt.New(t)

	guarantee := embedcatchup.Assess(embedcatchup.ModeDualWrite, mutable(),
		embedcatchup.Barrier{}, embedcatchup.DualWriteEvidence{}, now)

	c.Assert(guarantee.Blockers, qt.DeepEquals, []string{
		"the dual-write mode was selected and the writer has never reported anything",
	})
}

// TestDualWriteEvidence_CheckIsSeparateFromWhetherItExists pins the second
// question.
//
// A stale heartbeat and a missing one send an operator to different places: one
// is a writer that stopped, the other is a writer that was never wired up.
func TestDualWriteEvidence_CheckIsSeparateFromWhetherItExists(t *testing.T) {
	tests := []struct {
		name   string
		change func(*embedcatchup.DualWriteEvidence)
		want   string
	}{
		{
			name:   "a stale heartbeat",
			change: func(e *embedcatchup.DualWriteEvidence) { e.Heartbeat = now.Add(-time.Hour) },
			want:   "the writer last reported 1h0m0s ago and this policy allows 1m0s",
		},
		{
			name:   "the wrong generation",
			change: func(e *embedcatchup.DualWriteEvidence) { e.SupportsGeneration = "gen-old" },
			want:   `the writer reports generation "gen-old" and this run is building "gen-new"`,
		},
		{
			name:   "too many failed writes",
			change: func(e *embedcatchup.DualWriteEvidence) { e.Errors = 5 },
			want:   "the writer reports 5 failed writes and this policy allows 0",
		},
	}
	policy := embedcatchup.DualWritePolicy{
		MaxHeartbeatAge: time.Minute, MaxErrors: 0, Generation: "gen-new",
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			evidence := healthyWriter()
			test.change(&evidence)

			c.Assert(evidence.Check(policy, now), qt.Contains, test.want)
		})
	}
}

// TestDualWriteEvidence_AHealthyWriterPassesItsPolicy is the control.
func TestDualWriteEvidence_AHealthyWriterPassesItsPolicy(t *testing.T) {
	c := qt.New(t)
	policy := embedcatchup.DualWritePolicy{
		MaxHeartbeatAge: time.Minute, MaxErrors: 0, Generation: "gen-new",
	}

	c.Assert(healthyWriter().Check(policy, now), qt.HasLen, 0)
}

// TestParseMode_RefusesAModeThisBuildCannotAct is why external CDC is absent
// rather than present and unimplemented.
//
// A mode that could be selected and then silently did nothing is worse than one
// that cannot be selected: the operator believes changes are being captured.
func TestParseMode_RefusesAModeThisBuildCannotAct(t *testing.T) {
	tests := []struct {
		name string
		raw  string
		want embedcatchup.Mode
		ok   bool
	}{
		{name: "nothing", raw: "", want: embedcatchup.ModeNone, ok: true},
		{name: "immutable", raw: "immutable", want: embedcatchup.ModeImmutable, ok: true},
		{name: "dual write", raw: "dual_write", want: embedcatchup.ModeDualWrite, ok: true},
		{name: "outbox", raw: "outbox", want: embedcatchup.ModeOutbox, ok: true},
		{name: "debezium", raw: "debezium", want: embedcatchup.ModeNone, ok: false},
		{name: "logical replication", raw: "logical_replication", want: embedcatchup.ModeNone, ok: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			mode, err := embedcatchup.ParseMode(test.raw)

			c.Assert(err == nil, qt.Equals, test.ok, qt.Commentf("%v", err))
			c.Assert(mode, qt.Equals, test.want)
		})
	}
}
