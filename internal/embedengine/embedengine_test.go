package embedengine_test

import (
	"context"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/embedengine"
	"go.5x5.cz/ptah/internal/embedgen"
	"go.5x5.cz/ptah/internal/embedrun"
	"go.5x5.cz/ptah/internal/embedstore"
)

// at is a fixed instant, so a run's timestamps say what a test means.
var at = time.Date(2026, 8, 27, 12, 0, 0, 0, time.UTC)

// spec is the generation under test: two input fields, four dimensions, an
// empty input skipped rather than embedded.
func spec() embedgen.Spec {
	return embedgen.Spec{
		Source: embedgen.Source{
			Schema: "public", Table: "articles",
			KeyFields:   []string{"id"},
			InputFields: []string{"title", "body"},
		},
		Preprocessing: embedgen.Preprocessing{
			Separator:   "\n",
			NullPolicy:  embedgen.NullAsEmpty,
			EmptyPolicy: embedgen.EmptySkipRow,
		},
		Model: embedgen.Model{
			Provider: "fake", Identifier: "fake-model", Revision: "1",
			ReportedDimension: 4,
		},
		Target: embedgen.Target{
			Schema: "public", Table: "articles", Column: "embedding",
			Representation: "vector", Metric: embedgen.MetricCosine,
		},
	}
}

// sourceRows are four articles, one of which is empty and gets skipped.
func sourceRows() []embedgen.Row {
	return []embedgen.Row{
		{Key: []string{"1"}, Fields: []*string{new("First"), new("about pricing")}},
		{Key: []string{"2"}, Fields: []*string{new("Second"), new("about support")}},
		{Key: []string{"3"}, Fields: []*string{new(""), new("")}},
		{Key: []string{"4"}, Fields: []*string{new("Fourth"), new("about billing")}},
	}
}

// harness is an engine wired to fakes, with its run already in the store.
type harness struct {
	engine   *embedengine.Engine
	source   *fakeSource
	provider *fakeProvider
	target   *fakeTarget
	store    *embedstore.Memory
}

// newHarness builds one.
func newHarness(c *qt.C, bounds embedrun.BatchBounds) *harness {
	source := &fakeSource{rows: sourceRows(), versions: []string{"7", "7", "7", "7"}, failAfter: -1}
	provider := &fakeProvider{dimension: 4}
	store := embedstore.NewMemory()
	target := &fakeTarget{store: store}
	c.Assert(store.CreateRun(context.Background(), embedrun.Run{
		ID: "run-1", GenerationIdentity: spec().Identity().Digest,
		Phase: embedrun.PhaseBackfilling, Status: embedrun.StatusRunning,
		LeaseOwner: "worker-a", FencingToken: 1, CreatedAt: at, UpdatedAt: at,
	}), qt.IsNil)
	return &harness{
		engine: &embedengine.Engine{
			Spec: spec(), Source: source, Provider: provider, Target: target, Store: store,
			Bounds: bounds, Worker: "worker-a", Now: func() time.Time { return at },
		},
		source: source, provider: provider, target: target, store: store,
	}
}

// defaultBounds read two rows at a time and send two inputs at a time.
func defaultBounds() embedrun.BatchBounds {
	return embedrun.BatchBounds{MaxRows: 2, MaxInputs: 2}
}

// TestBackfill_EmbedsEveryRowAndCheckpointsWhatItWrote is the control.
func TestBackfill_EmbedsEveryRowAndCheckpointsWhatItWrote(t *testing.T) {
	c := qt.New(t)
	h := newHarness(c, defaultBounds())

	run, _, err := h.engine.Backfill(context.Background(), "run-1")

	c.Assert(err, qt.IsNil)
	c.Assert(run.Progress.RowsScanned, qt.Equals, int64(4))
	c.Assert(run.Progress.RowsEmbedded, qt.Equals, int64(3))
	c.Assert(run.Progress.RowsSkipped, qt.Equals, int64(1))
	c.Assert(run.Cursor, qt.DeepEquals, []string{"4"})
	// Two rows per scan across four rows: two pages that had rows and one that
	// reported the end. A scan limit that was ignored would read them all at
	// once and this count would say so.
	c.Assert(h.source.scans, qt.Equals, 2)
	stored, err := h.store.Run(context.Background(), "run-1")
	c.Assert(err, qt.IsNil)
	c.Assert(stored.Cursor, qt.DeepEquals, []string{"4"})
}

// TestBackfill_TheVectorsAndTheCheckpointAreOneTransaction is the ordering the
// package exists for.
//
// Every commit carries both, and the cursor it carries is the cursor for the
// rows in the same call. A checkpoint written first would leave a resumed run
// past rows nothing embedded, and the resumed run would look completely
// healthy.
func TestBackfill_TheVectorsAndTheCheckpointAreOneTransaction(t *testing.T) {
	c := qt.New(t)
	h := newHarness(c, defaultBounds())

	_, _, err := h.engine.Backfill(context.Background(), "run-1")

	c.Assert(err, qt.IsNil)
	c.Assert(h.target.commits, qt.HasLen, 2)
	c.Assert(h.target.commits[0].cursor, qt.DeepEquals, []string{"2"})
	c.Assert(lastKeyOf(h.target.commits[0].writes), qt.DeepEquals, []string{"2"})
	c.Assert(h.target.commits[1].cursor, qt.DeepEquals, []string{"4"})
	c.Assert(lastKeyOf(h.target.commits[1].writes), qt.DeepEquals, []string{"4"})
}

// TestBackfill_NothingIsCheckpointedThatWasNotCommitted is the failure side of
// the same rule.
//
// The target refuses the second commit. The run in memory had already advanced
// its cursor past those rows -- that is what Checkpoint does -- and what the
// store ends up holding must be the cursor from BEFORE them.
func TestBackfill_NothingIsCheckpointedThatWasNotCommitted(t *testing.T) {
	c := qt.New(t)
	h := newHarness(c, defaultBounds())
	h.target.failOn = 2

	run, _, err := h.engine.Backfill(context.Background(), "run-1")

	c.Assert(err, qt.ErrorMatches, `target: the target rejected the write`)
	c.Assert(run.Cursor, qt.DeepEquals, []string{"2"})
	c.Assert(run.Status, qt.Equals, embedrun.StatusFailed)
	stored, storeErr := h.store.Run(context.Background(), "run-1")
	c.Assert(storeErr, qt.IsNil)
	c.Assert(stored.Cursor, qt.DeepEquals, []string{"2"})
	c.Assert(stored.Progress.RowsScanned, qt.Equals, int64(2))
}

// TestBackfill_ResumesFromTheCheckpointRatherThanTheBeginning is what the
// cursor is for.
//
// The second run starts from a store the first one left mid-scan, and the
// source is asked for the page after the cursor rather than for the first page
// again.
func TestBackfill_ResumesFromTheCheckpointRatherThanTheBeginning(t *testing.T) {
	c := qt.New(t)
	h := newHarness(c, defaultBounds())
	h.target.failOn = 2
	_, _, err := h.engine.Backfill(context.Background(), "run-1")
	c.Assert(err, qt.IsNotNil)

	resumed := newHarness(c, defaultBounds())
	stored, err := h.store.Run(context.Background(), "run-1")
	c.Assert(err, qt.IsNil)
	stored.Status = embedrun.StatusRunning
	c.Assert(resumed.store.CreateRun(context.Background(), stored), qt.ErrorIs, embedstore.ErrConflict)
	c.Assert(resumed.store.SaveRun(context.Background(), stored), qt.IsNil)

	run, _, err := resumed.engine.Backfill(context.Background(), "run-1")

	c.Assert(err, qt.IsNil)
	c.Assert(run.Cursor, qt.DeepEquals, []string{"4"})
	// Four scanned in total, not six: the first two were not read again.
	c.Assert(run.Progress.RowsScanned, qt.Equals, int64(4))
	c.Assert(resumed.provider.calls, qt.HasLen, 1)
	c.Assert(resumed.provider.calls[0], qt.DeepEquals, []string{"Fourth\nabout billing"})
}

// TestBackfill_ASkippedRowIsNotSentToTheProviderAndIsStillWritten is the epic's
// rule about deliberate gaps.
//
// The empty row is not embedded -- there is nothing to embed -- and it still
// produces a target write, because verification reads a skip as something the
// specification asked for rather than as a row nobody got to. Dropping it here
// would make the two indistinguishable.
func TestBackfill_ASkippedRowIsNotSentToTheProviderAndIsStillWritten(t *testing.T) {
	c := qt.New(t)
	h := newHarness(c, embedrun.BatchBounds{MaxRows: 4, MaxInputs: 4})

	_, _, err := h.engine.Backfill(context.Background(), "run-1")

	c.Assert(err, qt.IsNil)
	c.Assert(h.provider.calls, qt.HasLen, 1)
	c.Assert(h.provider.calls[0], qt.DeepEquals, []string{
		"First\nabout pricing", "Second\nabout support", "Fourth\nabout billing",
	})
	c.Assert(h.target.commits, qt.HasLen, 1)
	c.Assert(h.target.commits[0].writes, qt.HasLen, 4)
	c.Assert(h.target.commits[0].writes[2].Kind, qt.Equals, embedrun.WriteSkip)
	c.Assert(h.target.commits[0].writes[2].Vector, qt.HasLen, 0)
	c.Assert(h.target.commits[0].writes[3].Kind, qt.Equals, embedrun.WriteUpsert)
}

// TestBackfill_TheVectorsLandOnTheRowsTheyWereComputedFrom is what a skipped
// row in the middle of a batch makes possible to get wrong.
//
// Three inputs go out and four writes come back, so the provider's answers are
// consumed at a different rate than the batch is walked. An off-by-one here
// gives every row after the skip somebody else's vector, and every count in the
// system still agrees.
func TestBackfill_TheVectorsLandOnTheRowsTheyWereComputedFrom(t *testing.T) {
	c := qt.New(t)
	h := newHarness(c, embedrun.BatchBounds{MaxRows: 4, MaxInputs: 4})

	_, _, err := h.engine.Backfill(context.Background(), "run-1")

	c.Assert(err, qt.IsNil)
	writes := h.target.commits[0].writes
	// The fake's vectors start at the input's length, so the vector says which
	// text produced it.
	c.Assert(writes[0].Vector[0], qt.Equals, float32(len("First\nabout pricing")))
	c.Assert(writes[1].Vector[0], qt.Equals, float32(len("Second\nabout support")))
	c.Assert(writes[3].Vector[0], qt.Equals, float32(len("Fourth\nabout billing")))
}

// TestBackfill_EachWriteCarriesWhatItWasComputedFrom is what makes freshness
// answerable later.
func TestBackfill_EachWriteCarriesWhatItWasComputedFrom(t *testing.T) {
	c := qt.New(t)
	h := newHarness(c, embedrun.BatchBounds{MaxRows: 4, MaxInputs: 4})

	_, _, err := h.engine.Backfill(context.Background(), "run-1")

	c.Assert(err, qt.IsNil)
	write := h.target.commits[0].writes[0]
	c.Assert(write.Generation, qt.Equals, spec().Identity().Digest)
	c.Assert(write.Version, qt.Equals, "7")
	input, err := spec().Canonicalize(sourceRows()[0])
	c.Assert(err, qt.IsNil)
	c.Assert(write.InputHash, qt.Equals, spec().SourceInputHash(input))
}

// TestBackfill_AProviderThatAnsweredShortIsRefused keeps a partial answer from
// becoming a partial corpus.
//
// The provider returns two vectors for three inputs. Nothing about the response
// says which input went unanswered, so the batch is a failure rather than two
// thirds of a success.
func TestBackfill_AProviderThatAnsweredShortIsRefused(t *testing.T) {
	c := qt.New(t)
	h := newHarness(c, embedrun.BatchBounds{MaxRows: 4, MaxInputs: 4})
	h.provider.shortBy = 1

	run, _, err := h.engine.Backfill(context.Background(), "run-1")

	c.Assert(err, qt.ErrorMatches, `provider: .*`)
	c.Assert(h.target.commits, qt.HasLen, 0)
	c.Assert(run.Status, qt.Equals, embedrun.StatusFailed)
	c.Assert(run.Cursor, qt.HasLen, 0)
}

// TestBackfill_FailuresAreClassifiedByWhatFailed keeps an operator from having
// to read a stack trace to know which system to look at.
func TestBackfill_FailuresAreClassifiedByWhatFailed(t *testing.T) {
	tests := []struct {
		name   string
		breaks func(*harness)
		want   string
	}{
		{
			name:   "the source",
			breaks: func(h *harness) { h.source.failAfter = 0 },
			want:   "source",
		},
		{
			name:   "the provider",
			breaks: func(h *harness) { h.provider.failOn = 1 },
			want:   "provider",
		},
		{
			name:   "the target",
			breaks: func(h *harness) { h.target.failOn = 1 },
			want:   "target",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			h := newHarness(c, defaultBounds())
			test.breaks(h)

			run, _, err := h.engine.Backfill(context.Background(), "run-1")

			c.Assert(err, qt.ErrorMatches, test.want+`: .*`)
			c.Assert(run.FailureClass, qt.Equals, test.want)
			c.Assert(run.Status, qt.Equals, embedrun.StatusFailed)
		})
	}
}

// TestBackfill_AFailureIsRecordedInTheTrail keeps a failed run explicable.
func TestBackfill_AFailureIsRecordedInTheTrail(t *testing.T) {
	c := qt.New(t)
	h := newHarness(c, defaultBounds())
	h.provider.failOn = 2

	_, _, err := h.engine.Backfill(context.Background(), "run-1")
	c.Assert(err, qt.IsNotNil)

	events, err := h.store.Events(context.Background(), "run-1")
	c.Assert(err, qt.IsNil)
	c.Assert(events, qt.HasLen, 2)
	c.Assert(string(events[0].Kind), qt.Equals, "checkpoint")
	c.Assert(string(events[1].Kind), qt.Equals, "failed")
	c.Assert(events[1].Detail, qt.Contains, "the provider returned 503")
}

// TestBackfill_ACancelledRunStopsWithoutFailing keeps an interruption from
// looking like a defect.
//
// The run is durable at its last checkpoint and another process can pick it up.
// Marking it failed would make an operator investigate a deploy.
func TestBackfill_ACancelledRunStopsWithoutFailing(t *testing.T) {
	c := qt.New(t)
	h := newHarness(c, defaultBounds())
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	run, _, err := h.engine.Backfill(ctx, "run-1")

	c.Assert(err, qt.ErrorIs, embedengine.ErrAborted)
	c.Assert(run.Status, qt.Not(qt.Equals), embedrun.StatusFailed)
	c.Assert(h.target.commits, qt.HasLen, 0)
}

// TestBackfill_ATakenOverRunStopsRatherThanFightingForIt is the fencing token
// reaching the engine.
//
// Another worker claimed the run while this one was mid-batch. Its target write
// already landed -- that transaction was committed before the store refused --
// and what it must not do is keep going.
func TestBackfill_ATakenOverRunStopsRatherThanFightingForIt(t *testing.T) {
	c := qt.New(t)
	h := newHarness(c, defaultBounds())
	// The takeover happens between this worker reading the run and its second
	// transaction, which is the window a lease check in the worker cannot see:
	// it checked, and it was true then.
	h.target.beforeCommit = stealAfter(c, h, 1)

	run, _, err := h.engine.Backfill(context.Background(), "run-1")

	c.Assert(err, qt.ErrorIs, embedengine.ErrFenced)
	c.Assert(run.LeaseOwner, qt.Equals, "worker-b")
	c.Assert(h.target.commits, qt.HasLen, 1)
	c.Assert(run.Status, qt.Not(qt.Equals), embedrun.StatusFailed)
}

// TestBackfill_AnEmptySourceCompletesWithoutAskingTheProvider is the degenerate
// case, and it must not be a failure.
func TestBackfill_AnEmptySourceCompletesWithoutAskingTheProvider(t *testing.T) {
	c := qt.New(t)
	h := newHarness(c, defaultBounds())
	h.source.rows = nil
	h.source.versions = nil

	run, _, err := h.engine.Backfill(context.Background(), "run-1")

	c.Assert(err, qt.IsNil)
	c.Assert(run.Progress.RowsScanned, qt.Equals, int64(0))
	c.Assert(h.provider.calls, qt.HasLen, 0)
	c.Assert(h.target.commits, qt.HasLen, 0)
}

// TestBackfill_ProviderUsageIsAccumulated gives an operator the cost view the
// run promises.
func TestBackfill_ProviderUsageIsAccumulated(t *testing.T) {
	c := qt.New(t)
	h := newHarness(c, defaultBounds())

	run, _, err := h.engine.Backfill(context.Background(), "run-1")

	c.Assert(err, qt.IsNil)
	// Three inputs across two calls, and the fake reports one prompt token per
	// input and two total.
	c.Assert(run.Progress.ProviderPromptTokens, qt.Equals, int64(3))
	c.Assert(run.Progress.ProviderTotalTokens, qt.Equals, int64(6))
}

// lastKeyOf returns the key of a batch's last write.
func lastKeyOf(writes []embedrun.TargetWrite) []string {
	if len(writes) == 0 {
		return nil
	}
	return writes[len(writes)-1].Key
}

// stealAfter hands the run to another worker once that many transactions have
// landed.
//
// The branch lives here rather than in the test body because a test asserts and
// does not branch, which is the rule scripts/check-test-style.sh enforces.
func stealAfter(c *qt.C, h *harness, commits int) func() {
	return func() {
		if len(h.target.commits) != commits {
			return
		}
		stolen, err := h.store.Run(context.Background(), "run-1")
		c.Assert(err, qt.IsNil)
		stolen.FencingToken = 99
		stolen.LeaseOwner = "worker-b"
		c.Assert(h.store.SaveRun(context.Background(), stolen), qt.IsNil)
	}
}

// TestBackfill_ABatchOfOnlySkippedRowsAsksTheProviderNothing keeps an empty
// request from going out.
//
// Every row in the page is skipped, so there is no text to embed. A request
// with no inputs is a round trip, a rate-limit slot and a line in somebody's
// audit log, for an answer that is known to be empty.
func TestBackfill_ABatchOfOnlySkippedRowsAsksTheProviderNothing(t *testing.T) {
	c := qt.New(t)
	h := newHarness(c, defaultBounds())
	h.source.rows = []embedgen.Row{
		{Key: []string{"1"}, Fields: []*string{new(""), new("")}},
		{Key: []string{"2"}, Fields: []*string{new(""), new("")}},
	}
	h.source.versions = []string{"7", "7"}

	run, _, err := h.engine.Backfill(context.Background(), "run-1")

	c.Assert(err, qt.IsNil)
	c.Assert(h.provider.calls, qt.HasLen, 0)
	c.Assert(run.Progress.RowsSkipped, qt.Equals, int64(2))
	c.Assert(h.target.commits, qt.HasLen, 1)
	c.Assert(h.target.commits[0].writes, qt.HasLen, 2)
}

// TestBackfill_ASourceThatDoesNotAdvanceIsRefused is the failure a loop cannot
// notice from the inside.
//
// A scan answering from the same position forever embeds one page of rows until
// somebody reads the provider bill. Every count in the run keeps rising, the
// phase stays right, and the corpus does not grow.
func TestBackfill_ASourceThatDoesNotAdvanceIsRefused(t *testing.T) {
	c := qt.New(t)
	h := newHarness(c, defaultBounds())
	h.source.stalled = true

	run, _, err := h.engine.Backfill(context.Background(), "run-1")

	c.Assert(err, qt.ErrorIs, embedengine.ErrStalled)
	c.Assert(run.Status, qt.Equals, embedrun.StatusFailed)
	c.Assert(run.FailureClass, qt.Equals, "source")
	// One page went through, and the second was refused before it reached the
	// provider: a first page that advances and a first page that does not look
	// identical until the second scan comes back from the same place, and from
	// there the answer costs nothing to give.
	c.Assert(h.target.commits, qt.HasLen, 1)
	c.Assert(h.provider.calls, qt.HasLen, 1)
}

// TestBackfill_AnEmptyPageStillMakesItsPositionDurable is what a filtered scan
// needs.
//
// A filter matching nothing for a stretch answers with no rows and is not done.
// The position it reached has to become durable, or the next run reads the same
// empty stretch again -- for a corpus where the gap is large, forever.
func TestBackfill_AnEmptyPageStillMakesItsPositionDurable(t *testing.T) {
	c := qt.New(t)
	h := newHarness(c, defaultBounds())
	h.source.emptyPagesBefore = 1

	run, _, err := h.engine.Backfill(context.Background(), "run-1")

	c.Assert(err, qt.IsNil)
	c.Assert(run.Cursor, qt.DeepEquals, []string{"4"})
	c.Assert(run.Progress.RowsEmbedded, qt.Equals, int64(3))
	// The empty page committed a checkpoint of its own, with nothing in it.
	c.Assert(h.target.commits, qt.HasLen, 3)
	c.Assert(h.target.commits[0].writes, qt.HasLen, 0)
	c.Assert(h.target.commits[0].cursor, qt.DeepEquals, []string{"0"})
}

// TestBackfill_AnEmptyPageThatSaysNothingIsRefused covers the two ways a page
// with no rows fails to be progress.
//
// It is the same defect as a repeated row, one level quieter: nothing comes
// back, so nothing looks wrong, and the next scan asks the question the last
// one did not answer. A page reporting no position at all is the worse of the
// two -- committing its cursor would erase the one the run already had.
func TestBackfill_AnEmptyPageThatSaysNothingIsRefused(t *testing.T) {
	tests := []struct {
		name string
		mode string
	}{
		{name: "it hands back the cursor it was given", mode: "same"},
		{name: "it reports no position at all", mode: "none"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			h := newHarness(c, defaultBounds())
			// The second scan, not the first: with an empty cursor the two
			// answers are the same bytes, and a fixture that used the first
			// scan would let either rule stand in for the other.
			h.source.emptyPageOnScan = 2
			h.source.emptyPageMode = test.mode

			run, _, err := h.engine.Backfill(context.Background(), "run-1")

			c.Assert(err, qt.ErrorIs, embedengine.ErrStalled)
			c.Assert(run.FailureClass, qt.Equals, "source")
			c.Assert(h.target.commits, qt.HasLen, 1)
			c.Assert(run.Cursor, qt.DeepEquals, []string{"2"})
		})
	}
}

// TestBackfill_AnEmptyFinalPageIsTheEndAndNotAStall keeps the guard from firing
// on the ordinary way a scan finishes.
func TestBackfill_AnEmptyFinalPageIsTheEndAndNotAStall(t *testing.T) {
	c := qt.New(t)
	h := newHarness(c, embedrun.BatchBounds{MaxRows: 4, MaxInputs: 4})
	h.source.rows = sourceRows()[:4]

	run, _, err := h.engine.Backfill(context.Background(), "run-1")

	c.Assert(err, qt.IsNil)
	c.Assert(run.Progress.RowsScanned, qt.Equals, int64(4))
}

// TestBackfill_AnInterruptIsNotAProviderFailure covers stokaro/ptah#2649
// finding 10.
//
// `fail` did its bookkeeping on the caller's context, so an interrupt arriving
// mid-request took whichever subsystem was in flight and reported that as the
// cause -- and then failed to record anything, because its first act was a
// store read on the same dead context. The operator got a cause that was not
// the cause, a second line about a reload nobody asked for, and a run left
// `running` with no failure class.
//
// The assertions are the three separate consequences, because a repair that
// fixed only the message would leave the run unmarked and a repair that only
// detached the context would still blame the provider.
func TestBackfill_AnInterruptIsNotAProviderFailure(t *testing.T) {
	c := qt.New(t)
	h := newHarness(c, defaultBounds())
	ctx, cancel := context.WithCancel(context.Background())
	h.provider.beforeEmbed = cancel

	run, _, err := h.engine.Backfill(ctx, "run-1")

	c.Assert(err, qt.ErrorIs, embedengine.ErrAborted)
	c.Assert(err.Error(), qt.Not(qt.Contains), "reload run")
	c.Assert(run.Status, qt.Equals, embedrun.StatusRunning)
	c.Assert(run.FailureClass, qt.Equals, "")
	c.Assert(failedRunEvents(c, h, "run-1"), qt.Equals, 0)
}

// TestBackfill_AProviderFailureIsStillAProviderFailure is the control.
//
// Every assertion above is satisfied by an engine that stopped recording
// failures at all. This is the same loop, the same call site, with a live
// context and a provider that will not answer.
func TestBackfill_AProviderFailureIsStillAProviderFailure(t *testing.T) {
	c := qt.New(t)
	h := newHarness(c, defaultBounds())
	h.provider.failOn = 1

	run, _, err := h.engine.Backfill(context.Background(), "run-1")

	c.Assert(err, qt.Not(qt.ErrorIs), embedengine.ErrAborted)
	c.Assert(err, qt.ErrorMatches, `provider: the provider returned 503`)
	c.Assert(run.Status, qt.Equals, embedrun.StatusFailed)
	c.Assert(run.FailureClass, qt.Equals, "provider")
	c.Assert(failedRunEvents(c, h, "run-1"), qt.Equals, 1)
}

// failedRunEvents counts the run's failure events.
func failedRunEvents(c *qt.C, h *harness, runID string) int {
	c.Helper()
	events, err := h.store.Events(context.Background(), runID)
	c.Assert(err, qt.IsNil)
	count := 0
	for _, event := range events {
		if event.Kind == embedrun.EventFailed {
			count++
		}
	}
	return count
}
