package embedengine_test

import (
	"cmp"
	"context"
	"errors"
	"slices"
	"strconv"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/embedcatchup"
	"go.5x5.cz/ptah/internal/embedgen"
	"go.5x5.cz/ptah/internal/embedrun"
)

// fakeChanges answers pages of settled events.
type fakeChanges struct {
	// pages are the answers, in order. An empty page ends the loop.
	pages [][]embedcatchup.Event
	// horizons are the boundaries each page was read up to.
	horizons []uint64
	// asked records the cursor each call was given, which is what makes "it
	// resumed rather than starting over" assertable.
	asked []embedcatchup.Cursor
	// failOn makes the call fail on that number, zero for never.
	failOn int
	calls  int
}

// Since answers the next page.
func (f *fakeChanges) Since(
	_ context.Context, from embedcatchup.Cursor, _ int,
) ([]embedcatchup.Event, uint64, error) {
	f.calls++
	f.asked = append(f.asked, from)
	if f.failOn == f.calls {
		return nil, 0, errors.New("the outbox was unreadable")
	}
	if f.calls > len(f.pages) {
		return nil, lastHorizon(f.horizons), nil
	}
	return f.pages[f.calls-1], f.horizons[f.calls-1], nil
}

// lastHorizon is the final boundary a source reports.
func lastHorizon(horizons []uint64) uint64 {
	if len(horizons) == 0 {
		return 1
	}
	return horizons[len(horizons)-1]
}

// fakeRereader answers with the rows that still exist.
type fakeRereader struct {
	// rows are the source rows still present, by key identity.
	rows map[string]embedgen.Row
	// versions are their current versions, by key identity.
	versions map[string]string
	// asked records the keys each call was given.
	asked [][]string
	// unreadable makes the reread fail, which is the source's own class of
	// failure rather than the provider's.
	unreadable bool
}

// Current returns the rows still present.
func (f *fakeRereader) Current(_ context.Context, keys [][]string) ([]embedgen.Row, []string, error) {
	if f.unreadable {
		return nil, nil, errors.New("the source was unreadable")
	}
	var rows []embedgen.Row
	var versions []string
	for _, key := range keys {
		f.asked = append(f.asked, key)
		identity := embedcatchup.KeyIdentity(key)
		row, found := f.rows[identity]
		if !found {
			continue
		}
		rows = append(rows, row)
		versions = append(versions, f.versions[identity])
	}
	return rows, versions, nil
}

// pagedLog is a Changes that pages the way a real outbox does.
//
// fakeChanges hands back pages a test wrote out, which cannot express a page
// the LIMIT cut: the cursor it is given is recorded and then ignored. This one
// holds an event log and answers from it, so what a page contains follows from
// the cursor and the limit rather than from the test -- which is what makes a
// cursor that steps over an unread event observable at this level at all.
type pagedLog struct {
	// events is the whole log, in no particular order.
	events []embedcatchup.Event
	// horizon is the boundary below which every transaction has concluded.
	horizon uint64
}

// Since answers the events the cursor still owes, in sequence order.
func (l *pagedLog) Since(
	_ context.Context, from embedcatchup.Cursor, limit int,
) ([]embedcatchup.Event, uint64, error) {
	pending := make([]embedcatchup.Event, 0, len(l.events))
	for _, event := range l.events {
		owed := event.Transaction >= from.Transaction &&
			(event.Transaction > from.Transaction || event.Sequence >= from.Sequence)
		if event.Transaction < l.horizon && owed {
			pending = append(pending, event)
		}
	}
	slices.SortFunc(pending, func(left, right embedcatchup.Event) int {
		return cmp.Or(
			cmp.Compare(left.Transaction, right.Transaction),
			cmp.Compare(left.Sequence, right.Sequence))
	})
	page := pending[:min(limit, len(pending))]
	slices.SortFunc(page, func(left, right embedcatchup.Event) int {
		return cmp.Compare(left.Sequence, right.Sequence)
	})
	return page, l.horizon, nil
}

// changed is one settled event.
func changed(transaction uint64, sequence int64, key string, operation embedcatchup.Operation) embedcatchup.Event {
	return embedcatchup.Event{
		Transaction: transaction, Sequence: sequence, Key: []string{key},
		Operation: operation, Version: "9",
	}
}

// livingRows is a reread that finds every row the test names present.
func livingRows(keys ...string) *fakeRereader {
	reader := &fakeRereader{
		rows:     make(map[string]embedgen.Row, len(keys)),
		versions: make(map[string]string, len(keys)),
	}
	for _, key := range keys {
		identity := embedcatchup.KeyIdentity([]string{key})
		reader.rows[identity] = embedgen.Row{
			Key: []string{key}, Fields: []*string{new("Changed"), new("body " + key)},
		}
		reader.versions[identity] = "9"
	}
	return reader
}

// caughtUp runs a catch-up over a harness whose run already has a boundary.
func caughtUp(
	c *qt.C, h *harness, changes *fakeChanges, source *fakeRereader,
) (embedrun.Run, embedrun.Progress, error) {
	c.Helper()
	stored, err := h.store.Run(context.Background(), "run-1")
	c.Assert(err, qt.IsNil)
	stored.SnapshotWatermark = "100"
	c.Assert(h.store.SaveRun(context.Background(), stored), qt.IsNil)
	return h.engine.CatchUp(context.Background(), "run-1", changes, source)
}

// TestCatchUp_RereadsAndEmbedsEveryChangedRow is the control.
func TestCatchUp_RereadsAndEmbedsEveryChangedRow(t *testing.T) {
	c := qt.New(t)
	h := newHarness(c, defaultBounds())
	changes := &fakeChanges{
		pages:    [][]embedcatchup.Event{{changed(101, 1, "1", embedcatchup.OperationUpdate)}},
		horizons: []uint64{102},
	}

	run, _, err := caughtUp(c, h, changes, livingRows("1"))

	c.Assert(err, qt.IsNil)
	c.Assert(h.provider.calls, qt.HasLen, 1)
	c.Assert(h.provider.calls[0], qt.DeepEquals, []string{"Changed\nbody 1"})
	c.Assert(run.Progress.RowsEmbedded, qt.Equals, int64(1))
	c.Assert(run.CatchUpWatermark, qt.Equals, "102")
}

// TestCatchUp_ResumesFromTheSnapshotBoundaryAndThenFromItself is what the two
// watermarks are for.
//
// The first page is asked for from the boundary the backfill recorded, and
// every page after that from where the last one got to. A catch-up that started
// from the boundary each time would re-embed the same rows for as long as the
// source keeps changing.
//
// Each page here holds one event against a limit of two, so none of them was
// truncated and each carries the cursor to its own horizon rather than to the
// event it happened to end on: everything below that horizon was returned, so
// re-reading the range between the two would find nothing.
func TestCatchUp_ResumesFromTheSnapshotBoundaryAndThenFromItself(t *testing.T) {
	c := qt.New(t)
	h := newHarness(c, defaultBounds())
	changes := &fakeChanges{
		pages: [][]embedcatchup.Event{
			{changed(101, 1, "1", embedcatchup.OperationUpdate)},
			{changed(150, 2, "2", embedcatchup.OperationUpdate)},
		},
		horizons: []uint64{151, 200},
	}

	run, _, err := caughtUp(c, h, changes, livingRows("1", "2"))

	c.Assert(err, qt.IsNil)
	c.Assert(changes.asked, qt.DeepEquals, []embedcatchup.Cursor{
		{Transaction: 100}, {Transaction: 151}, {Transaction: 200},
	})
	c.Assert(run.CatchUpWatermark, qt.Equals, "200")
}

// TestCatchUp_APageCutInsideATransactionStillProcessesTheRest is
// stokaro/ptah#2628.
//
// One transaction writes three rows and the page holds two. A cursor that
// advances to the highest transaction the page held puts the third event below
// itself, where no later page can reach it and no later run can either: the
// outbox reports nothing unprocessed, the run reads caught up, and the row
// keeps whatever vector it had. Every key the transaction touched has to be
// embedded, and the run has to end at the horizon rather than short of it.
func TestCatchUp_APageCutInsideATransactionStillProcessesTheRest(t *testing.T) {
	c := qt.New(t)
	h := newHarness(c, defaultBounds())
	changes := &pagedLog{
		events: []embedcatchup.Event{
			changed(500, 1, "1", embedcatchup.OperationInsert),
			changed(500, 2, "2", embedcatchup.OperationInsert),
			changed(500, 3, "3", embedcatchup.OperationInsert),
		},
		horizon: 501,
	}

	stored, err := h.store.Run(context.Background(), "run-1")
	c.Assert(err, qt.IsNil)
	stored.SnapshotWatermark = "100"
	c.Assert(h.store.SaveRun(context.Background(), stored), qt.IsNil)
	run, _, err := h.engine.CatchUp(context.Background(), "run-1", changes, livingRows("1", "2", "3"))

	c.Assert(err, qt.IsNil)
	c.Assert(embeddedKeys(h.target.commits), qt.DeepEquals, []string{"1", "2", "3"})
	c.Assert(run.CatchUpWatermark, qt.Equals, "501")
}

// TestCatchUp_ATransactionSpanningManyPagesLosesNothing is the same defect at
// the size that hides it.
//
// The transaction is five pages deep, so a cursor that cannot address a
// position inside a transaction drops four pages' worth rather than one row.
func TestCatchUp_ATransactionSpanningManyPagesLosesNothing(t *testing.T) {
	c := qt.New(t)
	h := newHarness(c, defaultBounds())
	log := &pagedLog{horizon: 900}
	var want []string
	for row := 1; row <= 10; row++ {
		key := strconv.Itoa(row)
		log.events = append(log.events,
			changed(800, int64(row), key, embedcatchup.OperationUpdate))
		want = append(want, key)
	}
	slices.Sort(want)

	stored, err := h.store.Run(context.Background(), "run-1")
	c.Assert(err, qt.IsNil)
	stored.SnapshotWatermark = "100"
	c.Assert(h.store.SaveRun(context.Background(), stored), qt.IsNil)
	run, _, err := h.engine.CatchUp(context.Background(), "run-1", log, livingRows(want...))

	c.Assert(err, qt.IsNil)
	c.Assert(embeddedKeys(h.target.commits), qt.DeepEquals, want)
	c.Assert(run.CatchUpWatermark, qt.Equals, "900")
}

// TestCatchUp_APageEndingOnATransactionEdgeResumesAtTheNextOne is the control
// for the two above.
//
// Every event sits in a transaction of its own, so no page can be cut inside
// one and the pair cursor has nothing to do that the transaction alone did not.
// Without it, a conversion that lost the boundary case would look like a fix.
func TestCatchUp_APageEndingOnATransactionEdgeResumesAtTheNextOne(t *testing.T) {
	c := qt.New(t)
	h := newHarness(c, defaultBounds())
	changes := &pagedLog{
		events: []embedcatchup.Event{
			changed(600, 1, "1", embedcatchup.OperationUpdate),
			changed(601, 2, "2", embedcatchup.OperationUpdate),
			changed(602, 3, "3", embedcatchup.OperationUpdate),
		},
		horizon: 700,
	}

	stored, err := h.store.Run(context.Background(), "run-1")
	c.Assert(err, qt.IsNil)
	stored.SnapshotWatermark = "100"
	c.Assert(h.store.SaveRun(context.Background(), stored), qt.IsNil)
	run, _, err := h.engine.CatchUp(context.Background(), "run-1", changes, livingRows("1", "2", "3"))

	c.Assert(err, qt.IsNil)
	c.Assert(embeddedKeys(h.target.commits), qt.DeepEquals, []string{"1", "2", "3"})
	c.Assert(run.CatchUpWatermark, qt.Equals, "700")
}

// embeddedKeys is every key a run wrote a vector for, in order and deduplicated.
func embeddedKeys(commits []commit) []string {
	var keys []string
	for _, written := range commits {
		for _, write := range written.writes {
			keys = append(keys, write.Key...)
		}
	}
	slices.Sort(keys)
	return slices.Compact(keys)
}

// TestCatchUp_RefusesToStartWithoutABoundary keeps "every change ever recorded"
// from being the default.
//
// Zero is a valid transaction identity and it means the whole outbox. On a
// long-lived one that is a different migration, run by accident.
func TestCatchUp_RefusesToStartWithoutABoundary(t *testing.T) {
	c := qt.New(t)
	h := newHarness(c, defaultBounds())

	_, _, err := h.engine.CatchUp(context.Background(), "run-1",
		&fakeChanges{}, livingRows())

	c.Assert(err, qt.ErrorMatches,
		`.*the run records no snapshot boundary, so nothing says which changes catch-up owes`)
}

// TestCatchUp_CollapsesRepeatedChangesIntoOneRequest is the epic's rule, at the
// place it saves the money.
//
// Five updates to one row during a backfill are five provider requests for four
// vectors nobody will read.
func TestCatchUp_CollapsesRepeatedChangesIntoOneRequest(t *testing.T) {
	c := qt.New(t)
	h := newHarness(c, defaultBounds())
	changes := &fakeChanges{
		pages: [][]embedcatchup.Event{{
			changed(101, 1, "1", embedcatchup.OperationUpdate),
			changed(102, 2, "1", embedcatchup.OperationUpdate),
			changed(103, 3, "1", embedcatchup.OperationUpdate),
		}},
		horizons: []uint64{104},
	}

	run, _, err := caughtUp(c, h, changes, livingRows("1"))

	c.Assert(err, qt.IsNil)
	c.Assert(h.provider.calls, qt.HasLen, 1)
	c.Assert(h.provider.calls[0], qt.HasLen, 1)
	c.Assert(run.Progress.RowsScanned, qt.Equals, int64(1))
}

// TestCatchUp_ADeletedRowIsTombstonedRatherThanEmbedded walks the delete path.
func TestCatchUp_ADeletedRowIsTombstonedRatherThanEmbedded(t *testing.T) {
	c := qt.New(t)
	h := newHarness(c, defaultBounds())
	changes := &fakeChanges{
		pages:    [][]embedcatchup.Event{{changed(101, 1, "9", embedcatchup.OperationDelete)}},
		horizons: []uint64{102},
	}

	run, _, err := caughtUp(c, h, changes, livingRows())

	c.Assert(err, qt.IsNil)
	c.Assert(h.provider.calls, qt.HasLen, 0)
	c.Assert(h.target.commits, qt.HasLen, 1)
	c.Assert(h.target.commits[0].writes, qt.HasLen, 1)
	c.Assert(h.target.commits[0].writes[0].Kind, qt.Equals, embedrun.WriteTombstone)
	c.Assert(h.target.commits[0].writes[0].Key, qt.DeepEquals, []string{"9"})
	c.Assert(run.Progress.RowsDeleted, qt.Equals, int64(1))
}

// TestCatchUp_TheRereadDecidesAndNotTheEvent is why an event carrying only a
// key is enough.
//
// The last event says the row was deleted. The reread finds it there -- it was
// re-inserted after the page was read, which during a catch-up over a live
// source is an ordinary Tuesday. Writing a tombstone on the event's word would
// leave the target refusing to answer for a row that exists.
func TestCatchUp_TheRereadDecidesAndNotTheEvent(t *testing.T) {
	c := qt.New(t)
	h := newHarness(c, defaultBounds())
	changes := &fakeChanges{
		pages:    [][]embedcatchup.Event{{changed(101, 1, "1", embedcatchup.OperationDelete)}},
		horizons: []uint64{102},
	}

	_, _, err := caughtUp(c, h, changes, livingRows("1"))

	c.Assert(err, qt.IsNil)
	c.Assert(h.target.commits, qt.HasLen, 1)
	c.Assert(h.target.commits[0].writes, qt.HasLen, 1)
	c.Assert(h.target.commits[0].writes[0].Kind, qt.Equals, embedrun.WriteUpsert)
}

// TestCatchUp_AnEmptyPageStillMovesTheWatermark keeps a finished catch-up from
// reading the same range forever.
//
// On a busy source that range only grows, and the run never reports caught up.
func TestCatchUp_AnEmptyPageStillMovesTheWatermark(t *testing.T) {
	c := qt.New(t)
	h := newHarness(c, defaultBounds())
	changes := &fakeChanges{pages: [][]embedcatchup.Event{{}}, horizons: []uint64{500}}

	run, _, err := caughtUp(c, h, changes, livingRows())

	c.Assert(err, qt.IsNil)
	c.Assert(run.CatchUpWatermark, qt.Equals, "500")
	c.Assert(h.target.commits, qt.HasLen, 1)
	c.Assert(h.target.commits[0].writes, qt.HasLen, 0)
}

// TestCatchUp_AWatermarkAlreadyAtTheHorizonWritesNothing is the control for the
// row above: the write happens because the watermark moved, not on every call.
func TestCatchUp_AWatermarkAlreadyAtTheHorizonWritesNothing(t *testing.T) {
	c := qt.New(t)
	h := newHarness(c, defaultBounds())
	stored, err := h.store.Run(context.Background(), "run-1")
	c.Assert(err, qt.IsNil)
	stored.SnapshotWatermark = "100"
	stored.CatchUpWatermark = "500"
	c.Assert(h.store.SaveRun(context.Background(), stored), qt.IsNil)
	changes := &fakeChanges{pages: [][]embedcatchup.Event{{}}, horizons: []uint64{500}}

	run, _, err := h.engine.CatchUp(context.Background(), "run-1", changes, livingRows())

	c.Assert(err, qt.IsNil)
	c.Assert(run.CatchUpWatermark, qt.Equals, "500")
	c.Assert(h.target.commits, qt.HasLen, 0)
}

// TestCatchUp_AnUnreadableOutboxFailsWithItsOwnClass keeps an operator from
// reading a stack trace to know which system to look at.
func TestCatchUp_AnUnreadableOutboxFailsWithItsOwnClass(t *testing.T) {
	c := qt.New(t)
	h := newHarness(c, defaultBounds())

	run, _, err := caughtUp(c, h, &fakeChanges{failOn: 1}, livingRows())

	c.Assert(err, qt.ErrorMatches, `changes: the outbox was unreadable`)
	c.Assert(run.FailureClass, qt.Equals, "changes")
	c.Assert(run.Status, qt.Equals, embedrun.StatusFailed)
}

// TestCatchUp_TheWatermarkIsATransactionAndNotAKey pins what the two cursors
// mean.
//
// The backfill resumes from a key and catch-up resumes from a transaction, and
// a catch-up that wrote a keyset cursor would send a resumed backfill to a row
// that has nothing to do with where it stopped.
func TestCatchUp_TheWatermarkIsATransactionAndNotAKey(t *testing.T) {
	c := qt.New(t)
	h := newHarness(c, defaultBounds())
	stored, err := h.store.Run(context.Background(), "run-1")
	c.Assert(err, qt.IsNil)
	stored.SnapshotWatermark = "100"
	stored.Cursor = []string{"4"}
	c.Assert(h.store.SaveRun(context.Background(), stored), qt.IsNil)
	changes := &fakeChanges{
		pages:    [][]embedcatchup.Event{{changed(101, 1, "1", embedcatchup.OperationUpdate)}},
		horizons: []uint64{102},
	}

	run, _, err := h.engine.CatchUp(context.Background(), "run-1", changes, livingRows("1"))

	c.Assert(err, qt.IsNil)
	c.Assert(run.Cursor, qt.DeepEquals, []string{"4"})
	watermark, parseErr := strconv.ParseUint(run.CatchUpWatermark, 10, 64)
	c.Assert(parseErr, qt.IsNil)
	c.Assert(watermark, qt.Equals, uint64(102))
}

// TestCatchUp_ARereadRowIsEmbeddedAtItsCurrentVersion keeps the vector bound to
// what the source says now.
//
// The event's version says when the row changed. The row may have changed again
// since -- which is the ordinary case during a catch-up over a live source -- and
// a write carrying the event's version would read as fresh against a source
// that has moved.
func TestCatchUp_ARereadRowIsEmbeddedAtItsCurrentVersion(t *testing.T) {
	c := qt.New(t)
	h := newHarness(c, defaultBounds())
	source := livingRows("1")
	source.versions[embedcatchup.KeyIdentity([]string{"1"})] = "12"
	changes := &fakeChanges{
		pages:    [][]embedcatchup.Event{{changed(101, 1, "1", embedcatchup.OperationUpdate)}},
		horizons: []uint64{102},
	}

	_, _, err := caughtUp(c, h, changes, source)

	c.Assert(err, qt.IsNil)
	c.Assert(h.target.commits[0].writes[0].Version, qt.Equals, "12")
}

// TestCatchUp_TheRereadIsAskedAboutEveryCollapsedKeyOnce is what makes the
// collapse save anything.
func TestCatchUp_TheRereadIsAskedAboutEveryCollapsedKeyOnce(t *testing.T) {
	c := qt.New(t)
	h := newHarness(c, defaultBounds())
	source := livingRows("1", "2")
	changes := &fakeChanges{
		pages: [][]embedcatchup.Event{{
			changed(101, 1, "1", embedcatchup.OperationUpdate),
			changed(102, 2, "2", embedcatchup.OperationUpdate),
			changed(103, 3, "1", embedcatchup.OperationUpdate),
		}},
		horizons: []uint64{104},
	}

	_, _, err := caughtUp(c, h, changes, source)

	c.Assert(err, qt.IsNil)
	c.Assert(source.asked, qt.DeepEquals, [][]string{{"1"}, {"2"}})
}

// TestCatchUp_AResumedRunStartsFromItsOwnWatermark is the difference between
// the two boundaries a run carries.
//
// The snapshot boundary is where catch-up BEGAN. A run that has been catching
// up for an hour and restarts must not go back to it: on a busy source that is
// an hour of changes re-read and re-embedded, every time the process bounces.
func TestCatchUp_AResumedRunStartsFromItsOwnWatermark(t *testing.T) {
	c := qt.New(t)
	h := newHarness(c, defaultBounds())
	stored, err := h.store.Run(context.Background(), "run-1")
	c.Assert(err, qt.IsNil)
	stored.SnapshotWatermark = "100"
	stored.CatchUpWatermark = "300"
	c.Assert(h.store.SaveRun(context.Background(), stored), qt.IsNil)
	changes := &fakeChanges{
		pages:    [][]embedcatchup.Event{{changed(301, 1, "1", embedcatchup.OperationUpdate)}},
		horizons: []uint64{400},
	}

	run, _, err := h.engine.CatchUp(context.Background(), "run-1", changes, livingRows("1"))

	c.Assert(err, qt.IsNil)
	c.Assert(changes.asked[0], qt.Equals, embedcatchup.Cursor{Transaction: 300})
	c.Assert(run.CatchUpWatermark, qt.Equals, "400")
}

// TestCatchUp_AProviderFailureIsRecordedOnTheRun covers stokaro/ptah#2649
// finding 9.
//
// The backfill's commitBatch records a provider failure; catch-up returned it
// and recorded nothing. So a catch-up that died against an unreachable provider
// left `status` at running with no failure class, no detail and no `failed`
// event -- and `status` is the verb an operator asks when a run stops moving.
//
// The event is asserted as well as the row, because the row is what `status`
// prints and the event is what an audit reads, and the two are written by
// different calls.
func TestCatchUp_AProviderFailureIsRecordedOnTheRun(t *testing.T) {
	c := qt.New(t)
	h := newHarness(c, defaultBounds())
	h.provider.failOn = 1

	changes := &fakeChanges{
		pages:    [][]embedcatchup.Event{{changed(101, 1, "1", embedcatchup.OperationUpdate)}},
		horizons: []uint64{102},
	}

	run, _, err := caughtUp(c, h, changes, livingRows("1"))

	c.Assert(err, qt.ErrorMatches, `provider: the provider returned 503`)
	c.Assert(run.FailureClass, qt.Equals, "provider")
	c.Assert(run.FailureDetail, qt.Equals, "the provider returned 503")
	c.Assert(run.Status, qt.Equals, embedrun.StatusFailed)
	c.Assert(failedEvents(c, h, "run-1"), qt.Equals, 1)
}

// TestCatchUp_AnUnreadableSourceFailsAsTheSource is the other class, and it is
// what keeps the class from being a constant.
//
// A reread that cannot read and a provider that will not answer send an
// operator to different systems. A fix that recorded one class for both would
// satisfy the test above and report a provider outage as a database problem.
func TestCatchUp_AnUnreadableSourceFailsAsTheSource(t *testing.T) {
	c := qt.New(t)
	h := newHarness(c, defaultBounds())

	source := livingRows("1")
	source.unreadable = true
	changes := &fakeChanges{
		pages:    [][]embedcatchup.Event{{changed(101, 1, "1", embedcatchup.OperationUpdate)}},
		horizons: []uint64{102},
	}

	run, _, err := caughtUp(c, h, changes, source)

	c.Assert(err, qt.ErrorMatches, `source: reread 1 changed rows: the source was unreadable`)
	c.Assert(run.FailureClass, qt.Equals, "source")
	c.Assert(run.Status, qt.Equals, embedrun.StatusFailed)
	c.Assert(failedEvents(c, h, "run-1"), qt.Equals, 1)
}

// failedEvents counts the run's failure events.
func failedEvents(c *qt.C, h *harness, runID string) int {
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
