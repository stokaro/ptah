package embedengine

import (
	"context"
	"errors"
	"fmt"

	"go.5x5.cz/ptah/internal/embedcatchup"
	"go.5x5.cz/ptah/internal/embedgen"
	"go.5x5.cz/ptah/internal/embedrun"
	"go.5x5.cz/ptah/internal/embedstore"
)

// Changes is where source changes come from.
type Changes interface {
	// Since reads the settled events a cursor still owes, and returns the
	// boundary it read up to.
	//
	// The boundary is returned rather than chosen by the caller because only
	// the implementation knows which transactions have concluded, and a caller
	// that picked one would eventually pick one too high.
	Since(ctx context.Context, from embedcatchup.Cursor, limit int) ([]embedcatchup.Event, uint64, error)
}

// Rereader reads the current state of the rows a change refers to.
//
// Catch-up rereads rather than trusting the event, which is what makes an event
// that carries only a key sufficient -- and it is what collapses a row updated
// five times during a backfill into one provider request rather than five.
type Rereader interface {
	// Current returns the rows still present, by key, in any order. A key that
	// is absent from the answer is a row that is gone.
	Current(ctx context.Context, keys [][]string) ([]embedgen.Row, []string, error)
}

// CatchUp processes source changes recorded after the snapshot boundary.
//
// It runs after the backfill and before verification, and its completion is
// what a cutover over a live source rests on: the backfill covers the source as
// of the boundary, and this covers everything since (stokaro/ptah#2068).
func (e *Engine) CatchUp(
	ctx context.Context, runID string, changes Changes, source Rereader,
) (embedrun.Run, embedrun.Progress, error) {
	run, token, err := e.claim(ctx, runID)
	if err != nil {
		return embedrun.Run{}, embedrun.Progress{}, err
	}
	// This pass's work, for the reason [Engine.Backfill] gives.
	started := run.Progress
	final, err := e.catchUpLoop(ctx, runID, run, token, changes, source)
	return final, progressSince(started, final.Progress), err
}

// catchUpLoop is the read-reread-commit loop itself, separated for the reason
// [Engine.backfillLoop] is.
func (e *Engine) catchUpLoop(
	ctx context.Context, runID string, run embedrun.Run, token int64,
	changes Changes, source Rereader,
) (embedrun.Run, error) {
	cursor, err := parseWatermark(run.CatchUpWatermark, run.SnapshotWatermark)
	if err != nil {
		return run, err
	}

	for {
		if err := ctx.Err(); err != nil {
			return e.reload(context.WithoutCancel(ctx), runID, errors.Join(ErrAborted, err))
		}
		events, horizon, err := changes.Since(ctx, cursor, e.Bounds.MaxRows)
		if err != nil {
			return e.fail(ctx, run, token, "changes", err)
		}
		if len(events) == 0 {
			// Nothing settled between the cursor and the horizon. The
			// watermark still moves: a catch-up that processed everything and
			// did not record how far it got reads the same range again, and on
			// a busy source that range only grows.
			return e.recordBarrier(ctx, run, token, horizon)
		}
		cursor = resumeAfterPage(events, horizon, e.Bounds.MaxRows)
		run, err = e.applyChanges(ctx, run, token, source, events, cursor)
		if err != nil {
			return run, err
		}
	}
}

// applyChanges collapses one page of events and writes what they mean.
//
// The writes and the cursor they carry the run to are one commit, so a run that
// dies here resumes at a position whose work is on disk.
func (e *Engine) applyChanges(
	ctx context.Context, run embedrun.Run, token int64, source Rereader,
	events []embedcatchup.Event, next embedcatchup.Cursor,
) (embedrun.Run, error) {
	collapsed := embedcatchup.Collapse(events)
	writes, outcome, err := e.resolveChanges(ctx, source, collapsed)
	if err != nil {
		// Recorded on the run, the way the backfill's commitBatch records its
		// own. Returning the error alone left `status` at running with no
		// failure class, no detail and no `failed` event -- so a catch-up that
		// died against an unreachable provider reported a run that was still
		// working, forever, and `status` was the verb an operator would ask
		// (stokaro/ptah#2649 finding 9).
		return e.fail(ctx, run, token, classOf(err), err)
	}
	outcome.CatchUpWatermark = next.String()
	return e.commitProgress(ctx, run, token, writes, outcome)
}

// resolveChanges turns collapsed events into target writes.
//
// Every surviving key is reread and re-embedded, and every absent one is
// tombstoned. The event's own version is not used to decide what to write: it
// says when the row changed, and the row may have changed again since -- which
// is the ordinary case during a catch-up over a live source.
// The failure class travels WITH the error, because it is a property of the
// failure rather than of the call: the two ways this can fail send an operator
// to different places -- a reread that could not read the source, and a
// provider that would not answer -- and the caller cannot tell them apart from
// the message. Guessing one class for both is how a provider outage gets
// reported as a database problem.
func (e *Engine) resolveChanges(
	ctx context.Context, source Rereader, collapsed []embedcatchup.Event,
) ([]embedrun.TargetWrite, embedrun.BatchOutcome, error) {
	keys := make([][]string, 0, len(collapsed))
	for _, event := range collapsed {
		keys = append(keys, event.Key)
	}
	rows, versions, err := source.Current(ctx, keys)
	if err != nil {
		return nil, embedrun.BatchOutcome{}, classified{"source",
			fmt.Errorf("reread %d changed rows: %w", len(keys), err)}
	}

	// The class comes out of embedRereadRows rather than being decided here.
	// A reread fails for two different reasons and only one of them is the
	// provider: canonicalizing a row the specification refuses never reaches an
	// endpoint, and recording it as a provider outage sends an operator to look
	// at a service that is working. The backfill already separates the two, and
	// a catch-up that folded them together made the same run report a different
	// cause depending on which loop was running (stokaro/ptah#2699 review).
	writes, outcome, err := e.embedRereadRows(ctx, rows, versions)
	if err != nil {
		return nil, embedrun.BatchOutcome{}, err
	}
	tombstones := tombstonesFor(collapsed, rows, e.Spec.Identity().Digest)
	writes = append(writes, tombstones...)

	outcome.RowsScanned = int64(len(collapsed))
	outcome.RowsDeleted = int64(len(tombstones))
	outcome.TargetCommitted = true
	outcome.DeletesCommitted = true
	return writes, outcome, nil
}

// embedRereadRows embeds the rows that still exist.
func (e *Engine) embedRereadRows(
	ctx context.Context, rows []embedgen.Row, versions []string,
) ([]embedrun.TargetWrite, embedrun.BatchOutcome, error) {
	if len(rows) == 0 {
		return nil, embedrun.BatchOutcome{}, nil
	}
	prepared, err := e.prepare(Page{Rows: rows, Versions: versions})
	if err != nil {
		return nil, embedrun.BatchOutcome{}, classified{"canonicalization",
			fmt.Errorf("canonicalize a changed row: %w", err)}
	}
	batch := embedrun.Batch{Rows: prepared}
	writes, outcome, err := e.embed(ctx, batch)
	if err != nil {
		return nil, embedrun.BatchOutcome{}, classified{"provider", err}
	}
	// The cursor belongs to the backfill's keyset and means nothing here:
	// catch-up resumes from a transaction identity, not from a key.
	outcome.Cursor = nil
	return writes, outcome, nil
}

// tombstonesFor writes a tombstone for every changed key the source no longer
// has.
//
// Derived from what the reread FOUND rather than from the event's operation. A
// delete followed by an insert during the same page collapses to the insert and
// the row is there; a delete the reread confirms is gone is a tombstone
// whatever the last event said.
func tombstonesFor(
	collapsed []embedcatchup.Event, rows []embedgen.Row, generation string,
) []embedrun.TargetWrite {
	present := make(map[string]bool, len(rows))
	for _, row := range rows {
		present[keyIdentity(row.Key)] = true
	}
	var tombstones []embedrun.TargetWrite
	for _, event := range collapsed {
		if present[keyIdentity(event.Key)] {
			continue
		}
		tombstones = append(tombstones, embedrun.TargetWrite{
			Key: event.Key, Generation: generation, Kind: embedrun.WriteTombstone,
			Version: event.Version,
		})
	}
	return tombstones
}

// recordBarrier moves the catch-up watermark when there was nothing to process.
//
// Nothing settled between the cursor and the horizon, so every transaction
// below the horizon is accounted for and the cursor owes the horizon in full.
func (e *Engine) recordBarrier(
	ctx context.Context, run embedrun.Run, token int64, horizon uint64,
) (embedrun.Run, error) {
	reached := embedcatchup.AtTransaction(horizon).String()
	if run.CatchUpWatermark == reached {
		return run, nil
	}
	return e.commitProgress(ctx, run, token, nil, embedrun.BatchOutcome{
		CatchUpWatermark: reached,
		TargetCommitted:  true, DeletesCommitted: true,
	})
}

// parseWatermark reads where catch-up resumes from, and refuses a run that
// records nowhere to resume.
//
// The recognition itself is embedcatchup.ResumeFrom, which the outbox pruner
// asks as well: what a run still owes and what may be deleted behind it are the
// same boundary, and a second copy of the rule here is how the two would come
// to disagree. What stays local is the refusal -- the engine cannot proceed
// without a position, while the pruner simply leaves such a run out of the
// reader set.
func parseWatermark(catchUp, snapshot string) (embedcatchup.Cursor, error) {
	cursor, ok, err := embedcatchup.ResumeFrom(catchUp, snapshot)
	if err != nil {
		return embedcatchup.Cursor{}, err
	}
	if !ok {
		return embedcatchup.Cursor{}, fmt.Errorf(
			"%w: the run records no snapshot boundary, so nothing says which changes catch-up owes",
			embedstore.ErrNotFound)
	}
	return cursor, nil
}

// resumeAfterPage is where catch-up resumes once a page is written.
//
// A page SHORTER than the limit was not truncated, so it held every settled
// event the cursor owed and the horizon itself is now owed in full. That is the
// last page of every catch-up, which is why an operator reading a finished run
// sees a plain transaction identity rather than a pair.
//
// A FULL page may have been cut anywhere, including inside a transaction, so
// the cursor resumes immediately after the furthest event the page held. The
// page is a prefix of the (transaction, sequence) order, so its furthest
// element leaves every unread event ahead of the cursor; the maximum is
// computed rather than read off the end of the slice because Since hands its
// page back in sequence order, which is a different order and deliberately so.
//
// Advancing a full page to the greatest TRANSACTION instead is
// stokaro/ptah#2628: the events of that transaction the page did not reach are
// then below the cursor, unread, and unreachable by any later run.
func resumeAfterPage(events []embedcatchup.Event, horizon uint64, limit int) embedcatchup.Cursor {
	if len(events) < limit {
		return embedcatchup.AtTransaction(horizon)
	}
	var furthest embedcatchup.Cursor
	for _, event := range events {
		if next := embedcatchup.After(event); furthest.Before(next) {
			furthest = next
		}
	}
	return furthest
}

// keyIdentity renders a key so two of them can be compared.
func keyIdentity(key []string) string {
	return embedcatchup.KeyIdentity(key)
}

// classified is an error that knows which failure class it belongs to.
//
// The class reaches [Engine.fail] this way rather than as a second return
// value, because revive holds a function to three results and because the
// class is a fact about the failure rather than about the call that noticed
// it: an error raised two frames down keeps its class on the way up.
type classified struct {
	class string
	err   error
}

// Error is the underlying message. The class is added by [Engine.fail], so an
// error that never reaches it reads exactly as it did before.
func (c classified) Error() string { return c.err.Error() }

// Unwrap keeps errors.Is and errors.As working through the classification.
func (c classified) Unwrap() error { return c.err }

// classOf reports the class an error was raised under, and the empty string for
// one raised with none -- which [embedrun.Run.Fail] treats as unclassified
// rather than as a class named "".
func classOf(err error) string {
	if carried, found := errors.AsType[classified](err); found {
		return carried.class
	}
	return ""
}
