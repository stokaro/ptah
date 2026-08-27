package embedengine

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	"go.5x5.cz/ptah/internal/embedcatchup"
	"go.5x5.cz/ptah/internal/embedgen"
	"go.5x5.cz/ptah/internal/embedrun"
	"go.5x5.cz/ptah/internal/embedstore"
)

// Changes is where source changes come from.
type Changes interface {
	// Since reads settled events from a transaction identity, and returns the
	// boundary it read up to.
	//
	// The boundary is returned rather than chosen by the caller because only
	// the implementation knows which transactions have concluded, and a caller
	// that picked one would eventually pick one too high.
	Since(ctx context.Context, from uint64, limit int) ([]embedcatchup.Event, uint64, error)
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
func (e *Engine) CatchUp(ctx context.Context, runID string, changes Changes, source Rereader) (embedrun.Run, error) {
	run, err := e.Store.Run(ctx, runID)
	if err != nil {
		return embedrun.Run{}, fmt.Errorf("load run %s: %w", runID, err)
	}
	token := run.FencingToken
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
		run, cursor, err = e.applyChanges(ctx, run, token, source, events)
		if err != nil {
			return run, err
		}
	}
}

// applyChanges collapses one page of events and writes what they mean.
func (e *Engine) applyChanges(
	ctx context.Context, run embedrun.Run, token int64, source Rereader, events []embedcatchup.Event,
) (embedrun.Run, uint64, error) {
	collapsed := embedcatchup.Collapse(events)
	writes, outcome, err := e.resolveChanges(ctx, source, collapsed)
	if err != nil {
		return run, 0, err
	}
	outcome.CatchUpWatermark = strconv.FormatUint(lastTransaction(events)+1, 10)

	updated, err := e.commitProgress(ctx, run, token, writes, outcome)
	if err != nil {
		return updated, 0, err
	}
	return updated, lastTransaction(events) + 1, nil
}

// resolveChanges turns collapsed events into target writes.
//
// Every surviving key is reread and re-embedded, and every absent one is
// tombstoned. The event's own version is not used to decide what to write: it
// says when the row changed, and the row may have changed again since -- which
// is the ordinary case during a catch-up over a live source.
func (e *Engine) resolveChanges(
	ctx context.Context, source Rereader, collapsed []embedcatchup.Event,
) ([]embedrun.TargetWrite, embedrun.BatchOutcome, error) {
	keys := make([][]string, 0, len(collapsed))
	for _, event := range collapsed {
		keys = append(keys, event.Key)
	}
	rows, versions, err := source.Current(ctx, keys)
	if err != nil {
		return nil, embedrun.BatchOutcome{}, fmt.Errorf("reread %d changed rows: %w", len(keys), err)
	}

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
		return nil, embedrun.BatchOutcome{}, fmt.Errorf("canonicalize a changed row: %w", err)
	}
	batch := embedrun.Batch{Rows: prepared}
	writes, outcome, err := e.embed(ctx, batch)
	if err != nil {
		return nil, embedrun.BatchOutcome{}, err
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
func (e *Engine) recordBarrier(
	ctx context.Context, run embedrun.Run, token int64, horizon uint64,
) (embedrun.Run, error) {
	if run.CatchUpWatermark == strconv.FormatUint(horizon, 10) {
		return run, nil
	}
	return e.commitProgress(ctx, run, token, nil, embedrun.BatchOutcome{
		CatchUpWatermark: strconv.FormatUint(horizon, 10),
		TargetCommitted:  true, DeletesCommitted: true,
	})
}

// parseWatermark reads where catch-up resumes from.
//
// Before catch-up has run, that is the snapshot boundary: everything from the
// transaction the backfill started at is catch-up's to process. A missing
// boundary is refused rather than defaulted to zero, because zero means "every
// change ever recorded" and on a long-lived outbox that is a different
// migration.
func parseWatermark(catchUp, snapshot string) (uint64, error) {
	if catchUp != "" {
		return parseTransaction(catchUp, "catch-up watermark")
	}
	if snapshot == "" {
		return 0, fmt.Errorf(
			"%w: the run records no snapshot boundary, so nothing says which changes catch-up owes",
			embedstore.ErrNotFound)
	}
	return parseTransaction(snapshot, "snapshot watermark")
}

// parseTransaction reads a watermark as a transaction identity.
func parseTransaction(raw, what string) (uint64, error) {
	value, err := strconv.ParseUint(raw, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("the %s %q is not a transaction identity: %w", what, raw, err)
	}
	return value, nil
}

// lastTransaction is the highest transaction identity in a page.
func lastTransaction(events []embedcatchup.Event) uint64 {
	var highest uint64
	for _, event := range events {
		if event.Transaction > highest {
			highest = event.Transaction
		}
	}
	return highest
}

// keyIdentity renders a key so two of them can be compared.
func keyIdentity(key []string) string {
	return embedcatchup.KeyIdentity(key)
}
