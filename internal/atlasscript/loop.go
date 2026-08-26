package atlasscript

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

// DefaultMaxBatches bounds a walk that does not end.
//
// A keyset loop ends when a batch comes back empty, and that depends on the
// body actually removing the rows the walk selects. A body that does not --
// a next query whose predicate the body does not satisfy, a DELETE that
// matches nothing -- walks the same batch forever, holding a transaction open
// against a live database. The bound turns that from an outage into an error
// naming the script (stokaro/ptah#1017).
const DefaultMaxBatches = 10000

// LoopOutcome is what one loop did.
type LoopOutcome struct {
	// Batches is how many batches the walk produced.
	Batches int
	// Rows is how many cursor rows the walk visited across them.
	Rows int
	// Steps are the exec outcomes, in the order they ran.
	Steps   []ExecOutcome
	Elapsed time.Duration
}

// RunLoop walks an iterator and runs the body once per batch.
//
// One transaction per batch, which is the documented default and the right
// one: a purge over a million rows in a single transaction holds locks for its
// whole run and rolls back an hour of work on the last statement. Per batch,
// a failure undoes that batch and the ones before it stand -- which is why the
// report names the batch, so a rerun knows where it stopped.
func RunLoop(ctx context.Context, db Transactor, script Script, opts RunOptions) (LoopOutcome, error) {
	if script.Kind != KindLoop {
		return LoopOutcome{}, fmt.Errorf(
			"script %q is a %s script; only loop scripts run here", script.Name, script.Kind)
	}
	if script.Iterator == nil {
		return LoopOutcome{}, fmt.Errorf("loop %q has no iterator", script.Name)
	}
	now := opts.Now
	if now == nil {
		now = time.Now
	}

	started := now()
	reportf(opts.Report, "Executing script %q (%s:%d):\n",
		script.Name, script.Range.Filename, script.Range.Start.Line)

	maxBatches := opts.MaxBatches
	if maxBatches <= 0 {
		maxBatches = DefaultMaxBatches
	}

	outcome := LoopOutcome{Steps: make([]ExecOutcome, 0)}
	cursor := make([]any, 0)
	for {
		if outcome.Batches >= maxBatches {
			return outcome, fmt.Errorf(
				"loop %q ran %d batches without the walk ending; the body is not consuming the rows the iterator selects",
				script.Name, outcome.Batches)
		}

		batch, err := nextBatch(ctx, db, script, cursor, outcome.Batches)
		if err != nil {
			return outcome, err
		}
		if len(batch) == 0 {
			break
		}
		outcome.Batches++
		outcome.Rows += len(batch)
		reportf(opts.Report, "-- batch %d | %d rows\n", outcome.Batches, len(batch))

		steps, err := runBatch(ctx, db, script, opts, now)
		if err != nil {
			return outcome, fmt.Errorf("loop %q batch %d: %w", script.Name, outcome.Batches, err)
		}
		outcome.Steps = append(outcome.Steps, steps...)
		cursor = batch[len(batch)-1]
	}

	outcome.Elapsed = now().Sub(started)
	reportf(opts.Report, "-----\n-- %s\n-- %d batches, %d rows\n",
		outcome.Elapsed, outcome.Batches, outcome.Rows)
	return outcome, nil
}

// nextBatch runs init for the first batch and next for the rest.
func nextBatch(
	ctx context.Context, db Transactor, script Script, cursor []any, batches int,
) ([][]any, error) {
	querier, ok := db.(Querier)
	if !ok {
		return nil, fmt.Errorf("loop %q: the target cannot run the iterator's queries", script.Name)
	}

	query := script.Iterator.InitSQL
	args := make([]any, 0, len(cursor))
	if batches > 0 {
		query = script.Iterator.NextSQL
		// The cursor row of the LAST batch, positioned as the next query's
		// arguments. Positional rather than named because that is what a
		// placeholder takes, and it is why the cursor is read in source order.
		args = append(args, cursor...)
	}

	rows, err := querier.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("loop %q iterator (%s:%d): %w",
			script.Name, script.Iterator.Range.Filename, script.Iterator.Range.Start.Line, err)
	}
	defer func() { _ = rows.Close() }()

	batch := make([][]any, 0)
	columns, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("loop %q iterator: read columns: %w", script.Name, err)
	}
	for rows.Next() {
		values := make([]any, len(columns))
		holders := make([]any, len(columns))
		for index := range values {
			holders[index] = &values[index]
		}
		if err := rows.Scan(holders...); err != nil {
			return nil, fmt.Errorf("loop %q iterator: scan: %w", script.Name, err)
		}
		batch = append(batch, values)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("loop %q iterator: %w", script.Name, err)
	}
	return batch, nil
}

// runBatch runs the loop's body once, in its own transaction.
func runBatch(
	ctx context.Context, db Transactor, script Script, opts RunOptions, now func() time.Time,
) ([]ExecOutcome, error) {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("open transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	reportf(opts.Report, "-- tx open\n")
	steps, err := runExecSteps(ctx, tx, script, opts, now)
	if err != nil {
		reportf(opts.Report, "-- tx rollback\n")
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit: %w", err)
	}
	reportf(opts.Report, "-- tx commit\n")
	return steps, nil
}

// assertTransactor keeps the interface a *sql.DB satisfies from drifting.
var _ Transactor = (*sql.DB)(nil)
