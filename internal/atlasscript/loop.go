package atlasscript

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/hashicorp/hcl/v2"
	"github.com/zclconf/go-cty/cty"
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
	cursor := cty.NilVal
	for {
		if outcome.Batches >= maxBatches {
			return outcome, fmt.Errorf(
				"loop %q ran %d batches without the walk ending; the body is not consuming the rows the iterator selects",
				script.Name, outcome.Batches)
		}

		page, err := nextBatch(ctx, db, script, cursor, outcome.Batches)
		if err != nil {
			return outcome, err
		}
		if len(page.rows) == 0 {
			break
		}
		outcome.Batches++
		outcome.Rows += len(page.rows)
		reportf(opts.Report, "-- batch %d | %d rows\n", outcome.Batches, len(page.rows))

		// The cursor is the last row of this page: the do body reads it as
		// iterator.keyset.cursor, and the next query reads it as cursor.
		cursor, err = rowValue(script.Iterator.Cursor, page.columns, page.rows[len(page.rows)-1])
		if err != nil {
			return outcome, fmt.Errorf("loop %q iterator cursor: %w", script.Name, err)
		}
		batch, err := pageValue(script.Iterator, page)
		if err != nil {
			return outcome, fmt.Errorf("loop %q iterator batch: %w", script.Name, err)
		}
		evaluation := bodyContext(cursor, batch, cty.NumberIntVal(int64(outcome.Batches-1)))

		steps, err := runBatch(ctx, db, script, opts, now, evaluation)
		if err != nil {
			return outcome, fmt.Errorf("loop %q batch %d: %w", script.Name, outcome.Batches, err)
		}
		outcome.Steps = append(outcome.Steps, steps...)
	}

	outcome.Elapsed = now().Sub(started)
	reportf(opts.Report, "-----\n-- %s\n-- %d batches, %d rows\n",
		outcome.Elapsed, outcome.Batches, outcome.Rows)
	return outcome, nil
}

// page is one batch the iterator read: the column names the result set
// reported, and its rows in order.
type page struct {
	columns []string
	rows    [][]any
}

// nextBatch runs init for the first batch and next for the rest.
//
// The next query's args are evaluated against the cursor of the page before,
// and bind in the order written. Binding the cursor row itself would position
// its columns by declaration order, so `args = [cursor.b, cursor.a]` on a
// cursor declared `a, b` would bind a and b the wrong way round.
func nextBatch(
	ctx context.Context, db Transactor, script Script, cursor cty.Value, batches int,
) (page, error) {
	querier, ok := db.(Querier)
	if !ok {
		return page{}, fmt.Errorf("loop %q: the target cannot run the iterator's queries", script.Name)
	}

	query, exprs, evaluation := script.Iterator.InitSQL, script.Iterator.InitArgs, constantContext()
	if batches > 0 {
		query, exprs = script.Iterator.NextSQL, script.Iterator.NextArgs
		evaluation = &hcl.EvalContext{
			Variables: map[string]cty.Value{"cursor": cursor},
			Functions: argFunctions,
		}
	}
	args, err := bindArgs(exprs, evaluation)
	if err != nil {
		return page{}, fmt.Errorf("loop %q iterator (%s:%d): %w",
			script.Name, script.Iterator.Range.Filename, script.Iterator.Range.Start.Line, err)
	}

	rows, err := querier.QueryContext(ctx, query, args...)
	if err != nil {
		return page{}, fmt.Errorf("loop %q iterator (%s:%d): %w",
			script.Name, script.Iterator.Range.Filename, script.Iterator.Range.Start.Line, err)
	}
	defer func() { _ = rows.Close() }()

	batch := make([][]any, 0)
	columns, err := rows.Columns()
	if err != nil {
		return page{}, fmt.Errorf("loop %q iterator: read columns: %w", script.Name, err)
	}
	for rows.Next() {
		values := make([]any, len(columns))
		holders := make([]any, len(columns))
		for index := range values {
			holders[index] = &values[index]
		}
		if err := rows.Scan(holders...); err != nil {
			return page{}, fmt.Errorf("loop %q iterator: scan: %w", script.Name, err)
		}
		batch = append(batch, values)
	}
	if err := rows.Err(); err != nil {
		return page{}, fmt.Errorf("loop %q iterator: %w", script.Name, err)
	}
	return page{columns: columns, rows: batch}, nil
}

// pageValue is the page a do body reads as iterator.keyset.batch: one object
// per row, carrying the batch columns, or the cursor's when none are declared.
func pageValue(iterator *Iterator, read page) (cty.Value, error) {
	columns := pageShape{cursor: iterator.Cursor, batch: iterator.Batch}.batchColumns()
	rows := make([]cty.Value, 0, len(read.rows))
	for _, row := range read.rows {
		value, err := rowValue(columns, read.columns, row)
		if err != nil {
			return cty.NilVal, err
		}
		rows = append(rows, value)
	}
	return cty.ListVal(rows), nil
}

// runBatch runs the loop's body once, in its own transaction, with evaluation
// carrying the page its args read.
func runBatch(
	ctx context.Context, db Transactor, script Script, opts RunOptions, now func() time.Time,
	evaluation *hcl.EvalContext,
) ([]ExecOutcome, error) {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("open transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	reportf(opts.Report, "-- tx open\n")
	steps, err := runExecSteps(ctx, tx, script, opts, now, evaluation)
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
