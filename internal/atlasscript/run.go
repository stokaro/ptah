package atlasscript

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"strings"
	"time"
)

// Querier is the read half of a database connection.
//
// An interface rather than the connection type, because a query script needs
// nothing else and a narrow dependency is what lets the executor be tested
// without a server. It is also the boundary that keeps this file read-only:
// there is no Exec here, so a query script cannot change data even by mistake.
type Querier interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

// Result is what one query step produced.
type Result struct {
	// Columns are the column names the database reported, in order.
	Columns []string
	// Rows are the values, masked, as strings. Strings because a report is
	// text: a mask rewrites a value into something that is no longer the
	// column's type, and carrying the original type after masking would invite
	// a consumer to read it back as one.
	Rows [][]string
	// Elapsed is how long the statement took.
	Elapsed time.Duration
}

// RunOptions configures a run.
type RunOptions struct {
	// Report receives the per-step report. Nil discards it, which is --quiet.
	Report io.Writer
	// Out receives the script's product -- the rows a query step returned.
	Out io.Writer
	// Now supplies the clock, so a test can assert on a report without its
	// timings changing between runs.
	Now func() time.Time
}

// RunQuery runs a query script and writes its rows.
//
// Only [KindQuery] is accepted. `exec` and `loop` change data, and running one
// through the read-only path would either fail confusingly at the driver or --
// worse -- succeed for a statement the driver happens to accept through a query
// call. Refusing by name is the honest answer while those verbs are unbuilt
// (stokaro/ptah#1017).
func RunQuery(ctx context.Context, db Querier, script Script, opts RunOptions) ([]Result, error) {
	if script.Kind != KindQuery {
		return nil, fmt.Errorf(
			"script %q is a %s script; only query scripts run here, because exec and loop change data",
			script.Name, script.Kind)
	}
	now := opts.Now
	if now == nil {
		now = time.Now
	}

	started := now()
	reportf(opts.Report, "Executing script %q (%s:%d):\n",
		script.Name, script.Range.Filename, script.Range.Start.Line)

	results := make([]Result, 0, len(script.Steps))
	statements := 0
	for _, step := range script.Steps {
		switch step.Kind {
		case StepOutput:
			reportf(opts.Report, "-- output (%s:%d): %s\n",
				step.Range.Filename, step.Range.Start.Line, step.Message)
			continue
		case StepQuery:
			result, err := runQueryStep(ctx, db, step, opts, now)
			if err != nil {
				return nil, err
			}
			results = append(results, result)
			statements++
		case StepCondition, StepExec:
			// A condition decides whether the rest runs and an exec changes
			// data. Skipping either would run a partial script and report it as
			// a whole one, which is the failure mode a report exists to prevent.
			return nil, fmt.Errorf(
				"script %q has a %s step at %s:%d, which the query path does not run",
				script.Name, step.Kind, step.Range.Filename, step.Range.Start.Line)
		}
	}

	reportf(opts.Report, "-----\n-- %s\n-- %d statements\n", now().Sub(started), statements)
	return results, nil
}

func runQueryStep(
	ctx context.Context, db Querier, step Step, opts RunOptions, now func() time.Time,
) (Result, error) {
	reportf(opts.Report, "-- query %q (%s:%d)\n   -> %s\n",
		step.Name, step.Range.Filename, step.Range.Start.Line, step.SQL)

	started := now()
	args := make([]any, 0, len(step.Args))
	for _, arg := range step.Args {
		args = append(args, arg)
	}
	rows, err := db.QueryContext(ctx, step.SQL, args...)
	if err != nil {
		return Result{}, fmt.Errorf("query %q (%s:%d): %w",
			step.Name, step.Range.Filename, step.Range.Start.Line, err)
	}
	defer func() { _ = rows.Close() }()

	columns, err := rows.Columns()
	if err != nil {
		return Result{}, fmt.Errorf("query %q: read columns: %w", step.Name, err)
	}

	result := Result{Columns: columns, Rows: make([][]string, 0)}
	for rows.Next() {
		values := make([]any, len(columns))
		holders := make([]any, len(columns))
		for index := range values {
			holders[index] = &values[index]
		}
		if err := rows.Scan(holders...); err != nil {
			return Result{}, fmt.Errorf("query %q: scan: %w", step.Name, err)
		}
		masked := make([]string, len(columns))
		for index, column := range columns {
			masked[index] = step.Masks.Apply(column, renderValue(values[index]))
		}
		result.Rows = append(result.Rows, masked)
	}
	// Checked after the loop rather than only at the end of the function: a
	// result set that failed mid-read reports its error here, and returning the
	// rows read so far would be a truncated report that looks complete.
	if err := rows.Err(); err != nil {
		return Result{}, fmt.Errorf("query %q: %w", step.Name, err)
	}
	result.Elapsed = now().Sub(started)

	reportf(opts.Report, "-- ok (%s) | %d rows\n", result.Elapsed, len(result.Rows))
	writeCSV(opts.Out, result)
	return result, nil
}

// renderValue turns a scanned value into the text a report carries.
//
// A NULL becomes the empty string rather than the word "NULL": the report is
// CSV, and a literal NULL is indistinguishable from a column whose value is the
// four characters. Bytes become their string form, which is what a driver hands
// back for a text column on several engines.
func renderValue(value any) string {
	switch typed := value.(type) {
	case nil:
		return ""
	case []byte:
		return string(typed)
	case string:
		return typed
	default:
		return fmt.Sprint(typed)
	}
}

// writeCSV writes a result as comma-separated rows.
func writeCSV(out io.Writer, result Result) {
	if out == nil {
		return
	}
	fmt.Fprintln(out, strings.Join(result.Columns, ","))
	for _, row := range result.Rows {
		fmt.Fprintln(out, strings.Join(row, ","))
	}
}

func reportf(out io.Writer, format string, args ...any) {
	if out == nil {
		return
	}
	fmt.Fprintf(out, format, args...)
}
