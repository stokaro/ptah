package atlasscript

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

// Execer is the write half of a database connection: a transaction, or
// anything that behaves like one.
//
// An exec script runs inside a transaction, so this is what a step is handed
// rather than the pool. That is not a convenience -- a script whose third
// statement fails must leave the first two undone, and handing steps the pool
// would make each one its own committed unit.
type Execer interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

// Transactor opens the transaction an exec script runs in.
type Transactor interface {
	BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error)
}

// ErrConditionFalse stops a script because a condition did not hold.
//
// Its own error rather than a generic failure: a false condition is the script
// working -- a purge guarded by "only if there is something to purge" and
// finding nothing is a successful run with no work, not a fault. A caller that
// treats every non-nil error as a failure would page somebody for it.
var ErrConditionFalse = errors.New("condition is not satisfied")

// ExecOutcome is what one exec step did.
type ExecOutcome struct {
	Name string
	// Affected is the row count the driver reported, and -1 when it does not
	// report one. Distinguished rather than folded into 0, because "no rows
	// changed" and "the driver will not say" are different answers and only the
	// first can be asserted against expect_rows.
	Affected int64
	Elapsed  time.Duration
}

// RunExec runs an exec script inside one transaction.
//
// Everything or nothing: a step that fails, a condition that does not hold, or
// an expect_rows that is not met rolls the whole script back. A data operation
// that half-ran is the outcome this shape exists to prevent -- it leaves a
// database in a state no script describes, and the report would say which steps
// succeeded without saying that the rest did not (stokaro/ptah#1017).
func RunExec(ctx context.Context, db Transactor, script Script, opts RunOptions) ([]ExecOutcome, error) {
	if script.Kind != KindExec {
		return nil, fmt.Errorf(
			"script %q is a %s script; only exec scripts run here", script.Name, script.Kind)
	}
	now := opts.Now
	if now == nil {
		now = time.Now
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("script %q: open transaction: %w", script.Name, err)
	}
	// Rolled back on every path that does not commit, including a panic. The
	// error is deliberately ignored: a rollback after a successful commit is
	// sql.ErrTxDone and means nothing went wrong.
	defer func() { _ = tx.Rollback() }()

	started := now()
	reportf(opts.Report, "Executing script %q (%s:%d):\n-- tx open\n",
		script.Name, script.Range.Filename, script.Range.Start.Line)

	outcomes, err := runExecSteps(ctx, tx, script, opts, now)
	if err != nil {
		reportf(opts.Report, "-- tx rollback\n")
		return nil, err
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("script %q: commit: %w", script.Name, err)
	}
	reportf(opts.Report, "-- tx commit\n-----\n-- %s\n-- %d statements\n",
		now().Sub(started), len(outcomes))
	return outcomes, nil
}

func runExecSteps(
	ctx context.Context, tx Execer, script Script, opts RunOptions, now func() time.Time,
) ([]ExecOutcome, error) {
	outcomes := make([]ExecOutcome, 0, len(script.Steps))
	for _, step := range script.Steps {
		switch step.Kind {
		case StepOutput:
			reportf(opts.Report, "-- output (%s:%d): %s\n",
				step.Range.Filename, step.Range.Start.Line, step.Message)
		case StepCondition:
			if err := runCondition(ctx, tx, step, opts); err != nil {
				return nil, err
			}
		case StepExec:
			outcome, err := runExecStep(ctx, tx, step, opts, now)
			if err != nil {
				return nil, err
			}
			outcomes = append(outcomes, outcome)
		case StepQuery:
			// A query inside an exec script is a read the author put there to
			// report on. It runs, and its rows go to the product like a query
			// script's do.
			if _, err := runQueryStep(ctx, tx, step, opts, now); err != nil {
				return nil, err
			}
		}
	}
	return outcomes, nil
}

// runCondition stops the script when its first column of its first row is not
// true.
//
// An empty result set is false rather than an error: "SELECT id FROM users
// WHERE active = 0 LIMIT 1" returning nothing is exactly the guard working.
func runCondition(ctx context.Context, tx Execer, step Step, opts RunOptions) error {
	rows, err := tx.QueryContext(ctx, step.SQL)
	if err != nil {
		return fmt.Errorf("condition %q (%s:%d): %w",
			step.Name, step.Range.Filename, step.Range.Start.Line, err)
	}
	defer func() { _ = rows.Close() }()

	satisfied := false
	if rows.Next() {
		var value any
		if err := rows.Scan(&value); err != nil {
			return fmt.Errorf("condition %q: scan: %w", step.Name, err)
		}
		satisfied = truthy(value)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("condition %q: %w", step.Name, err)
	}

	if !satisfied {
		reportf(opts.Report, "-- condition %q not satisfied (%s:%d)\n",
			step.Name, step.Range.Filename, step.Range.Start.Line)
		return fmt.Errorf("condition %q (%s:%d): %w",
			step.Name, step.Range.Filename, step.Range.Start.Line, ErrConditionFalse)
	}
	reportf(opts.Report, "-- condition %q ok (%s:%d)\n",
		step.Name, step.Range.Filename, step.Range.Start.Line)
	return nil
}

// truthy reads a condition's value the way SQL engines report one.
//
// Engines disagree about what a boolean column comes back as -- a bool, an
// int64, or the bytes "t"/"f" -- so the reading is over the shapes rather than
// over one type. A value nobody recognizes is false: a guard that cannot be
// read must not be treated as satisfied.
func truthy(value any) bool {
	switch typed := value.(type) {
	case nil:
		return false
	case bool:
		return typed
	case int64:
		return typed != 0
	case float64:
		return typed != 0
	case []byte:
		return truthyString(string(typed))
	case string:
		return truthyString(typed)
	default:
		return false
	}
}

func truthyString(value string) bool {
	switch value {
	case "t", "true", "TRUE", "True", "1", "y", "yes":
		return true
	default:
		return false
	}
}

func runExecStep(
	ctx context.Context, tx Execer, step Step, opts RunOptions, now func() time.Time,
) (ExecOutcome, error) {
	reportf(opts.Report, "-- exec %q (%s:%d)\n   -> %s\n",
		step.Name, step.Range.Filename, step.Range.Start.Line, step.SQL)

	started := now()
	args := make([]any, 0, len(step.Args))
	for _, arg := range step.Args {
		args = append(args, arg)
	}
	result, err := tx.ExecContext(ctx, step.SQL, args...)
	if err != nil {
		return ExecOutcome{}, fmt.Errorf("exec %q (%s:%d): %w",
			step.Name, step.Range.Filename, step.Range.Start.Line, err)
	}

	outcome := ExecOutcome{Name: step.Name, Affected: -1, Elapsed: now().Sub(started)}
	if affected, err := result.RowsAffected(); err == nil {
		outcome.Affected = affected
	}
	reportf(opts.Report, "-- ok (%s) | %s\n", outcome.Elapsed, describeAffected(outcome.Affected))

	if step.ExpectRows != nil {
		if outcome.Affected < 0 {
			return ExecOutcome{}, fmt.Errorf(
				"exec %q (%s:%d): expect_rows is %d, and this driver does not report a row count, so the assertion cannot be made",
				step.Name, step.Range.Filename, step.Range.Start.Line, *step.ExpectRows)
		}
		if outcome.Affected != int64(*step.ExpectRows) {
			return ExecOutcome{}, fmt.Errorf(
				"exec %q (%s:%d): expected %d rows, changed %d",
				step.Name, step.Range.Filename, step.Range.Start.Line, *step.ExpectRows, outcome.Affected)
		}
	}
	return outcome, nil
}

func describeAffected(affected int64) string {
	if affected < 0 {
		return "row count not reported"
	}
	return fmt.Sprintf("%d rows affected", affected)
}
