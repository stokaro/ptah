package dbschema

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"ptah.run/catalog"
	"ptah.run/internal/connector"
	"ptah.run/internal/sqlrunner"
)

// ErrValueChangedToFit is the error a statement run by
// [DatabaseConnection.WithStoredValuesKept] fails with when the server refuses
// it because it would change a value to fit its column: a value a table
// stores, under an ALTER TABLE that changes the column, or a value the
// statement writes. The error that wraps it also wraps the server's own
// error.
var ErrValueChangedToFit = errors.New("the server would change a value to fit its column")

// storedValuesRestoreTimeout bounds giving a session back the settings
// [DatabaseConnection.WithStoredValuesKept] changed, which runs after the
// caller's context may have ended.
const storedValuesRestoreTimeout = 5 * time.Second

// WithStoredValuesKept calls use with one physical database session on which
// the server refuses a statement that would change a value to fit its column,
// where a setting of the session would let the server make that change and
// warn. On MySQL and MariaDB that is a session whose sql_mode names no strict
// mode: there an ALTER TABLE that drops an ENUM or SET member some row holds,
// or narrows a column below a value some row holds, succeeds and rewrites the
// rows. Other engines refuse such a statement whatever the session says, and
// use runs on the session unchanged.
//
// Run Ptah's own plan through it, never SQL the user wrote: a migration file
// may depend on the server's settings, and runs as written.
//
// A connection that is not pinned pins a session for use and discards it
// afterward, where the engine decides by a setting of the session, and a
// pinned one runs use on its own session. Either way the session has the
// settings it started with again when WithStoredValuesKept returns, and an
// error setting them back is joined to the result.
//
// The server keeps the session's settings with some objects and applies them
// each time the object runs: MySQL keeps the sql_mode of a trigger, a stored
// routine, an event and a MariaDB package. A statement that defines one runs
// under the settings the session started with, so the object keeps them, and
// the session's settings for the rest of use are set again after it, whether
// the statement succeeded or not. This holds for a statement use sends
// through the session's writer, a transaction the writer begins, the
// session's executor, or [DatabaseConnection.ExecContext]. A statement the
// engine cannot read far enough to tell whether it defines one, such as one
// holding an executable comment whose effect on the server is unknown, is
// refused before any of it is sent.
//
// An error from use that the server gives for a statement it refuses this way
// wraps [ErrValueChangedToFit] and names the column the server names. A
// session that refuses such a statement already is left as it is, and the
// server's error comes back unchanged.
func (dc *DatabaseConnection) WithStoredValuesKept(
	ctx context.Context,
	use func(*DatabaseConnection) error,
) error {
	if dc == nil {
		return fmt.Errorf("stored values session requires an open database connection")
	}
	if use == nil {
		return fmt.Errorf("database session callback is nil")
	}
	if dc.native != nil || !dc.engine().StoredValuesBySession() {
		// A native engine runs no SQL session whose settings could let a
		// statement rewrite stored values, and another refuses one whatever
		// the session says.
		return use(dc)
	}
	return dc.WithSessionOrCurrent(ctx, func(session *DatabaseConnection) (resultErr error) {
		if session.session == nil {
			return fmt.Errorf("stored values session requires a pinned SQL session")
		}
		engine := session.engine()
		window, err := engine.KeepStoredValues(ctx, session.session)
		if err != nil {
			return fmt.Errorf("keep stored values on the session: %w", err)
		}
		if window == nil {
			return use(session)
		}
		defer func() {
			restoreCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), storedValuesRestoreTimeout)
			defer cancel()
			resultErr = errors.Join(resultErr, window.Restore(restoreCtx))
		}()
		err = use(session.inStoredValuesWindow(window))
		if column, refused := engine.StoredValueRefusal(err); refused {
			return storedValuesError(column, err)
		}
		return err
	})
}

// storedValuesError is err, refused by the server because it would change a
// value to fit column, with the column in front of the reason.
func storedValuesError(column string, err error) error {
	if column == "" {
		return fmt.Errorf("%w: %w", ErrValueChangedToFit, err)
	}
	return fmt.Errorf("column %q: %w: %w", column, ErrValueChangedToFit, err)
}

// inStoredValuesWindow is the session with every path a statement takes to
// the server running through window: the writer and the transactions it
// begins, the executor, and the runner [DatabaseConnection.ExecContext]
// reads. The reader is the session's own, since a read stores nothing.
func (dc *DatabaseConnection) inStoredValuesWindow(window connector.StoredValuesWindow) *DatabaseConnection {
	scoped := *dc
	if dc.writer != nil {
		scoped.writer = windowWriter{SchemaWriter: dc.writer, window: window}
	}
	switch executor := dc.executor.(type) {
	case nil:
	case catalog.SchemaTransaction:
		scoped.executor = windowTransaction{SchemaTransaction: executor, window: window}
	default:
		scoped.executor = windowExecutor{SchemaExecutor: executor, window: window}
	}
	if dc.runner != nil {
		scoped.runner = windowRunner{Runner: dc.runner, window: window}
	}
	return &scoped
}

// windowWriter is a schema writer whose statements run through window.
type windowWriter struct {
	catalog.SchemaWriter
	window connector.StoredValuesWindow
}

// ExecuteSQL runs statement through the window.
func (w windowWriter) ExecuteSQL(ctx context.Context, statement string, args ...any) error {
	return executeInWindow(ctx, w.SchemaWriter, w.window, statement, args)
}

// BeginTransaction begins a transaction whose statements run through the
// window.
func (w windowWriter) BeginTransaction(ctx context.Context) (catalog.SchemaTransaction, error) {
	tx, err := w.SchemaWriter.BeginTransaction(ctx)
	if err != nil {
		return nil, err
	}
	return windowTransaction{SchemaTransaction: tx, window: w.window}, nil
}

// windowTransaction is a schema transaction whose statements run through
// window. It reports no rollback evidence of its own, as
// [catalog.CancellationRollback] allows, since no caller in a window reads
// any.
type windowTransaction struct {
	catalog.SchemaTransaction
	window connector.StoredValuesWindow
}

// ExecuteSQL runs statement through the window.
func (t windowTransaction) ExecuteSQL(ctx context.Context, statement string, args ...any) error {
	return executeInWindow(ctx, t.SchemaTransaction, t.window, statement, args)
}

// windowExecutor is a schema executor whose statements run through window.
type windowExecutor struct {
	catalog.SchemaExecutor
	window connector.StoredValuesWindow
}

// ExecuteSQL runs statement through the window.
func (e windowExecutor) ExecuteSQL(ctx context.Context, statement string, args ...any) error {
	return executeInWindow(ctx, e.SchemaExecutor, e.window, statement, args)
}

// windowRunner is a SQL runner whose statements run through window.
type windowRunner struct {
	sqlrunner.Runner
	window connector.StoredValuesWindow
}

// ExecContext runs query through the window.
func (r windowRunner) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	var result sql.Result
	err := runInWindow(ctx, r.window, query,
		func(ctx context.Context, settings connector.SessionStatement) error {
			_, err := r.Runner.ExecContext(ctx, settings.SQL, settings.Args...)
			return err
		},
		func(ctx context.Context) error {
			var err error
			result, err = r.Runner.ExecContext(ctx, query, args...)
			return err
		},
	)
	return result, err
}

// Exec runs query through the window, with the context the runner binds.
func (r windowRunner) Exec(query string, args ...any) (sql.Result, error) {
	var result sql.Result
	err := runInWindow(context.Background(), r.window, query,
		func(_ context.Context, settings connector.SessionStatement) error {
			_, err := r.Runner.Exec(settings.SQL, settings.Args...)
			return err
		},
		func(context.Context) error {
			var err error
			result, err = r.Runner.Exec(query, args...)
			return err
		},
	)
	return result, err
}

// executeInWindow runs statement on executor through window. The settings
// go through executor too, so a dry run sends none of them.
func executeInWindow(
	ctx context.Context,
	executor catalog.SchemaExecutor,
	window connector.StoredValuesWindow,
	statement string,
	args []any,
) error {
	return runInWindow(ctx, window, statement,
		func(ctx context.Context, settings connector.SessionStatement) error {
			return executor.ExecuteSQL(ctx, settings.SQL, settings.Args...)
		},
		func(ctx context.Context) error {
			return executor.ExecuteSQL(ctx, statement, args...)
		},
	)
}

// runInWindow runs statement with run. A statement whose settings the server
// keeps with the object it defines runs between set giving the session its
// own settings and set giving it the window's again. The second runs even when
// the statement fails, on a context that outlives the caller's, since the rest
// of the session's statements need it; its error is joined to the result. A
// statement the window cannot read far enough to tell is not run.
func runInWindow(
	ctx context.Context,
	window connector.StoredValuesWindow,
	statement string,
	set func(context.Context, connector.SessionStatement) error,
	run func(context.Context) error,
) (resultErr error) {
	settings, err := window.StoredWith(statement)
	if err != nil {
		return fmt.Errorf("read whether the statement defines an object that keeps the session's settings: %w", err)
	}
	if settings == nil {
		return run(ctx)
	}
	if err := set(ctx, settings.Own); err != nil {
		return fmt.Errorf("give the session its own settings for a statement that keeps them: %w", err)
	}
	defer func() {
		keptCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), storedValuesRestoreTimeout)
		defer cancel()
		if err := set(keptCtx, settings.Window); err != nil {
			resultErr = errors.Join(resultErr, fmt.Errorf("set the session's settings again after a statement that keeps them: %w", err))
		}
	}()
	return run(ctx)
}
