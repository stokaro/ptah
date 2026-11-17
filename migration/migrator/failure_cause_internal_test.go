package migrator

// White-box testing required: only the migrator's executor builds a statement
// error marked as sent, and a run whose statement got no answer while the
// rollback after it did cannot be produced on SQLite, the one engine the unit
// contour has: its rollback does not fail.

import (
	"errors"
	"fmt"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/jackc/pgx/v5/pgconn"
)

// The cause is the statement's own error. A rollback that failed after it is
// joined to the run's error, and its answer is not the statement's: a
// statement whose connection was lost gets no code, and no server details,
// from the rollback the run tried next.
func TestFailureCauseOfIsTheStatementsOwnError(t *testing.T) {
	c := qt.New(t)
	lost := errors.New("unexpected EOF")
	rollback := &pgconn.PgError{Code: "25P02", Message: "current transaction is aborted"}
	runErr := &FailedMigrationError{Version: 2, VersionKey: "2", Err: errors.Join(
		sentStatementError(fmt.Errorf("failed to execute migration SQL: %w", lost), "INSERT INTO t VALUES (1)", 1, 1),
		fmt.Errorf("failed to roll back migration 2: %w", rollback),
	)}

	cause, ok := FailureCauseOf(runErr)

	c.Assert(ok, qt.IsTrue)
	c.Assert(cause.Failure, qt.DeepEquals, RunFailure{Version: 2, VersionKey: "2", Statement: 1, Statements: 1})
	c.Assert(cause.Statement, qt.Equals, "INSERT INTO t VALUES (1)")
	c.Assert(cause.Err, qt.ErrorIs, lost)
	c.Assert(cause.Err, qt.Not(qt.ErrorIs), rollback)
}
