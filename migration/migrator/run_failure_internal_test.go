package migrator

// White-box testing required: these chains are what a run document reads after a
// session died, and only a lost session builds them. A restore that fails and a
// rollback that answers where the commit could not need a server to break at
// exactly that step; TestMigrationsUpFailureRecordOfALostConnectionE2E breaks one
// of them on a live server, and neither can be reached offline.

import (
	"context"
	"errors"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/jackc/pgx/v5/pgconn"
)

// A run that cannot put a session's timeouts back after a statement failed reports
// the restore first. The statement is still what stopped the run.
func TestRestoreTimeoutsAfterFailureKeepsTheFailedStatement(t *testing.T) {
	c := qt.New(t)
	failed := newFailedMigrationError(&Migration{Version: 7}, sentStatementError(errors.New("connection lost"), "CREATE TABLE t (id INT)", 2, 3))
	restore := func(context.Context) error { return errors.New("the session is gone") }

	err := (&Migrator{}).restoreTimeoutsAfterFailure(c.Context(), 7, restore, failed)

	c.Assert(err, qt.ErrorMatches, `(?s)failed to restore timeouts after migration 7 failed: the session is gone \(original error: .*`)
	c.Assert(runFailure(err), qt.DeepEquals, &RunFailure{Version: 7, VersionKey: "7", Statement: 2, Statements: 3})
}

// A commit the server gave no answer to has no code, and the rollback tried after it
// does not lend it one.
func TestRunFailureReadsTheCommitsOwnAnswer(t *testing.T) {
	c := qt.New(t)
	commit := &failedCommitError{statements: 3, err: errors.New("connection lost")}
	failed := newFailedMigrationError(&Migration{Version: 7}, errors.Join(commit, &pgconn.PgError{Code: "57P01"}))

	c.Assert(runFailure(failed), qt.DeepEquals, &RunFailure{Version: 7, VersionKey: "7", Statements: 3, Commit: true})
}
