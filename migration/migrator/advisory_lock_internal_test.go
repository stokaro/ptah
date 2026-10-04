package migrator

// White-box testing required: how a lost migration lock and a failed release
// combine with the run's own outcome is decided by unexported functions, and
// a loss reaches them through MigrateUp only on a live YDB whose lock session
// stops answering for longer than its session timeout. The live tests under
// integration/dbschema/ydb drive that path; these pin each combination.

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/internal/dblock"
)

var lostMigrationLock = &dblock.LostError{Dialect: "ydb", Name: "ptah_migrate"}

const lostMigrationLockText = `migration lock for up: advisory lock "ptah_migrate" on ydb was lost while it was ` +
	`held: the server no longer confirms that this session owns it, so another session may hold it now; ` +
	`the run stopped there, and the revision table records what it committed`

func TestMigrationLockOutcome_HappyPath(t *testing.T) {
	c := qt.New(t)
	c.Assert(migrationLockOutcome("up", nil, nil, nil), qt.IsNil)
}

// A lost lock and a failed release are the run's failure, whatever the run
// itself reported.
func TestMigrationLockOutcome_FailurePath(t *testing.T) {
	tests := []struct {
		name       string
		runErr     error
		lostErr    error
		releaseErr error
		wantErr    string
	}{
		{name: "the run failed", runErr: errors.New("boom"), wantErr: "boom"},
		{name: "the lock was lost under a run that succeeded", lostErr: lostMigrationLock,
			wantErr: lostMigrationLockText},
		{name: "the lock was lost and the run stopped on it", runErr: lostMigrationLock, lostErr: lostMigrationLock,
			wantErr: lostMigrationLockText},
		{name: "the lock was lost and the run failed otherwise", runErr: errors.New("boom"),
			lostErr: lostMigrationLock, wantErr: lostMigrationLockText + ` \(the run reported: boom\)`},
		{name: "the release failed after a run that succeeded", releaseErr: errors.New("session expired"),
			wantErr: "failed to release migration lock for up: session expired"},
		{name: "the release failed after a run that failed", runErr: errors.New("boom"),
			releaseErr: errors.New("session expired"),
			wantErr:    "boom; additionally failed to release migration lock: session expired"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(migrationLockOutcome("up", test.runErr, test.lostErr, test.releaseErr), qt.ErrorMatches,
				test.wantErr)
		})
	}
}

// heldLockAnswer is a held lock whose Err answers err.
type heldLockAnswer struct{ err error }

func (h heldLockAnswer) Err() error { return h.err }

// A run asks the lock it holds, and only that lock answers.
func TestMigrationLockLost(t *testing.T) {
	tests := []struct {
		name    string
		ctx     context.Context
		wantErr error
	}{
		{name: "no lock held", ctx: context.Background()},
		{name: "a lock still held", ctx: withHeldMigrationLock(context.Background(), heldLockAnswer{})},
		{name: "a lock lost", ctx: withHeldMigrationLock(context.Background(), heldLockAnswer{err: lostMigrationLock}),
			wantErr: lostMigrationLock},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(migrationLockLost(test.ctx), qt.ErrorIs, test.wantErr)
		})
	}
}

// openLockedSQLite opens a SQLite database, which a test drives the statement
// loops over with a held lock the test says is lost.
func openLockedSQLite(c *qt.C) *dbschema.DatabaseConnection {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(context.Background(), "sqlite://"+filepath.Join(c.TempDir(), "locked.db"))
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { _ = conn.Close() })
	return conn
}

// sqliteTables counts the tables in a SQLite database.
func sqliteTables(c *qt.C, conn *dbschema.DatabaseConnection) int {
	c.Helper()
	var count int
	c.Assert(conn.QueryRowContext(context.Background(),
		"SELECT COUNT(*) FROM sqlite_master WHERE type = 'table'").Scan(&count), qt.IsNil)
	return count
}

var lostLockContext = withHeldMigrationLock(context.Background(), heldLockAnswer{err: lostMigrationLock})

// Each loop that runs a body asks for the lock before each statement, so a
// run that lost it runs nothing more, whether or not anything canceled its
// context.
func TestExecuteMigrationFileSQL_StopsWhenTheLockIsLost(t *testing.T) {
	c := qt.New(t)
	conn := openLockedSQLite(c)

	err := executeMigrationFileSQL(lostLockContext, conn, "1_a.up.sql", "CREATE TABLE a (id INTEGER);",
		statementExecutionHooks{}, migrationExecutionNoTransaction)

	c.Assert(err, qt.ErrorIs, error(lostMigrationLock))
	c.Assert(sqliteTables(c, conn), qt.Equals, 0)
}

func TestExecuteSQLStatements_StopsWhenTheLockIsLost(t *testing.T) {
	c := qt.New(t)
	conn := openLockedSQLite(c)

	err := executeSQLStatements(lostLockContext, conn, "CREATE TABLE a (id INTEGER);", migrationExecutionNoTransaction)

	c.Assert(err, qt.ErrorIs, error(lostMigrationLock))
	c.Assert(sqliteTables(c, conn), qt.Equals, 0)
}

func TestResumeStatementsOnSession_StopsWhenTheLockIsLost(t *testing.T) {
	c := qt.New(t)
	conn := openLockedSQLite(c)
	m := NewMigrator(conn, nil)
	migration := CreateMigrationFromSQL(1, "a", "CREATE TABLE a (id INTEGER);", "DROP TABLE a;")

	err := m.resumeStatementsOnSession(lostLockContext, migration, []string{"CREATE TABLE a (id INTEGER)"}, 1,
		MigrationDirectionUp)

	c.Assert(err, qt.ErrorIs, error(lostMigrationLock))
	c.Assert(sqliteTables(c, conn), qt.Equals, 0)
}
