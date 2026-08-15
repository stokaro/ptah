package migrator_test

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/migration/migrator"
)

// dropUsersUp is a migration whose only body statement drops users, guarded by a
// pre-migration check that users is empty.
const dropUsersUp = `-- +ptah check name="users_empty" assert="SELECT count(*) = 0 FROM users"` + "\nDROP TABLE users;\n"

func newSQLiteCheckMigrator(t *testing.T, seededRows int) (*dbschema.DatabaseConnection, *migrator.Migrator) {
	t.Helper()
	return newSQLiteCheckMigratorWithSQL(t, seededRows, dropUsersUp)
}

func newSQLiteCheckMigratorWithSQL(t *testing.T, seededRows int, upSQL string) (*dbschema.DatabaseConnection, *migrator.Migrator) {
	c := qt.New(t)
	t.Helper()
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "checks.db")
	conn, err := dbschema.ConnectToDatabase(ctx, "sqlite://"+path)
	c.Assert(err, qt.IsNil)
	t.Cleanup(func() { _ = conn.Close() })

	_, err = conn.Exec("CREATE TABLE users (id INTEGER PRIMARY KEY)")
	c.Assert(err, qt.IsNil)
	for i := range seededRows {
		_, err = conn.Exec("INSERT INTO users (id) VALUES (?)", i+1)
		c.Assert(err, qt.IsNil)
	}

	migration := migrator.CreateMigrationFromSQL(1, "drop_users", upSQL, "CREATE TABLE users (id INTEGER PRIMARY KEY);\n")
	m := migrator.NewMigrator(conn, migrator.NewRegisteredMigrationProvider(migration))
	c.Assert(m.Initialize(ctx), qt.IsNil)
	return conn, m
}

func usersTableExists(t *testing.T, conn *dbschema.DatabaseConnection) bool {
	c := qt.New(t)
	t.Helper()
	var count int
	err := conn.QueryRow("SELECT count(*) FROM sqlite_master WHERE type='table' AND name='users'").Scan(&count)
	c.Assert(err, qt.IsNil)
	return count == 1
}

func TestMigrateUp_FailingCheckAbortsWithNothingApplied(t *testing.T) {
	c := qt.New(t)
	conn, m := newSQLiteCheckMigrator(t, 1) // one row -> users not empty -> check fails

	err := m.MigrateUp(context.Background())

	c.Assert(err, qt.IsNotNil)
	var checkErr *migrator.CheckFailedError
	c.Assert(err, qt.ErrorAs, &checkErr, qt.Commentf("want CheckFailedError, got %v", err))
	c.Assert(checkErr.Version, qt.Equals, int64(1))
	c.Assert(checkErr.Name, qt.Equals, "users_empty")
	// The DROP TABLE never ran: nothing was applied.
	c.Assert(usersTableExists(t, conn), qt.IsTrue)
}

// TestMigrateUp_FailingCheckWritesNoRevisionRow pins that the no-bookkeeping
// contract is a property of the check engine, not of the txtar mapping: a
// failed `-- +ptah check` also leaves the revision table untouched, so the
// retry after fixing the data needs neither --allow-dirty nor a repair.
func TestMigrateUp_FailingCheckWritesNoRevisionRow(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
	conn, m := newSQLiteCheckMigrator(t, 1) // one row -> check fails

	c.Assert(m.MigrateUp(ctx), qt.IsNotNil)

	var rows int
	c.Assert(conn.QueryRow("SELECT count(*) FROM schema_migrations").Scan(&rows), qt.IsNil)
	c.Assert(rows, qt.Equals, 0, qt.Commentf("a failed check must not record a dirty migration row"))

	status, err := m.GetMigrationStatus(ctx)
	c.Assert(err, qt.IsNil)
	c.Assert(status.CurrentVersion, qt.Equals, int64(0))
	c.Assert(status.DirtyRevision, qt.IsNil)

	// Retry after fixing the data succeeds without any bypass flag.
	_, err = conn.Exec("DELETE FROM users")
	c.Assert(err, qt.IsNil)
	c.Assert(m.MigrateUp(ctx), qt.IsNil)
	c.Assert(usersTableExists(t, conn), qt.IsFalse)
}

// TestMigrateUp_MultiColumnCheckResultFailsClosed pins that an assertion
// returning more than one column fails the migration rather than being
// interpreted: Scan rejects the extra column, and a check that cannot be
// evaluated must block, never pass.
func TestMigrateUp_MultiColumnCheckResultFailsClosed(t *testing.T) {
	c := qt.New(t)
	up := `-- +ptah check name="two_columns" assert="SELECT 1, 1"` + "\nDROP TABLE users;\n"
	conn, m := newSQLiteCheckMigratorWithSQL(t, 0, up)

	err := m.MigrateUp(context.Background())

	c.Assert(err, qt.IsNotNil)
	var checkErr *migrator.CheckFailedError
	c.Assert(err, qt.ErrorAs, &checkErr, qt.Commentf("want CheckFailedError, got %v", err))
	c.Assert(checkErr.Err, qt.IsNotNil, qt.Commentf("multi-column result must surface the scan error"))
	c.Assert(checkErr.Error(), qt.Contains, "could not run")
	c.Assert(usersTableExists(t, conn), qt.IsTrue) // nothing applied
}

// multiRowCheckSQL builds a migration guarded by a check whose predicate
// returns two rows, so the result must be rejected regardless of row order.
func multiRowCheckSQL(assert string) string {
	return `-- +ptah check name="multi_row" assert="` + assert + `"` + "\nDROP TABLE users;\n"
}

// TestMigrateUp_MultiRowCheckFalsyFirstRowFailsClosed verifies that a check
// cannot hide an invalid result shape behind a falsy first row.
func TestMigrateUp_MultiRowCheckFalsyFirstRowFailsClosed(t *testing.T) {
	c := qt.New(t)
	conn, m := newSQLiteCheckMigratorWithSQL(t, 0, multiRowCheckSQL("SELECT 0 UNION ALL SELECT 1"))

	err := m.MigrateUp(context.Background())

	c.Assert(err, qt.ErrorMatches, `.*check assertion must return exactly one row, got more than 1.*`)
	c.Assert(usersTableExists(t, conn), qt.IsTrue) // nothing applied
}

// TestMigrateUp_MultiRowCheckTruthyFirstRowFailsClosed verifies that a truthy
// first row cannot conceal later rows.
func TestMigrateUp_MultiRowCheckTruthyFirstRowFailsClosed(t *testing.T) {
	c := qt.New(t)
	conn, m := newSQLiteCheckMigratorWithSQL(t, 0, multiRowCheckSQL("SELECT 1 UNION ALL SELECT 0"))

	err := m.MigrateUp(context.Background())

	c.Assert(err, qt.ErrorMatches, `.*check assertion must return exactly one row, got more than 1.*`)
	c.Assert(usersTableExists(t, conn), qt.IsTrue)
}

func TestMigrateUp_MutatingCheckFailsClosedWithoutDeletingData(t *testing.T) {
	c := qt.New(t)
	conn, m := newSQLiteCheckMigratorWithSQL(t, 1, multiRowCheckSQL("DELETE FROM users RETURNING 1"))

	err := m.MigrateUp(context.Background())

	c.Assert(err, qt.ErrorMatches, `.*check assertion must be a read-only SELECT statement.*`)
	var count int
	c.Assert(conn.QueryRow("SELECT count(*) FROM users").Scan(&count), qt.IsNil)
	c.Assert(count, qt.Equals, 1)
	c.Assert(usersTableExists(t, conn), qt.IsTrue)
}

func TestMigrateUp_ZeroRowCheckFailsClosed(t *testing.T) {
	c := qt.New(t)
	conn, m := newSQLiteCheckMigratorWithSQL(t, 0, multiRowCheckSQL("SELECT 1 WHERE 0"))

	err := m.MigrateUp(context.Background())

	c.Assert(err, qt.ErrorMatches, `.*check assertion must return exactly one row, got 0.*`)
	c.Assert(usersTableExists(t, conn), qt.IsTrue)
}

func TestMigrateUp_PassingCheckProceeds(t *testing.T) {
	c := qt.New(t)
	conn, m := newSQLiteCheckMigrator(t, 0) // empty users -> check passes

	err := m.MigrateUp(context.Background())

	c.Assert(err, qt.IsNil)
	c.Assert(usersTableExists(t, conn), qt.IsFalse) // users dropped
}

func TestMigrateUp_FailingCheckOnNoTransactionPath(t *testing.T) {
	c := qt.New(t)
	// The no_transaction directive forces the non-transactional apply path; the
	// check must still run before any body statement and abort with nothing applied.
	up := "-- +ptah no_transaction\n" + dropUsersUp
	conn, m := newSQLiteCheckMigratorWithSQL(t, 1, up)

	err := m.MigrateUp(context.Background())

	c.Assert(err, qt.IsNotNil)
	var checkErr *migrator.CheckFailedError
	c.Assert(err, qt.ErrorAs, &checkErr, qt.Commentf("want CheckFailedError, got %v", err))
	c.Assert(usersTableExists(t, conn), qt.IsTrue)
}

func TestMigrateUp_PassingCheckDoesNotDeadlockSingleConnectionPool(t *testing.T) {
	c := qt.New(t)
	// Regression: an in-memory SQLite pool is capped at one connection. A check
	// that runs before the migration transaction opens must not contend with the
	// tx for that single connection. A bounded context turns a regression into a
	// failure instead of a hang.
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	conn, err := dbschema.ConnectToDatabase(ctx, "sqlite://:memory:")
	c.Assert(err, qt.IsNil)
	t.Cleanup(func() { _ = conn.Close() })

	_, err = conn.Exec("CREATE TABLE users (id INTEGER PRIMARY KEY)") // empty -> check passes
	c.Assert(err, qt.IsNil)
	migration := migrator.CreateMigrationFromSQL(1, "drop_users", dropUsersUp, "CREATE TABLE users (id INTEGER PRIMARY KEY);\n")
	m := migrator.NewMigrator(conn, migrator.NewRegisteredMigrationProvider(migration))
	c.Assert(m.Initialize(ctx), qt.IsNil)

	c.Assert(m.MigrateUp(ctx), qt.IsNil)
	c.Assert(usersTableExists(t, conn), qt.IsFalse)
}

func TestMigrateUp_ChecksRejectedUnderTxModeAll(t *testing.T) {
	c := qt.New(t)
	// Empty users, so the check would pass; tx-mode all still rejects checks
	// because a pool read cannot see the batch's uncommitted state.
	conn, m := newSQLiteCheckMigrator(t, 0)

	err := m.WithTransactionMode(migrator.MigrationTxModeAll).MigrateUp(context.Background())

	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Contains, "cannot run with tx-mode all")
	c.Assert(usersTableExists(t, conn), qt.IsTrue) // nothing applied
}

func TestMigrateUp_TxModeAllWithSkipChecksProceeds(t *testing.T) {
	c := qt.New(t)
	conn, m := newSQLiteCheckMigrator(t, 1) // non-empty; check would fail, but bypass lifts the restriction

	err := m.WithTransactionMode(migrator.MigrationTxModeAll).WithSkipChecks(true).MigrateUp(context.Background())

	c.Assert(err, qt.IsNil)
	c.Assert(usersTableExists(t, conn), qt.IsFalse)
}

func TestMigrateUp_SkipChecksBypassesFailingCheck(t *testing.T) {
	c := qt.New(t)
	conn, m := newSQLiteCheckMigrator(t, 1) // non-empty, check would fail

	err := m.WithSkipChecks(true).MigrateUp(context.Background())

	c.Assert(err, qt.IsNil)
	c.Assert(usersTableExists(t, conn), qt.IsFalse) // bypass -> users dropped
}
