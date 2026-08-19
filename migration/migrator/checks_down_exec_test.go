package migrator_test

import (
	"context"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/migration/migrator"
)

// A rollback guarded by a precondition: the table may only be recreated while
// nothing depends on the rows the up migration left behind.
const guardedDownSQL = `-- +ptah check name="audit_empty" assert="SELECT count(*) = 0 FROM audit"` +
	"\nDROP TABLE users;\n"

// newSQLiteDownCheckMigrator builds a migrator whose DOWN body carries the
// check, and whose up body carries none.
//
// The asymmetry is the point: it is what tells "the down check ran" apart from
// "some check ran".
func newSQLiteDownCheckMigrator(t *testing.T, auditRows int) (*dbschema.DatabaseConnection, *migrator.Migrator) {
	c := qt.New(t)
	t.Helper()
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "downchecks.db")
	conn, err := dbschema.ConnectToDatabase(ctx, "sqlite://"+path)
	c.Assert(err, qt.IsNil)
	t.Cleanup(func() { _ = conn.Close() })

	_, err = conn.Exec("CREATE TABLE audit (id INTEGER PRIMARY KEY)")
	c.Assert(err, qt.IsNil)
	for i := range auditRows {
		_, err = conn.Exec("INSERT INTO audit (id) VALUES (?)", i+1)
		c.Assert(err, qt.IsNil)
	}

	migration := migrator.CreateMigrationFromSQL(1, "create_users",
		"CREATE TABLE users (id INTEGER PRIMARY KEY);\n", guardedDownSQL)
	m := migrator.NewMigrator(conn, migrator.NewRegisteredMigrationProvider(migration))
	c.Assert(m.Initialize(ctx), qt.IsNil)
	c.Assert(m.MigrateUp(ctx), qt.IsNil)
	return conn, m
}

// TestMigrateDown_FailingCheckAbortsWithNothingRolledBack is the assertion the
// issue was filed for.
//
// A `-- +ptah check` in a down body used to be parsed by nothing: no error, no
// warning, no assertion, and the rollback simply ran. A safety gate that is
// accepted and discarded is worse than one that was never offered
// (stokaro/ptah#1715).
func TestMigrateDown_FailingCheckAbortsWithNothingRolledBack(t *testing.T) {
	c := qt.New(t)
	conn, m := newSQLiteDownCheckMigrator(t, 1) // one audit row -> the check fails

	err := m.MigrateDown(context.Background())

	c.Assert(err, qt.IsNotNil)
	var checkErr *migrator.CheckFailedError
	c.Assert(err, qt.ErrorAs, &checkErr, qt.Commentf("want CheckFailedError, got %v", err))
	c.Assert(checkErr.Name, qt.Equals, "audit_empty")
	// The DROP TABLE never ran, so what the up migration created is still there.
	c.Assert(usersTableExists(t, conn), qt.IsTrue)
}

// TestMigrateDown_SatisfiedCheckLetsTheRollbackProceed is the control on the
// test above.
//
// Without it, a change that refused every rollback carrying a check -- or that
// failed them all for an unrelated reason -- would pass. The two cases differ
// only in the data the assertion reads.
func TestMigrateDown_SatisfiedCheckLetsTheRollbackProceed(t *testing.T) {
	c := qt.New(t)
	conn, m := newSQLiteDownCheckMigrator(t, 0) // no audit rows -> the check holds

	c.Assert(m.MigrateDown(context.Background()), qt.IsNil)

	c.Assert(usersTableExists(t, conn), qt.IsFalse)
}

// TestMigrateDown_UpCheckDoesNotGuardTheRollback pins the direction boundary
// from the other side.
//
// The fix reads the body the direction is about to run, not both. An up check
// that would fail against this data must not stop a rollback, or "checks are
// direction-aware" would have quietly become "checks run twice".
func TestMigrateDown_UpCheckDoesNotGuardTheRollback(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "updowncheck.db")
	conn, err := dbschema.ConnectToDatabase(ctx, "sqlite://"+path)
	c.Assert(err, qt.IsNil)
	t.Cleanup(func() { _ = conn.Close() })

	_, err = conn.Exec("CREATE TABLE audit (id INTEGER PRIMARY KEY)")
	c.Assert(err, qt.IsNil)

	// The up check holds while audit is empty, so the migration applies. A row
	// is inserted afterwards, which is what would fail the up check if the
	// rollback were still reading it.
	migration := migrator.CreateMigrationFromSQL(1, "create_users",
		`-- +ptah check name="audit_empty" assert="SELECT count(*) = 0 FROM audit"`+
			"\nCREATE TABLE users (id INTEGER PRIMARY KEY);\n",
		"DROP TABLE users;\n")
	m := migrator.NewMigrator(conn, migrator.NewRegisteredMigrationProvider(migration))
	c.Assert(m.Initialize(ctx), qt.IsNil)
	c.Assert(m.MigrateUp(ctx), qt.IsNil)

	_, err = conn.Exec("INSERT INTO audit (id) VALUES (1)")
	c.Assert(err, qt.IsNil)

	c.Assert(m.MigrateDown(ctx), qt.IsNil)
	c.Assert(usersTableExists(t, conn), qt.IsFalse)
}
