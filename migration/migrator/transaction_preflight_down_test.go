package migrator_test

import (
	"path/filepath"
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/migration/migrator"
)

// These tests roll a SQLite database back to version 0 over three migrations,
// whose second down file is the one each test writes. SQLite refuses VACUUM
// inside a transaction, which is the statement the second down file holds.

// preflightDownMigrator applies three migrations on a fresh SQLite database
// and returns it with the migrator that applied them. down is the second
// migration's down file.
func preflightDownMigrator(c *qt.C, down string) (*dbschema.DatabaseConnection, *migrator.Migrator) {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(c.Context(), "sqlite://"+filepath.Join(c.TempDir(), "preflight.db"))
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(conn.Close(), qt.IsNil) })
	m, err := migrator.NewFSMigrator(conn, fstest.MapFS{
		"0000000001_first.up.sql":    {Data: []byte("CREATE TABLE first (id INTEGER PRIMARY KEY);\n")},
		"0000000001_first.down.sql":  {Data: []byte("DROP TABLE first;\n")},
		"0000000002_second.up.sql":   {Data: []byte("CREATE TABLE second (id INTEGER PRIMARY KEY);\n")},
		"0000000002_second.down.sql": {Data: []byte(down)},
		"0000000003_third.up.sql":    {Data: []byte("CREATE TABLE third (id INTEGER PRIMARY KEY);\n")},
		"0000000003_third.down.sql":  {Data: []byte("DROP TABLE third;\n")},
	})
	c.Assert(err, qt.IsNil)
	c.Assert(m.MigrateUp(c.Context()), qt.IsNil)
	return conn, m
}

// revisionStates is the state of each revision row, by version.
func revisionStates(c *qt.C, m *migrator.Migrator) map[int64]string {
	c.Helper()
	revisions, err := m.GetRevisions(c.Context())
	c.Assert(err, qt.IsNil)
	states := make(map[int64]string, len(revisions))
	for _, revision := range revisions {
		states[revision.Version] = revision.State
	}
	return states
}

// A down file the rollback's transaction would refuse is refused before any
// migration of the run is rolled back. The third migration's rollback, which
// comes first, does not run, and no revision is left dirty: every row still
// records its migration applied.
func TestMigrateDownTo_TransactionRefusalComesBeforeTheRollback_FailurePath(t *testing.T) {
	c := qt.New(t)
	conn, m := preflightDownMigrator(c, "DROP TABLE second;\nVACUUM;\n")

	err := m.MigrateDownTo(c.Context(), 0)

	c.Assert(err, qt.ErrorMatches,
		`(?s)0000000002_second\.down\.sql cannot run inside a transaction: line 2 VACUUM is refused inside a `+
			`transaction block; mark the file .*\nSQL: VACUUM`)
	c.Assert(tableExists(c, conn, "third"), qt.IsTrue)
	c.Assert(tableExists(c, conn, "second"), qt.IsTrue)
	c.Assert(revisionStates(c, m), qt.DeepEquals, map[int64]string{1: "applied", 2: "applied", 3: "applied"})
}

// The control: the same statement in a down file that asks for no transaction
// is not refused, and the whole run rolls back.
func TestMigrateDownTo_TransactionRefusalComesBeforeTheRollback_HappyPath(t *testing.T) {
	c := qt.New(t)
	conn, m := preflightDownMigrator(c, "-- +ptah no_transaction\nDROP TABLE second;\nVACUUM;\n")

	err := m.MigrateDownTo(c.Context(), 0)

	c.Assert(err, qt.IsNil)
	c.Assert(tableExists(c, conn, "third"), qt.IsFalse)
	c.Assert(tableExists(c, conn, "second"), qt.IsFalse)
	c.Assert(revisionStates(c, m), qt.DeepEquals, make(map[int64]string))
}
