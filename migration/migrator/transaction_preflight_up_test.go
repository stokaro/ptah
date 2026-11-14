package migrator_test

import (
	"path/filepath"
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/migration/migrator"
)

// These tests apply three migrations to a fresh SQLite database, whose second
// up file is the one each test writes. SQLite refuses VACUUM inside a
// transaction, which is the statement the second up file holds.

// preflightUpMigrator returns a fresh SQLite database and a migrator, loaded
// with opts, over three migrations whose second up file is up.
func preflightUpMigrator(
	c *qt.C,
	up string,
	opts ...migrator.FSProviderOption,
) (*dbschema.DatabaseConnection, *migrator.Migrator) {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(c.Context(), "sqlite://"+filepath.Join(c.TempDir(), "preflight.db"))
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(conn.Close(), qt.IsNil) })
	m, err := migrator.NewFSMigrator(conn, fstest.MapFS{
		"0000000001_first.up.sql":    {Data: []byte("CREATE TABLE first (id INTEGER PRIMARY KEY);\n")},
		"0000000001_first.down.sql":  {Data: []byte("DROP TABLE first;\n")},
		"0000000002_second.up.sql":   {Data: []byte(up)},
		"0000000002_second.down.sql": {Data: []byte("DROP TABLE second;\n")},
		"0000000003_third.up.sql":    {Data: []byte("CREATE TABLE third (id INTEGER PRIMARY KEY);\n")},
		"0000000003_third.down.sql":  {Data: []byte("DROP TABLE third;\n")},
	}, opts...)
	c.Assert(err, qt.IsNil)
	return conn, m
}

// existingTables is which of the three migrations' tables exist, by name.
func existingTables(c *qt.C, conn *dbschema.DatabaseConnection) map[string]bool {
	c.Helper()
	return map[string]bool{
		"first":  tableExists(c, conn, "first"),
		"second": tableExists(c, conn, "second"),
		"third":  tableExists(c, conn, "third"),
	}
}

// An up file the run's transaction would refuse is refused before any
// migration of the run is applied, in the per-file transaction and in the one
// transaction of tx-mode all. The first migration, which comes before it, does
// not run, and no revision is written: the run leaves the database as it found
// it rather than with the first migration applied and the second failed with
// nothing applied.
func TestMigrateUp_TransactionRefusalComesBeforeTheFirstFile_FailurePath(t *testing.T) {
	tests := []struct {
		name string
		mode migrator.MigrationTxMode
	}{
		{name: "tx-mode file", mode: migrator.MigrationTxModeFile},
		{name: "tx-mode all", mode: migrator.MigrationTxModeAll},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			conn, m := preflightUpMigrator(c, "CREATE TABLE second (id INTEGER PRIMARY KEY);\nVACUUM;\n")

			err := m.WithTransactionMode(test.mode).MigrateUp(c.Context())

			c.Assert(err, qt.ErrorMatches,
				`(?s)0000000002_second\.up\.sql cannot run inside a transaction: line 2 VACUUM is refused inside a `+
					`transaction block; mark the file .*\nSQL: VACUUM`)
			c.Assert(existingTables(c, conn), qt.DeepEquals, map[string]bool{"first": false, "second": false, "third": false})
			c.Assert(revisionStates(c, m), qt.DeepEquals, make(map[int64]string))
		})
	}
}

// The controls: a file that runs without a transaction around it is not
// asked, so the whole run applies. The file opts out by Ptah's directive or
// Atlas's, the run by tx-mode none, and under the golang-migrate runner a file
// of one statement is sent alone.
func TestMigrateUp_TransactionRefusalComesBeforeTheFirstFile_HappyPath(t *testing.T) {
	tests := []struct {
		name    string
		up      string
		mode    migrator.MigrationTxMode
		options []migrator.FSProviderOption
	}{
		{
			name: "the file is marked no_transaction",
			up:   "-- +ptah no_transaction\nCREATE TABLE second (id INTEGER PRIMARY KEY);\nVACUUM;\n",
			mode: migrator.MigrationTxModeFile,
		},
		{
			name: "the file is marked atlas:txmode none",
			up:   "-- atlas:txmode none\nCREATE TABLE second (id INTEGER PRIMARY KEY);\nVACUUM;\n",
			mode: migrator.MigrationTxModeFile,
		},
		{
			name: "the run is tx-mode none",
			up:   "CREATE TABLE second (id INTEGER PRIMARY KEY);\nVACUUM;\n",
			mode: migrator.MigrationTxModeNone,
		},
		{
			name:    "the golang-migrate runner sends a one-statement file alone",
			up:      "VACUUM;\n",
			mode:    migrator.MigrationTxModeFile,
			options: []migrator.FSProviderOption{migrator.WithOwnTransactions()},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			conn, m := preflightUpMigrator(c, test.up, test.options...)

			err := m.WithTransactionMode(test.mode).MigrateUp(c.Context())

			c.Assert(err, qt.IsNil)
			c.Assert(tableExists(c, conn, "first"), qt.IsTrue)
			c.Assert(tableExists(c, conn, "third"), qt.IsTrue)
			c.Assert(revisionStates(c, m), qt.DeepEquals, map[int64]string{1: "applied", 2: "applied", 3: "applied"})
		})
	}
}

// Only the files the run selects are asked: a run limited to the first
// migration applies it, and the second file is refused when a later run
// reaches it, with the first migration still recorded applied.
func TestMigrateUp_TransactionRefusalAsksTheSelectedFilesOnly(t *testing.T) {
	c := qt.New(t)
	conn, m := preflightUpMigrator(c, "CREATE TABLE second (id INTEGER PRIMARY KEY);\nVACUUM;\n")

	c.Assert(m.MigrateUpWithOptions(c.Context(), migrator.MigrateUpOptions{Amount: 1}), qt.IsNil)
	c.Assert(existingTables(c, conn), qt.DeepEquals, map[string]bool{"first": true, "second": false, "third": false})

	err := m.MigrateUp(c.Context())

	c.Assert(err, qt.ErrorMatches, `(?s)0000000002_second\.up\.sql cannot run inside a transaction: .*`)
	c.Assert(existingTables(c, conn), qt.DeepEquals, map[string]bool{"first": true, "second": false, "third": false})
	c.Assert(revisionStates(c, m), qt.DeepEquals, map[int64]string{1: "applied"})
}
