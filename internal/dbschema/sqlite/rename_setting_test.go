package sqlite_test

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/internal/dbschema/sqlite"
)

// sessionSettingsResetter is the method a migration run calls on a writer or
// a transaction after each file.
type sessionSettingsResetter interface {
	ResetSessionSettings(ctx context.Context) error
}

// openRenameSettingConn opens a file database with the URL options given and
// returns one of its connections.
func openRenameSettingConn(c *qt.C, options string) *sql.Conn {
	c.Helper()
	db, err := sql.Open("sqlite", filepath.Join(c.TempDir(), "rename.sqlite")+"?_pragma=busy_timeout(2000)"+options)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(db.Close(), qt.IsNil) })
	conn, err := db.Conn(c.Context())
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(conn.Close(), qt.IsNil) })
	return conn
}

// renameSettingOf reads legacy_alter_table on conn.
func renameSettingOf(c *qt.C, conn *sql.Conn) int {
	c.Helper()
	var value int
	c.Assert(conn.QueryRowContext(c.Context(), "PRAGMA legacy_alter_table").Scan(&value), qt.IsNil)
	return value
}

// legacyAlterTableOn is the URL option that opens a connection with
// legacy_alter_table on.
const legacyAlterTableOn = "&_pragma=legacy_alter_table(1)"

// A transaction gives its connection back the legacy_alter_table value it held
// before the first statement that named the setting, whether it commits or
// rolls back. A rebuild plan writes the setting off after its rename, whatever
// the connection held, so a connection that held it on gets it on again.
func TestTransactionGivesTheConnectionBackItsRenameSetting(t *testing.T) {
	turnsItOn := []string{"PRAGMA legacy_alter_table = on"}
	rebuildPair := []string{"PRAGMA legacy_alter_table = on", "PRAGMA legacy_alter_table = off"}
	tests := []struct {
		name, options string
		statements    []string
		finish        func(catalog.SchemaTransaction) error
		want          int
	}{
		{name: "a plan that turns it on, committed", statements: turnsItOn, finish: catalog.SchemaTransaction.Commit, want: 0},
		{name: "a plan that turns it on, rolled back", statements: turnsItOn, finish: catalog.SchemaTransaction.Rollback, want: 0},
		{name: "a rebuild on a connection that has it on, committed", options: legacyAlterTableOn,
			statements: rebuildPair, finish: catalog.SchemaTransaction.Commit, want: 1},
		{name: "a rebuild on a connection that has it on, rolled back", options: legacyAlterTableOn,
			statements: rebuildPair, finish: catalog.SchemaTransaction.Rollback, want: 1},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			conn := openRenameSettingConn(c, test.options)
			tx, err := sqlite.NewSQLiteWriterForConnection(conn, "").BeginTransaction(c.Context())
			c.Assert(err, qt.IsNil)
			c.Assert(executeAll(t, tx, test.statements), qt.IsNil)

			c.Assert(test.finish(tx), qt.IsNil)

			c.Assert(renameSettingOf(c, conn), qt.Equals, test.want)
		})
	}
}

// Several migration files can share one transaction, and the run resets the
// setting after each file. After the reset, a rename in the same transaction
// rewrites the view that reads the renamed table, as it does with the setting
// off; without it, the view keeps the old name.
func TestTransactionResetSessionSettingsWithinTheTransaction(t *testing.T) {
	tests := []struct {
		name   string
		resets int
		want   string
	}{
		{name: "reset after the file that turns it on", resets: 1, want: `CREATE VIEW v AS SELECT a FROM "t2"`},
		{name: "no reset", want: "CREATE VIEW v AS SELECT a FROM t"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			conn := openRenameSettingConn(c, "")
			tx, err := sqlite.NewSQLiteWriterForConnection(conn, "").BeginTransaction(c.Context())
			c.Assert(err, qt.IsNil)
			c.Assert(executeAll(t, tx, []string{"PRAGMA legacy_alter_table = on"}), qt.IsNil)
			resetter, ok := tx.(sessionSettingsResetter)
			c.Assert(ok, qt.IsTrue)

			for range test.resets {
				c.Assert(resetter.ResetSessionSettings(c.Context()), qt.IsNil)
			}
			c.Assert(executeAll(t, tx, []string{
				"CREATE TABLE t (a int)", "CREATE VIEW v AS SELECT a FROM t", "ALTER TABLE t RENAME TO t2",
			}), qt.IsNil)
			c.Assert(tx.Commit(), qt.IsNil)

			var view string
			c.Assert(conn.QueryRowContext(c.Context(), "SELECT sql FROM sqlite_schema WHERE name = 'v'").Scan(&view), qt.IsNil)
			c.Assert(view, qt.Equals, test.want)
		})
	}
}

// A writer pinned to one connection, which a run outside a transaction uses,
// gives the connection back the value it held before a statement through the
// writer changed it.
func TestPinnedWriterResetSessionSettings(t *testing.T) {
	c := qt.New(t)
	conn := openRenameSettingConn(c, legacyAlterTableOn)
	writer := sqlite.NewSQLiteWriterForConnection(conn, "")
	c.Assert(writer.ExecuteSQL(c.Context(), "PRAGMA legacy_alter_table = off"), qt.IsNil)
	c.Assert(renameSettingOf(c, conn), qt.Equals, 0)

	c.Assert(writer.ResetSessionSettings(c.Context()), qt.IsNil)

	c.Assert(renameSettingOf(c, conn), qt.Equals, 1)
}
