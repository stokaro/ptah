//go:build integration

package integration_test

import (
	"database/sql"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/clirun"
)

// sqliteRenameFile renames a table that a view reads. With legacy_alter_table
// off, the rename rewrites the view to read t2; with it on, the view keeps
// reading t.
const sqliteRenameFile = "CREATE TABLE t (a int);\nCREATE VIEW v AS SELECT a FROM t;\nALTER TABLE t RENAME TO t2;\n"

// TestSQLiteMigrationsUpGivesEachFileTheRenameSettingE2E runs two migration
// files: the first changes legacy_alter_table, and the second renames a table
// that a view reads. The setting belongs to the connection and outlives the
// transaction that changed it, so without a reset the second file renames
// under the rules the first one left. `ptah migrations up` gives the
// connection back the value it held before the first file, in every
// transaction mode: a rebuild plan that ends with the setting off does not
// turn it off for the next file of a connection that started with it on, and
// a file that turns it on does not turn it on for the next one.
func TestSQLiteMigrationsUpGivesEachFileTheRenameSettingE2E(t *testing.T) {
	const (
		turnsItOn  = "PRAGMA legacy_alter_table = on;\n"
		turnsItOff = "PRAGMA legacy_alter_table = on;\nPRAGMA legacy_alter_table = off;\n"
		rewritten  = `CREATE VIEW v AS SELECT a FROM "t2"`
		leftAlone  = "CREATE VIEW v AS SELECT a FROM t"
	)
	tests := []struct {
		name, mode, query, first, want string
	}{
		{name: "a file that turns it on, one transaction per file", mode: "file", first: turnsItOn, want: rewritten},
		{name: "a file that turns it on, one transaction for all", mode: "all", first: turnsItOn, want: rewritten},
		{name: "a file that turns it on, no transaction", mode: "none", first: turnsItOn, want: rewritten},
		{name: "a file that turns it off on a connection that has it on, one transaction per file", mode: "file",
			query: "?_pragma=legacy_alter_table(1)", first: turnsItOff, want: leftAlone},
		{name: "a file that turns it off on a connection that has it on, one transaction for all", mode: "all",
			query: "?_pragma=legacy_alter_table(1)", first: turnsItOff, want: leftAlone},
		{name: "a file that turns it off on a connection that has it on, no transaction", mode: "none",
			query: "?_pragma=legacy_alter_table(1)", first: turnsItOff, want: leftAlone},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			work := c.TempDir()
			migrations := filepath.Join(work, "migrations")
			c.Assert(os.MkdirAll(migrations, 0o750), qt.IsNil)
			files := map[string]string{
				"000001_setting.up.sql": test.first, "000001_setting.down.sql": "SELECT 1;\n",
				"000002_rename.up.sql": sqliteRenameFile, "000002_rename.down.sql": "SELECT 1;\n",
			}
			for name, body := range files {
				c.Assert(os.WriteFile(filepath.Join(migrations, name), []byte(body), 0o600), qt.IsNil)
			}

			applied := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: work}, "migrations", "up",
				"--db-url", "sqlite://app.db"+test.query, "--migrations-dir", "migrations", "--tx-mode", test.mode)

			c.Assert(applied.ExitCode, qt.Equals, 0, historyComment(applied))
			db, err := sql.Open("sqlite", filepath.Join(work, "app.db"))
			c.Assert(err, qt.IsNil)
			c.Cleanup(func() { c.Check(db.Close(), qt.IsNil) })
			var view string
			c.Assert(db.QueryRowContext(c.Context(), "SELECT sql FROM sqlite_schema WHERE name = 'v'").Scan(&view), qt.IsNil)
			c.Assert(view, qt.Equals, test.want)
		})
	}
}

// TestSQLiteMigrateApplyGivesEachFileTheRenameSettingE2E is the same run
// through `ptah-compat migrate apply` over an Atlas directory: the first file
// turns legacy_alter_table on, and the second file's rename still rewrites the
// view, in every transaction mode. The pinned community binary leaves the view
// reading t, a table the rename took away.
func TestSQLiteMigrateApplyGivesEachFileTheRenameSettingE2E(t *testing.T) {
	for _, mode := range []string{"file", "all", "none"} {
		t.Run(mode, func(t *testing.T) {
			c := qt.New(t)
			work := c.TempDir()
			migrations := filepath.Join(work, "migrations")
			c.Assert(os.MkdirAll(migrations, 0o750), qt.IsNil)
			c.Assert(os.WriteFile(filepath.Join(migrations, "1_setting.sql"), []byte("PRAGMA legacy_alter_table = on;\n"), 0o600), qt.IsNil)
			c.Assert(os.WriteFile(filepath.Join(migrations, "2_rename.sql"), []byte(sqliteRenameFile), 0o600), qt.IsNil)
			hashed := clirun.Run(c, clirun.Compat, clirun.Options{Dir: work}, "migrate", "hash", "--dir", "file://migrations")
			c.Assert(hashed.ExitCode, qt.Equals, 0, historyComment(hashed))

			applied := clirun.Run(c, clirun.Compat, clirun.Options{Dir: work}, "migrate", "apply",
				"--dir", "file://migrations", "-u", "sqlite://app.db", "--tx-mode", mode)

			c.Assert(applied.ExitCode, qt.Equals, 0, historyComment(applied))
			db, err := sql.Open("sqlite", filepath.Join(work, "app.db"))
			c.Assert(err, qt.IsNil)
			c.Cleanup(func() { c.Check(db.Close(), qt.IsNil) })
			var view string
			c.Assert(db.QueryRowContext(c.Context(), "SELECT sql FROM sqlite_schema WHERE name = 'v'").Scan(&view), qt.IsNil)
			c.Assert(view, qt.Equals, `CREATE VIEW v AS SELECT a FROM "t2"`)
		})
	}
}
