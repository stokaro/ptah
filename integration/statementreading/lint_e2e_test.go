//go:build integration

package statementreading_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/internal/atlasurl"
	"ptah.run/internal/clirun"
)

// TestSQLLintE2E runs the shipped binary, whose exit code and stream are the claim. SQLite
// is a real server for it: the compiled-in engine takes the first rows and refuses the
// rest, as the live tests measure on every engine.
func TestSQLLintE2E(t *testing.T) {
	tests := []struct {
		name       string
		sql        string
		wantExit   int
		wantStdout string
	}{
		{name: "a dropped view", sql: "DROP VIEW v;\n", wantStdout: "info SQL004: no rule examined DROP VIEW in this file"},
		{name: "a dropped view under IF EXISTS", sql: "DROP VIEW IF EXISTS v;\n", wantStdout: "info SQL004: no rule examined DROP VIEW in this file"},
		{name: "a dropped trigger", sql: "DROP TRIGGER t;\n", wantStdout: "info SQL004: no rule examined DROP TRIGGER in this file"},
		{name: "a savepoint", sql: "SAVEPOINT s;\n", wantStdout: "info SQL004: no rule examined SAVEPOINT in this file"},
		{name: "a file of comments", sql: "-- nothing\n", wantStdout: "No SQL lint findings."},
		{name: "a misspelled DROP", sql: "DROP TABEL t;\n", wantExit: 1, wantStdout: "error SQL001: no engine Ptah reads has this statement: DROP TABEL"},
		{name: "no statement at all", sql: "FOO BAR;\n", wantExit: 1, wantStdout: "error SQL001: no engine Ptah reads has this statement: FOO"},
		{name: "punctuation", sql: "@@@ x;\n", wantExit: 1, wantStdout: "error SQL001: expected SQL keyword"},
		{name: "a list SQLite refuses", sql: "DROP VIEW v, w;\n", wantExit: 1, wantStdout: "error SQL001: expected the end of DROP VIEW"},
		{name: "a clause SQLite refuses", sql: "DROP VIEW v CASCADE;\n", wantExit: 1, wantStdout: "error SQL002: the SQL parser does not read this statement"},
		{name: "a statement SQLite refuses", sql: "TRUNCATE t;\n", wantExit: 1, wantStdout: "error SQL002: the SQL parser does not read this statement"},
		{name: "a statement nothing reads", sql: "SELECT 1;\n", wantExit: 1, wantStdout: "error SQL002: ptah sql lint does not lint SELECT statements yet"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			dir := c.TempDir()
			c.Assert(os.WriteFile(filepath.Join(dir, "x.sql"), []byte(test.sql), 0o600), qt.IsNil)

			run := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: dir}, "sql", "lint", "--dialect", "sqlite", "x.sql")

			c.Assert(run.ExitCode, qt.Equals, test.wantExit, qt.Commentf("stdout: %s\nstderr: %s", run.Stdout, run.Stderr))
			c.Assert(run.Stdout, qt.Contains, test.wantStdout)
		})
	}
}

// TestSchemaDiffE2E holds a comparison to the read: a file that drops a view declares no
// such view, whether or not the server holds one.
func TestSchemaDiffE2E(t *testing.T) {
	const declare = "CREATE TABLE t (id INT PRIMARY KEY);\nCREATE VIEW keep AS SELECT id FROM t;\n"
	tests := []struct {
		name       string
		live       string
		file       string
		wantStdout string
	}{
		{name: "a file that drops the view the server lacks", live: declare, file: declare + "CREATE VIEW gone AS SELECT id FROM t;\nDROP VIEW gone;\n", wantStdout: "Schemas are synced"},
		{name: "a file that opens by dropping it", live: declare, file: "DROP VIEW IF EXISTS keep;\n" + declare, wantStdout: "Schemas are synced"},
		{name: "a file that drops a view the server holds", live: declare + "CREATE VIEW gone AS SELECT id FROM t;\n", file: declare + "CREATE VIEW gone AS SELECT id FROM t;\nDROP VIEW gone;\n", wantStdout: `CREATE VIEW "gone"`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			dir := c.TempDir()
			address := atlasurl.SQLiteURLFromPath(filepath.Join(dir, "live.db"))
			conn, err := dbschema.ConnectToDatabase(c.Context(), address)
			c.Assert(err, qt.IsNil)
			_, err = conn.ExecContext(c.Context(), test.live)
			c.Assert(err, qt.IsNil)
			c.Assert(conn.Close(), qt.IsNil)
			file := filepath.Join(dir, "schema.sql")
			c.Assert(os.WriteFile(file, []byte(test.file), 0o600), qt.IsNil)

			run := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: dir}, "schema", "diff", "--from", file, "--to", address)

			c.Assert(run.ExitCode, qt.Equals, 0, qt.Commentf("stdout: %s\nstderr: %s", run.Stdout, run.Stderr))
			c.Assert(run.Stdout, qt.Contains, test.wantStdout)
		})
	}
}

// TestSchemaDiffE2E_RefusesADropOfAnotherKind is the reader's other answer through the
// binary: the drop of an object the model does not follow stops the run.
func TestSchemaDiffE2E_RefusesADropOfAnotherKind(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()
	file := filepath.Join(dir, "schema.sql")
	c.Assert(os.WriteFile(file, []byte("CREATE TABLE t (id INT PRIMARY KEY);\nDROP TRIGGER IF EXISTS g;\n"), 0o600), qt.IsNil)
	address := atlasurl.SQLiteURLFromPath(filepath.Join(dir, "live.db"))

	run := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: dir}, "schema", "diff", "--from", file, "--to", address)

	c.Assert(run.ExitCode, qt.Not(qt.Equals), 0)
	c.Assert(run.Stderr, qt.Contains, "DROP TRIGGER g: a schema read applies DROP TABLE, DROP INDEX, DROP VIEW and DROP MATERIALIZED VIEW")
}
