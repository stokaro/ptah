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

// TestDirtyRevisionRefusalNamesAnAcceptedRepair_FailurePath drives each dirty
// row `ptah migrations up --allow-dirty` refuses to resume, and runs the repair
// command the refusal names on the same row. The command has to be accepted:
// a refusal that names a plain `repair --version` sends the reader to a
// command that refuses these rows too, because it cannot show the rest of the
// body ran. SQLite holds the row; the shapes are fabricated with an UPDATE,
// since each is decided by Ptah's reading of the row rather than by the
// engine.
//
// A plain repair on the same row refuses, which is why the refusal names the
// forced one. The first run fails on its second statement, so t1 and t2 exist and the row
// records 1 of 2 statements. The database then holds what the body asks for,
// which is the reconciliation a repair with --force records.
func TestDirtyRevisionRefusalNamesAnAcceptedRepair_FailurePath(t *testing.T) {
	const (
		twoStatements   = "-- +ptah no_transaction\nCREATE TABLE t1 (id int);\nCREATE TABLE t2 (id int);\n"
		threeStatements = "-- +ptah no_transaction\nCREATE TABLE t1 (id int);\nCREATE TABLE t2 (id int);\n" +
			"CREATE TABLE t3 (id int);\n"
		forced = "'ptah migrations repair --version 000001 --force'"
	)
	tests := []struct {
		name string
		// row turns the failed row into the shape under test.
		row string
		// body is the up file the refused run reads.
		body string
		// reconcile is what the operator runs by hand before the repair.
		reconcile string
		want      string
		// status is the command `ptah migrations status` names for the row, the
		// one the refusal names.
		status string
	}{
		{
			name: "unknown statement outcome",
			row: "UPDATE schema_migrations SET state = 'pending', applied = 0, " +
				"error = 'statement execution outcome is unknown after process interruption' WHERE version = 1",
			body:      twoStatements,
			reconcile: "SELECT 1",
			want: "migration 000001 cannot resume automatically: the outcome of statement 1 is unknown after an " +
				"interrupted run; inspect the database, then use " + forced,
			status: forced,
		},
		{
			name: "rollback left statements in place",
			row: "UPDATE schema_migrations SET error = 'the rollback of the migration''s transaction left some " +
				"statements in place and undid others' WHERE version = 1",
			body:      twoStatements,
			reconcile: "SELECT 1",
			want: "migration 000001 cannot resume automatically: the rollback of its transaction left some " +
				"statements in place and undid others; inspect the database, then use " + forced,
			status: forced,
		},
		{
			name:      "statement count changed",
			row:       "SELECT 1",
			body:      threeStatements,
			reconcile: "CREATE TABLE t3 (id int)",
			want: "migration 000001 cannot resume automatically: the previous attempt applied 1 of 2 statements " +
				"but the file now has 3; inspect the database, then use " + forced,
			// A resume from statement 2 would index into the changed file.
			status: forced,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			dir := c.TempDir()
			database := filepath.Join(dir, "app.db")
			up := filepath.Join(dir, "migrations", "000001_a.up.sql")
			c.Assert(os.MkdirAll(filepath.Dir(up), 0o750), qt.IsNil)
			c.Assert(os.WriteFile(up, []byte(twoStatements), 0o600), qt.IsNil)
			c.Assert(os.WriteFile(filepath.Join(dir, "migrations", "000001_a.down.sql"),
				[]byte("DROP TABLE t1;\nDROP TABLE t2;\n"), 0o600), qt.IsNil)
			sqliteExec(c, database, "CREATE TABLE t2 (id int)")
			ptah := func(args ...string) clirun.Result {
				return clirun.Run(c, clirun.Ptah, clirun.Options{Dir: dir, Env: environmentWithoutPtahVariables()},
					append(args, "--db-url", "sqlite://app.db", "--migrations-dir", "migrations")...)
			}
			first := ptah("migrations", "up")
			c.Assert(first.ExitCode, qt.Equals, 2, qt.Commentf("stderr:\n%s", first.Stderr))
			sqliteExec(c, database, tt.row)
			c.Assert(os.WriteFile(up, []byte(tt.body), 0o600), qt.IsNil)

			refused := ptah("migrations", "up", "--allow-dirty")
			plain := ptah("migrations", "repair", "--version", "000001")
			status := ptah("migrations", "status")
			sqliteExec(c, database, tt.reconcile)
			repaired := ptah("migrations", "repair", "--version", "000001", "--force")

			c.Assert(refused.ExitCode, qt.Equals, 2)
			c.Assert(refused.Stderr, qt.Contains, tt.want)
			c.Assert(plain.ExitCode, qt.Equals, 2, qt.Commentf("stdout:\n%s", plain.Stdout))
			c.Assert(status.Stdout, qt.Contains, tt.status)
			c.Assert(repaired.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", repaired.Stderr))
			c.Assert(sqliteRevisionState(c, database), qt.Equals, "applied")
		})
	}
}

// sqliteExec runs statement against the SQLite database at path.
func sqliteExec(c *qt.C, path, statement string) {
	c.Helper()
	db, err := sql.Open("sqlite", path)
	c.Assert(err, qt.IsNil)
	defer func() { c.Check(db.Close(), qt.IsNil) }()
	_, err = db.ExecContext(c.Context(), statement)
	c.Assert(err, qt.IsNil)
}

// sqliteRevisionState reads the state the Ptah revision table records for
// version 1.
func sqliteRevisionState(c *qt.C, path string) string {
	c.Helper()
	db, err := sql.Open("sqlite", path)
	c.Assert(err, qt.IsNil)
	defer func() { c.Check(db.Close(), qt.IsNil) }()
	var state string
	c.Assert(db.QueryRowContext(c.Context(), "SELECT state FROM schema_migrations WHERE version = 1").Scan(&state),
		qt.IsNil)
	return state
}

// TestDirtyRollbackOverAChangedDownFile_FailurePath stops a rollback after the
// first of two down statements and then gives the down file a statement in
// between. A resume from statement 2 would run the new statement 2 and drop t2
// as well, over a count that no longer describes the file, so `repair
// --resume-from 2` refuses and runs nothing. The refusal, the status hint and
// the hint `migrations down` adds name the same two commands, and the forced
// repair the operator runs after restoring t1 is accepted.
func TestDirtyRollbackOverAChangedDownFile_FailurePath(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()
	database := filepath.Join(dir, "app.db")
	migrations := filepath.Join(dir, "migrations")
	down := filepath.Join(migrations, "000001_a.down.sql")
	c.Assert(os.MkdirAll(migrations, 0o750), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(migrations, "000001_a.up.sql"),
		[]byte("CREATE TABLE t1 (id int);\nCREATE TABLE t2 (id int);\n"), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(down, []byte("-- +ptah no_transaction\nDROP TABLE t1;\nDROP TABLE t_missing;\n"), 0o600),
		qt.IsNil)
	ptah := func(args ...string) clirun.Result {
		return clirun.Run(c, clirun.Ptah, clirun.Options{Dir: dir, Env: environmentWithoutPtahVariables()},
			append(args, "--db-url", "sqlite://app.db", "--migrations-dir", "migrations")...)
	}
	up := ptah("migrations", "up")
	c.Assert(up.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", up.Stderr))
	stopped := ptah("migrations", "down", "--target", "0", "--confirm")
	c.Assert(stopped.ExitCode, qt.Equals, 2, qt.Commentf("stderr:\n%s", stopped.Stderr))
	c.Assert(os.WriteFile(down,
		[]byte("-- +ptah no_transaction\nDROP TABLE t1;\nCREATE TABLE t_kept (id int);\nDROP TABLE t2;\n"), 0o600),
		qt.IsNil)
	const (
		forced   = "'ptah migrations repair --version 000001 --force'"
		previous = "'ptah migrations set --version <previous>'"
	)

	resumed := ptah("migrations", "repair", "--version", "000001", "--resume-from", "2")
	status := ptah("migrations", "status")
	again := ptah("migrations", "down", "--target", "0", "--confirm")
	tables := sqliteTables(c, database)
	sqliteExec(c, database, "CREATE TABLE t1 (id int)")
	repaired := ptah("migrations", "repair", "--version", "000001", "--force")

	c.Assert(resumed.ExitCode, qt.Equals, 2)
	c.Assert(resumed.Stderr, qt.Contains,
		"migration 000001 cannot resume the rollback: the previous attempt applied 1 of 2 down statements but "+
			"the down file now has 3; inspect the database, then use "+forced+
			" if you restored what the rollback reverted, or "+previous+" if you finished the rollback by hand")
	c.Assert(status.Stdout, qt.Contains, "and its down file now has 3")
	c.Assert(status.Stdout, qt.Contains, "then run "+forced+" if you restored what the rollback reverted, or "+previous)
	c.Assert(again.Stderr, qt.Contains, "then run "+forced+" if you restored what the rollback reverted, or "+previous)
	c.Assert(tables, qt.DeepEquals, []string{"schema_migrations", "schema_migrations_log", "t2"})
	c.Assert(repaired.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", repaired.Stderr))
	c.Assert(sqliteRevisionState(c, database), qt.Equals, "applied")
}
