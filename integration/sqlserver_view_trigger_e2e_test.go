//go:build integration

package integration_test

import (
	"database/sql"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/clirun"
	"ptah.run/internal/dbtarget"
)

// An INSTEAD OF trigger is how a SQL Server view takes writes. The catalog
// reader joined a trigger to sys.tables alone, so a trigger on a view was
// never read: `ptah db read` left it out, and a database built from the
// description took no writes through the view.

// sqlServerViewTriggerFixture is a table, a view over it, and an INSTEAD OF
// INSERT trigger on the view, which writes ten times the value it is given,
// beside an AFTER trigger on the table. Each
// statement runs in a batch of its own, as CREATE VIEW and CREATE TRIGGER
// must.
var sqlServerViewTriggerFixture = []string{
	"CREATE TABLE t (id int PRIMARY KEY, a int)",
	"CREATE VIEW v_t AS SELECT id, a FROM dbo.t",
	"CREATE TRIGGER trg_view ON v_t INSTEAD OF INSERT AS BEGIN INSERT INTO dbo.t(id, a) SELECT id, a * 10 FROM inserted; END",
	"CREATE TRIGGER trg_table ON t AFTER UPDATE AS BEGIN SET NOCOUNT ON; END",
}

// sqlServerTriggerParents reads each trigger's name, its parent and the
// parent's kind, and its timing from the catalog, ordered by name.
func sqlServerTriggerParents(c *qt.C, rawURL string) []string {
	c.Helper()
	db, err := sql.Open("sqlserver", rawURL)
	c.Assert(err, qt.IsNil)
	defer func() { c.Check(db.Close(), qt.IsNil) }()
	rows, err := db.QueryContext(c.Context(), `
		SELECT tr.name, o.name, o.type_desc,
			CASE WHEN tr.is_instead_of_trigger = 1 THEN 'INSTEAD OF' ELSE 'AFTER' END
		FROM sys.triggers tr
		JOIN sys.objects o ON o.object_id = tr.parent_id
		ORDER BY tr.name COLLATE Latin1_General_BIN`)
	c.Assert(err, qt.IsNil)
	defer func() { c.Check(rows.Close(), qt.IsNil) }()
	var facts []string
	for rows.Next() {
		var trigger, parent, kind, timing string
		c.Assert(rows.Scan(&trigger, &parent, &kind, &timing), qt.IsNil)
		facts = append(facts, trigger+" ON "+parent+" "+kind+" "+timing)
	}
	c.Assert(rows.Err(), qt.IsNil)
	return facts
}

// TestSQLServerDBReadDescribesAViewTriggerE2E describes a database with a
// trigger on a view, plans the description against the database it came from,
// and replays it on an empty database with `ptah migrations up`. Both
// catalogs then hold the same triggers on the same parents, and a row written
// through the view in the replayed database reaches the table as the trigger
// writes it, where the view alone would write it as given.
func TestSQLServerDBReadDescribesAViewTriggerE2E(t *testing.T) {
	c := qt.New(t)
	adminURL := dbtarget.URL(c, dbtarget.SQLServer)
	const drop = "ALTER DATABASE %[1]s SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE %[1]s"
	source := createdDevDialectDatabase(c, adminURL, drop, replaceSQLServerDatabase)
	target := createdDevDialectDatabase(c, adminURL, drop, replaceSQLServerDatabase)
	sourceDB, err := sql.Open("sqlserver", source)
	c.Assert(err, qt.IsNil)
	defer func() { c.Check(sourceDB.Close(), qt.IsNil) }()
	for _, statement := range sqlServerViewTriggerFixture {
		_, err := sourceDB.ExecContext(c.Context(), statement)
		c.Assert(err, qt.IsNil, qt.Commentf("%s", statement))
	}

	read := clirun.Run(c, clirun.Ptah, clirun.Options{}, "db", "read", "--db-url", source)
	c.Assert(read.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", read.Stderr))
	schema := filepath.Join(c.TempDir(), "described.sql")
	c.Assert(os.WriteFile(schema, []byte(read.Stdout), 0o600), qt.IsNil)
	migrations := c.TempDir()
	c.Assert(os.WriteFile(filepath.Join(migrations, "1_replay.up.sql"), []byte(read.Stdout), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(migrations, "1_replay.down.sql"), []byte("SELECT 1;\n"), 0o600), qt.IsNil)
	plan := clirun.Run(c, clirun.Ptah, clirun.Options{}, "migrations", "plan", "--db-url", source, "--schema-file", schema)
	applied := clirun.Run(c, clirun.Ptah, clirun.Options{},
		"migrations", "up", "--db-url", target, "--migrations-dir", migrations)

	c.Assert(read.Stdout, qt.Contains, "CREATE TRIGGER [trg_view] ON [v_t] INSTEAD OF INSERT AS BEGIN")
	c.Assert(plan.ExitCode, qt.Equals, 0,
		qt.Commentf("description:\n%s\nstdout:\n%s\nstderr:\n%s", read.Stdout, plan.Stdout, plan.Stderr))
	c.Assert(plan.Stdout, qt.Contains, "Safety: no executable migration statements")
	c.Assert(applied.ExitCode, qt.Equals, 0,
		qt.Commentf("description:\n%s\nstdout:\n%s\nstderr:\n%s", read.Stdout, applied.Stdout, applied.Stderr))
	want := sqlServerTriggerParents(c, source)
	c.Assert(want, qt.DeepEquals, []string{"trg_table ON t USER_TABLE AFTER", "trg_view ON v_t VIEW INSTEAD OF"})
	c.Assert(sqlServerTriggerParents(c, target), qt.DeepEquals, want)
	targetDB, err := sql.Open("sqlserver", target)
	c.Assert(err, qt.IsNil)
	defer func() { c.Check(targetDB.Close(), qt.IsNil) }()
	_, err = targetDB.ExecContext(c.Context(), "INSERT INTO v_t (id, a) VALUES (1, 7)")
	c.Assert(err, qt.IsNil)
	var a int
	c.Assert(targetDB.QueryRowContext(c.Context(), "SELECT a FROM t WHERE id = 1").Scan(&a), qt.IsNil)
	c.Assert(a, qt.Equals, 70)
}
