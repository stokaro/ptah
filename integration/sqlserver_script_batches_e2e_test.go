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

// A SQL Server trigger's or routine's body runs to the end of its batch, and
// CREATE TRIGGER, PROCEDURE, FUNCTION and VIEW must each open one. The
// scripts Ptah wrote for SQL Server ended no batch, so a trigger or a
// procedure whose body is a plain statement rather than a BEGIN ... END block
// took every statement after it into its body: `ptah migrations up` on the
// output of `ptah db read` sent the procedure, the tables, the view and the
// triggers after it as one statement, which SQL Server refused, and the
// migration file `ptah migrations generate` wrote failed the same way. Every
// statement of such a script now ends its batch with GO.

// sqlServerStatementBodyFixture is a procedure and a trigger whose bodies are
// plain statements, each followed by more objects, beside a BEGIN ... END
// trigger and a view. Each statement runs in a batch of its own.
var sqlServerStatementBodyFixture = []string{
	"CREATE PROCEDURE p_stmt AS SELECT 1 AS one",
	"CREATE TABLE t (id int PRIMARY KEY, a int)",
	"CREATE TRIGGER trg_stmt ON t AFTER INSERT AS SET NOCOUNT ON",
	"CREATE TABLE u (id int PRIMARY KEY)",
	"CREATE TRIGGER trg_block ON u AFTER INSERT AS BEGIN SET NOCOUNT ON; END",
	"CREATE VIEW v_t AS SELECT id, a FROM dbo.t",
}

// sqlServerStatementBodyObjects is what the catalog holds for the fixture.
var sqlServerStatementBodyObjects = []string{
	"P p_stmt", "TR trg_block", "TR trg_stmt", "U t", "U u", "V v_t",
}

// sqlServerBatchScratchDatabase is an empty database on the SQL Server the
// suite names, dropped when the test ends.
func sqlServerBatchScratchDatabase(c *qt.C) string {
	c.Helper()
	const drop = "ALTER DATABASE %[1]s SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE %[1]s"
	return createdDevDialectDatabase(c, dbtarget.URL(c, dbtarget.SQLServer), drop, replaceSQLServerDatabase)
}

// sqlServerUserObjects reads the type and name of every table, view,
// procedure and trigger from the catalog, leaving out the revision tables a
// migration run adds, ordered by type and name.
func sqlServerUserObjects(c *qt.C, rawURL string) []string {
	c.Helper()
	db, err := sql.Open("sqlserver", rawURL)
	c.Assert(err, qt.IsNil)
	defer func() { c.Check(db.Close(), qt.IsNil) }()
	rows, err := db.QueryContext(c.Context(), `
		SELECT RTRIM(o.type), o.name FROM sys.objects o
		WHERE o.is_ms_shipped = 0 AND o.type IN ('U', 'V', 'P', 'TR')
			AND o.name NOT IN ('schema_migrations', 'schema_migrations_log', 'atlas_schema_revisions')
		ORDER BY o.type, o.name COLLATE Latin1_General_BIN`)
	c.Assert(err, qt.IsNil)
	defer func() { c.Check(rows.Close(), qt.IsNil) }()
	var objects []string
	for rows.Next() {
		var kind, name string
		c.Assert(rows.Scan(&kind, &name), qt.IsNil)
		objects = append(objects, kind+" "+name)
	}
	c.Assert(rows.Err(), qt.IsNil)
	return objects
}

// TestSQLServerDBReadEndsEveryBatchE2E describes a database with `ptah db
// read`, replays the description on an empty database with `ptah migrations
// up`, and plans it against the database it came from. The replay creates
// every object the source holds, and the plan is empty.
func TestSQLServerDBReadEndsEveryBatchE2E(t *testing.T) {
	c := qt.New(t)
	source := sqlServerBatchScratchDatabase(c)
	target := sqlServerBatchScratchDatabase(c)
	db, err := sql.Open("sqlserver", source)
	c.Assert(err, qt.IsNil)
	defer func() { c.Check(db.Close(), qt.IsNil) }()
	for _, statement := range sqlServerStatementBodyFixture {
		_, err := db.ExecContext(c.Context(), statement)
		c.Assert(err, qt.IsNil, qt.Commentf("%s", statement))
	}
	read := clirun.Run(c, clirun.Ptah, clirun.Options{}, "db", "read", "--db-url", source)
	c.Assert(read.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", read.Stderr))
	c.Assert(read.Stdout, qt.Contains, "AS\nSELECT 1 AS one;\nGO\n")
	migrations := c.TempDir()
	c.Assert(os.WriteFile(filepath.Join(migrations, "1_replay.up.sql"), []byte(read.Stdout), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(migrations, "1_replay.down.sql"), []byte("SELECT 1;\n"), 0o600), qt.IsNil)
	schema := filepath.Join(c.TempDir(), "described.sql")
	c.Assert(os.WriteFile(schema, []byte(read.Stdout), 0o600), qt.IsNil)

	applied := clirun.Run(c, clirun.Ptah, clirun.Options{},
		"migrations", "up", "--db-url", target, "--migrations-dir", migrations)
	plan := clirun.Run(c, clirun.Ptah, clirun.Options{}, "migrations", "plan", "--db-url", source, "--schema-file", schema)

	c.Assert(applied.ExitCode, qt.Equals, 0,
		qt.Commentf("description:\n%s\nstdout:\n%s\nstderr:\n%s", read.Stdout, applied.Stdout, applied.Stderr))
	c.Assert(sqlServerUserObjects(c, source), qt.DeepEquals, sqlServerStatementBodyObjects)
	c.Assert(sqlServerUserObjects(c, target), qt.DeepEquals, sqlServerStatementBodyObjects)
	c.Assert(plan.ExitCode, qt.Equals, 0,
		qt.Commentf("description:\n%s\nstdout:\n%s\nstderr:\n%s", read.Stdout, plan.Stdout, plan.Stderr))
	c.Assert(plan.Stdout, qt.Contains, "Safety: no executable migration statements")
}

// sqlServerStatementBodySchemaFile declares the fixture in a schema file, a
// statement-bodied trigger and procedure first, so that more objects follow
// each in the plan.
const sqlServerStatementBodySchemaFile = `CREATE TABLE t (id int PRIMARY KEY, a int);
GO
CREATE TRIGGER trg_stmt ON t AFTER INSERT AS SET NOCOUNT ON;
GO
CREATE PROCEDURE p_stmt AS SELECT 1 AS one;
GO
CREATE TABLE u (id int PRIMARY KEY);
GO
CREATE TRIGGER trg_block ON u AFTER INSERT AS BEGIN SET NOCOUNT ON; END;
GO
CREATE VIEW v_t AS SELECT id, a FROM dbo.t;
GO
`

// TestSQLServerMigrationsGenerateEndsEveryBatchE2E writes a migration with
// `ptah migrations generate`, runs it with `ptah migrations up`, and plans the
// schema file again. The migration creates every object, and the plan is
// empty. Split as one script before it was written, the plan put the objects
// after the statement-bodied trigger into the trigger's body, and the last
// statement of such a body was written with a second semicolon.
func TestSQLServerMigrationsGenerateEndsEveryBatchE2E(t *testing.T) {
	c := qt.New(t)
	target := sqlServerBatchScratchDatabase(c)
	schema := filepath.Join(c.TempDir(), "schema.sql")
	c.Assert(os.WriteFile(schema, []byte(sqlServerStatementBodySchemaFile), 0o600), qt.IsNil)
	migrations := c.TempDir()

	generated := clirun.Run(c, clirun.Ptah, clirun.Options{},
		"migrations", "generate", "--db-url", target, "--schema-file", schema, "--migrations-dir", migrations, "--name", "init")
	c.Assert(generated.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", generated.Stdout, generated.Stderr))
	up, err := filepath.Glob(filepath.Join(migrations, "*_init.up.sql"))
	c.Assert(err, qt.IsNil)
	c.Assert(up, qt.HasLen, 1)
	upSQL, err := os.ReadFile(up[0])
	c.Assert(err, qt.IsNil)
	applied := clirun.Run(c, clirun.Ptah, clirun.Options{},
		"migrations", "up", "--db-url", target, "--migrations-dir", migrations)
	plan := clirun.Run(c, clirun.Ptah, clirun.Options{}, "migrations", "plan", "--db-url", target, "--schema-file", schema)

	c.Assert(string(upSQL), qt.Not(qt.Contains), ";;")
	c.Assert(applied.ExitCode, qt.Equals, 0,
		qt.Commentf("migration:\n%s\nstdout:\n%s\nstderr:\n%s", upSQL, applied.Stdout, applied.Stderr))
	c.Assert(sqlServerUserObjects(c, target), qt.DeepEquals, sqlServerStatementBodyObjects)
	c.Assert(plan.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", plan.Stdout, plan.Stderr))
	c.Assert(plan.Stdout, qt.Contains, "Safety: no executable migration statements")
}

// TestSQLServerCompatMigrateDiffEndsEveryBatchE2E writes a migration with
// `ptah-compat migrate diff` and runs it with `ptah-compat migrate apply`,
// which creates every object of the schema file.
func TestSQLServerCompatMigrateDiffEndsEveryBatchE2E(t *testing.T) {
	c := qt.New(t)
	dev := sqlServerBatchScratchDatabase(c)
	target := sqlServerBatchScratchDatabase(c)
	schema := filepath.Join(c.TempDir(), "schema.sql")
	c.Assert(os.WriteFile(schema, []byte(sqlServerStatementBodySchemaFile), 0o600), qt.IsNil)
	dir := c.TempDir()

	diffed := clirun.Run(c, clirun.Compat, clirun.Options{},
		"migrate", "diff", "init", "--dir", "file://"+filepath.ToSlash(dir), "--to", "file://"+filepath.ToSlash(schema), "--dev-url", dev)
	c.Assert(diffed.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", diffed.Stdout, diffed.Stderr))
	applied := clirun.Run(c, clirun.Compat, clirun.Options{},
		"migrate", "apply", "--url", target, "--dir", "file://"+filepath.ToSlash(dir))

	c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
	c.Assert(sqlServerUserObjects(c, target), qt.DeepEquals, sqlServerStatementBodyObjects)
}
