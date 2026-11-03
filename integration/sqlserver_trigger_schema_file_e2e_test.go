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

// A SQL Server schema file declares a trigger in T-SQL's order, `CREATE
// TRIGGER name ON table {FOR | AFTER | INSTEAD OF} events AS body`, which is
// also how `ptah db read` describes one. Read in the order PostgreSQL and
// MySQL write, the file is refused at the table with `expected trigger
// timing`, so the description of a database with a trigger cannot be planned
// against that database, and a schema file with a trigger cannot be applied.

// sqlServerDescribedTriggerFixture builds triggers whose description is read
// back: an INSTEAD OF trigger, an AFTER trigger on two events, and one written
// with FOR, which the catalog records as AFTER. Each statement runs in a batch
// of its own, as CREATE TRIGGER must.
var sqlServerDescribedTriggerFixture = []string{
	"CREATE TABLE audit_log (id int PRIMARY KEY, msg nvarchar(10))",
	"CREATE TABLE t (id int PRIMARY KEY)",
	"CREATE TRIGGER trg_instead ON t INSTEAD OF DELETE AS BEGIN INSERT INTO dbo.audit_log(id, msg) VALUES(1, N'd'); END",
	"CREATE TRIGGER trg_multi ON t AFTER INSERT, UPDATE AS BEGIN INSERT INTO dbo.audit_log(id, msg) VALUES(2, N'm'); END",
	"CREATE TRIGGER trg_for ON t FOR DELETE, INSERT AS BEGIN SET NOCOUNT ON; END",
}

// sqlServerTriggerScratchDatabase is an empty database on the SQL Server the
// suite names, dropped when the test ends.
func sqlServerTriggerScratchDatabase(c *qt.C) string {
	c.Helper()
	const drop = "ALTER DATABASE %[1]s SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE %[1]s"
	return createdDevDialectDatabase(c, dbtarget.URL(c, dbtarget.SQLServer), drop, replaceSQLServerDatabase)
}

// TestSQLServerDescribedTriggersPlanNothingE2E describes a database with `ptah
// db read`, reads the description back as the desired schema, and plans it
// against the database it came from. The two are the same schema, so nothing
// is planned.
func TestSQLServerDescribedTriggersPlanNothingE2E(t *testing.T) {
	c := qt.New(t)
	source := sqlServerTriggerScratchDatabase(c)
	db, err := sql.Open("sqlserver", source)
	c.Assert(err, qt.IsNil)
	defer func() { c.Check(db.Close(), qt.IsNil) }()
	for _, statement := range sqlServerDescribedTriggerFixture {
		_, err := db.ExecContext(c.Context(), statement)
		c.Assert(err, qt.IsNil, qt.Commentf("%s", statement))
	}

	read := clirun.Run(c, clirun.Ptah, clirun.Options{}, "db", "read", "--db-url", source)
	c.Assert(read.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", read.Stderr))
	c.Assert(read.Stdout, qt.Contains, "CREATE TRIGGER [trg_for] ON [t] AFTER INSERT, DELETE AS BEGIN SET NOCOUNT ON; END;")
	c.Assert(read.Stdout, qt.Contains, "CREATE TRIGGER [trg_instead] ON [t] INSTEAD OF DELETE AS BEGIN")
	c.Assert(read.Stdout, qt.Contains, "CREATE TRIGGER [trg_multi] ON [t] AFTER INSERT, UPDATE AS BEGIN")
	schema := filepath.Join(c.TempDir(), "described.sql")
	c.Assert(os.WriteFile(schema, []byte(read.Stdout), 0o600), qt.IsNil)

	plan := clirun.Run(c, clirun.Ptah, clirun.Options{}, "migrations", "plan", "--db-url", source, "--schema-file", schema)

	c.Assert(plan.ExitCode, qt.Equals, 0,
		qt.Commentf("description:\n%s\nstdout:\n%s\nstderr:\n%s", read.Stdout, plan.Stdout, plan.Stderr))
	c.Assert(plan.Stdout, qt.Contains, "Safety: no executable migration statements")
	c.Assert(sqlServerTriggerFacts(c, source), qt.DeepEquals, []string{
		"trg_for AFTER INSERT, DELETE", "trg_instead INSTEAD OF DELETE", "trg_multi AFTER INSERT, UPDATE",
	})
}

// sqlServerTriggerSchemaFile is a schema file in the form sqlcmd runs: GO ends
// each batch, so a trigger's body may hold several statements. It writes a
// schema-qualified name, OR ALTER, EXECUTE AS CALLER and FOR.
const sqlServerTriggerSchemaFile = `CREATE TABLE t (id int PRIMARY KEY, a int);
GO
CREATE TRIGGER trg ON t AFTER INSERT AS SET NOCOUNT ON;
GO
CREATE OR ALTER TRIGGER dbo.trg_for ON dbo.t WITH EXECUTE AS CALLER FOR UPDATE, DELETE AS
SET NOCOUNT ON;
UPDATE t SET a = a WHERE 1 = 0;
GO
CREATE TRIGGER [trg_instead] ON [t] INSTEAD OF DELETE AS BEGIN SET NOCOUNT ON; END;
`

// TestSQLServerSchemaApplyCreatesTriggersE2E applies a schema file with
// triggers to an empty database and reads each trigger's timing and events
// back from sys.triggers, and the whole body of the one that runs to its GO
// from its definition. Planned again, the file and the database agree.
func TestSQLServerSchemaApplyCreatesTriggersE2E(t *testing.T) {
	c := qt.New(t)
	target := sqlServerTriggerScratchDatabase(c)
	schema := filepath.Join(c.TempDir(), "schema.sql")
	c.Assert(os.WriteFile(schema, []byte(sqlServerTriggerSchemaFile), 0o600), qt.IsNil)

	applied := clirun.Run(c, clirun.Ptah, clirun.Options{},
		"schema", "apply", "--db-url", target, "--schema-file", schema, "--auto-approve")

	c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
	c.Assert(sqlServerTriggerFacts(c, target), qt.DeepEquals, []string{
		"trg AFTER INSERT", "trg_for AFTER UPDATE, DELETE", "trg_instead INSTEAD OF DELETE",
	})
	db, err := sql.Open("sqlserver", target)
	c.Assert(err, qt.IsNil)
	defer func() { c.Check(db.Close(), qt.IsNil) }()
	var definition string
	c.Assert(db.QueryRowContext(c.Context(), "SELECT OBJECT_DEFINITION(OBJECT_ID('dbo.trg_for'))").Scan(&definition), qt.IsNil)
	c.Assert(definition, qt.Matches, `(?s).*AS SET NOCOUNT ON;\s+UPDATE t SET a = a WHERE 1 = 0;\s*`)
	plan := clirun.Run(c, clirun.Ptah, clirun.Options{}, "migrations", "plan", "--db-url", target, "--schema-file", schema)
	c.Assert(plan.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", plan.Stdout, plan.Stderr))
	c.Assert(plan.Stdout, qt.Contains, "Safety: no executable migration statements")
}
