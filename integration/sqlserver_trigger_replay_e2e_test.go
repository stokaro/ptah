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

// SQL Server keeps a trigger's whole CREATE TRIGGER statement as its module
// definition, and whether it is INSTEAD OF and which events fire it in
// sys.triggers and sys.trigger_events. `ptah db read` describes the trigger
// from all three. Read from the definition alone, every trigger is described
// as AFTER INSERT with its own CREATE TRIGGER nested inside the generated one,
// and SQL Server refuses the replay with `Incorrect syntax near the keyword
// 'TRIGGER'` (stokaro/ptah#2206).

// sqlServerTriggerFixture is an INSTEAD OF trigger and an AFTER trigger on two
// events. Each statement runs in a batch of its own, as CREATE TRIGGER must.
var sqlServerTriggerFixture = []string{
	"CREATE TABLE audit_log (id int IDENTITY PRIMARY KEY, msg nvarchar(10))",
	"CREATE TABLE t (id int PRIMARY KEY)",
	"CREATE TRIGGER trg_instead ON t INSTEAD OF DELETE AS BEGIN INSERT INTO dbo.audit_log(msg) VALUES(N'd'); END",
	"CREATE TRIGGER trg_multi ON t AFTER INSERT, UPDATE AS BEGIN INSERT INTO dbo.audit_log(msg) VALUES(N'm'); END",
}

// sqlServerTriggerFacts reads each trigger's name, timing and events from the
// catalog, ordered by name.
func sqlServerTriggerFacts(c *qt.C, rawURL string) []string {
	c.Helper()
	db, err := sql.Open("sqlserver", rawURL)
	c.Assert(err, qt.IsNil)
	defer func() { c.Check(db.Close(), qt.IsNil) }()
	rows, err := db.QueryContext(c.Context(), `
		SELECT tr.name,
			CASE WHEN tr.is_instead_of_trigger = 1 THEN 'INSTEAD OF' ELSE 'AFTER' END,
			ISNULL((SELECT STRING_AGG(te.type_desc, ', ') WITHIN GROUP (ORDER BY te.type)
					FROM sys.trigger_events te WHERE te.object_id = tr.object_id), '')
		FROM sys.triggers tr
		ORDER BY tr.name`)
	c.Assert(err, qt.IsNil)
	defer func() { c.Check(rows.Close(), qt.IsNil) }()
	var facts []string
	for rows.Next() {
		var name, timing, events string
		c.Assert(rows.Scan(&name, &timing, &events), qt.IsNil)
		facts = append(facts, name+" "+timing+" "+events)
	}
	c.Assert(rows.Err(), qt.IsNil)
	return facts
}

// TestSQLServerDBReadReplaysTriggersE2E describes a database with `ptah db
// read`, runs the description on an empty database with `ptah migrations up`,
// and reads each trigger's timing and events back from both catalogs.
func TestSQLServerDBReadReplaysTriggersE2E(t *testing.T) {
	c := qt.New(t)
	adminURL := dbtarget.URL(c, dbtarget.SQLServer)
	const drop = "ALTER DATABASE %[1]s SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE %[1]s"
	source := createdDevDialectDatabase(c, adminURL, drop, replaceSQLServerDatabase)
	target := createdDevDialectDatabase(c, adminURL, drop, replaceSQLServerDatabase)
	sourceDB, err := sql.Open("sqlserver", source)
	c.Assert(err, qt.IsNil)
	defer func() { c.Check(sourceDB.Close(), qt.IsNil) }()
	for _, statement := range sqlServerTriggerFixture {
		_, err := sourceDB.ExecContext(c.Context(), statement)
		c.Assert(err, qt.IsNil, qt.Commentf("%s", statement))
	}

	read := clirun.Run(c, clirun.Ptah, clirun.Options{}, "db", "read", "--db-url", source)
	c.Assert(read.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", read.Stderr))
	migrations := c.TempDir()
	c.Assert(os.WriteFile(filepath.Join(migrations, "1_replay.up.sql"), []byte(read.Stdout), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(migrations, "1_replay.down.sql"), []byte("SELECT 1;\n"), 0o600), qt.IsNil)
	applied := clirun.Run(c, clirun.Ptah, clirun.Options{},
		"migrations", "up", "--db-url", target, "--migrations-dir", migrations)
	c.Assert(applied.ExitCode, qt.Equals, 0,
		qt.Commentf("description:\n%s\nstdout:\n%s\nstderr:\n%s", read.Stdout, applied.Stdout, applied.Stderr))

	want := []string{"trg_instead INSTEAD OF DELETE", "trg_multi AFTER INSERT, UPDATE"}
	c.Assert(sqlServerTriggerFacts(c, source), qt.DeepEquals, want)
	c.Assert(sqlServerTriggerFacts(c, target), qt.DeepEquals, want)
}
