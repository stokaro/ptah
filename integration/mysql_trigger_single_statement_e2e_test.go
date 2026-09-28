//go:build integration

package integration_test

import (
	"database/sql"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/dbtarget"
)

// singleStatementTriggerFile is a MySQL trigger whose body is one statement,
// which MySQL and MariaDB run and Atlas CE v1.3.0 reports synced with the
// database the file builds (stokaro/ptah#3915).
const singleStatementTriggerFile = "CREATE TABLE a (id int PRIMARY KEY, n int);\n" +
	"CREATE TRIGGER tr BEFORE INSERT ON a FOR EACH ROW SET NEW.n = NEW.id;\n"

// singleStatementTriggerEngines are the engines whose trigger body may be one
// statement.
var singleStatementTriggerEngines = []struct {
	name   string
	engine dbtarget.Engine
}{
	{name: "MySQL", engine: dbtarget.MySQLAdmin},
	{name: "MariaDB", engine: dbtarget.MariaDBAdmin},
}

// TestSchemaDiffFindsASingleStatementTriggerSyncedE2E diffs the database the
// file built with the file through ptah-compat: synced. The reader refused
// the file for want of BEGIN.
func TestSchemaDiffFindsASingleStatementTriggerSyncedE2E(t *testing.T) {
	for _, engine := range singleStatementTriggerEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			scratch := newMySQLFamilyScratch(c, engine.engine)
			_, built := scratch.builtFrom(c, singleStatementTriggerFile)
			_, dev := scratch.database(c, "trigger_dev")
			file := filepath.Join(c.TempDir(), "schema.sql")
			c.Assert(os.WriteFile(file, []byte(singleStatementTriggerFile), 0o600), qt.IsNil)

			out, err := runCompatVerb("schema", "diff", "--from", built, "--to", "file://"+file, "--dev-url", dev)

			c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
			c.Assert(out, qt.Contains, "Schemas are synced")
		})
	}
}

// TestSchemaApplyCreatesASingleStatementTriggerE2E applies the file to an
// empty database natively and inserts a row: the trigger ran and set n.
func TestSchemaApplyCreatesASingleStatementTriggerE2E(t *testing.T) {
	for _, engine := range singleStatementTriggerEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			scratch := newMySQLFamilyScratch(c, engine.engine)
			name, target := scratch.database(c, "trigger_target")
			file := filepath.Join(c.TempDir(), "schema.sql")
			c.Assert(os.WriteFile(file, []byte(singleStatementTriggerFile), 0o600), qt.IsNil)

			runPtahNative(c, "schema", "apply", "--db-url", target, "--schema-file", file, "--auto-approve")
			conn, err := sql.Open("mysql", mySQLDSNForDatabase(c, scratch.adminDSN, name))
			c.Assert(err, qt.IsNil)
			defer func() { c.Check(conn.Close(), qt.IsNil) }()
			_, err = conn.ExecContext(c.Context(), "INSERT INTO a (id) VALUES (7)")
			c.Assert(err, qt.IsNil)
			var n int
			c.Assert(conn.QueryRowContext(c.Context(), "SELECT n FROM a WHERE id = 7").Scan(&n), qt.IsNil)

			c.Assert(n, qt.Equals, 7)
			c.Assert(runPtahNative(c, "schema", "apply", "--db-url", target, "--schema-file", file, "--dry-run"),
				qt.Contains, "Schema is synced")
		})
	}
}
