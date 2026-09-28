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

// keyIndexDropEngines are the engines that keep an index for every foreign
// key and refuse a drop that takes the last one away.
var keyIndexDropEngines = []struct {
	name   string
	engine dbtarget.Engine
}{
	{name: "MySQL", engine: dbtarget.MySQLAdmin},
	{name: "MariaDB", engine: dbtarget.MariaDBAdmin},
}

// keyIndexDropRun is one schema file run on the server as written and planned
// by `schema apply --dry-run`.
type keyIndexDropRun struct {
	serverErr error
	ptahErr   error
	out       string
}

// runKeyIndexDrop runs schema on a scratch database of engine, and plans it
// against another.
func runKeyIndexDrop(c *qt.C, engine dbtarget.Engine, schema string) keyIndexDropRun {
	c.Helper()
	scratch := newMySQLFamilyScratch(c, engine)
	built, _ := scratch.database(c, "keyindex_built")
	_, target := scratch.database(c, "keyindex_target")
	_, dev := scratch.database(c, "keyindex_dev")
	file := filepath.Join(c.TempDir(), "schema.sql")
	c.Assert(os.WriteFile(file, []byte(schema), 0o600), qt.IsNil)
	conn, err := sql.Open("mysql", mySQLDSNForDatabase(c, scratch.adminDSN, built)+"?multiStatements=true")
	c.Assert(err, qt.IsNil)
	defer func() { c.Check(conn.Close(), qt.IsNil) }()
	_, serverErr := conn.ExecContext(c.Context(), schema)
	out, ptahErr := runCompatVerb("schema", "apply", "--url", target, "--to", "file://"+file,
		"--dev-url", dev, "--dry-run")
	return keyIndexDropRun{serverErr: serverErr, ptahErr: ptahErr, out: out}
}

// TestSchemaApplyRefusesDroppingTheIndexAForeignKeyNeedsE2E runs a schema file
// that drops the last index a foreign key can be checked against. The server
// answers ERROR 1553, and Ptah refuses the file too, naming the key. Read as a
// drop, the plan dropped an index the server refuses to drop
// (stokaro/ptah#3913).
func TestSchemaApplyRefusesDroppingTheIndexAForeignKeyNeedsE2E(t *testing.T) {
	const table = "CREATE TABLE p (id int PRIMARY KEY);\n" +
		"CREATE TABLE c (id int PRIMARY KEY, x int, KEY ix (x), CONSTRAINT fk FOREIGN KEY (x) REFERENCES p (id));\n"
	for _, engine := range keyIndexDropEngines {
		for _, drop := range []string{"DROP INDEX ix ON c;\n", "ALTER TABLE c DROP INDEX ix;\n"} {
			t.Run(engine.name+"/"+drop, func(t *testing.T) {
				c := qt.New(t)

				run := runKeyIndexDrop(c, engine.engine, table+drop)

				c.Assert(run.serverErr, qt.ErrorMatches, `Error 1553 \(HY000\): Cannot drop index 'ix': needed in a foreign key constraint`)
				c.Assert(run.ptahErr, qt.ErrorMatches, `(?s).*foreign key fk needs an index that begins with \(x\).*ERROR 1553.*`)
			})
		}
	}
}

// TestSchemaApplyTakesDroppingAnIndexAForeignKeyCanDoWithoutE2E is the control:
// another index begins with the key's columns, so the server takes the drop
// and checks the key against that index, and Ptah plans the file.
func TestSchemaApplyTakesDroppingAnIndexAForeignKeyCanDoWithoutE2E(t *testing.T) {
	const schema = "CREATE TABLE p (id int PRIMARY KEY);\n" +
		"CREATE TABLE c (id int PRIMARY KEY, x int, KEY ix (x), KEY ix2 (x, id), CONSTRAINT fk FOREIGN KEY (x) REFERENCES p (id));\n" +
		"DROP INDEX ix ON c;\n"
	for _, engine := range keyIndexDropEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)

			run := runKeyIndexDrop(c, engine.engine, schema)

			c.Assert(run.serverErr, qt.IsNil)
			c.Assert(run.ptahErr, qt.IsNil, qt.Commentf("%s", run.out))
		})
	}
}
