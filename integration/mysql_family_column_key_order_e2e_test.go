//go:build integration

package integration_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/dbtarget"
)

// columnKeyOrder are schema files that write a column's own UNIQUE or
// REFERENCES after a table-level key, with the secondary indexes of `c` the
// server built from each.
//
// MySQL and MariaDB name the keys of a CREATE TABLE in the order the body
// writes them, and a column's own UNIQUE, or the index MariaDB builds for its
// REFERENCES, takes its name at the column's place (stokaro/ptah#3803). A file
// whose model names them in another order plans to rename them against the
// database it built, or is refused. Every row was measured on MySQL 8.4.11 and
// 26.7.0 and MariaDB 11.8.9 and 12.3.3, where it runs, and Atlas CE v1.3.0
// reports each synced. A column's REFERENCES runs on MariaDB only: MySQL 8.4
// builds nothing from it, and Ptah refuses it under `--dialect mysql`.
var columnKeyOrder = []struct {
	name    string
	engines []dbtarget.Engine
	schema  string
	wantIdx string
}{
	{
		name:    "an index over the column written before it",
		engines: bothMySQLEngines,
		schema: `CREATE TABLE p (id int PRIMARY KEY);
CREATE TABLE c (id int PRIMARY KEY, KEY (a), a int UNIQUE);`,
		wantIdx: "a(a) a_2(a)",
	},
	{
		name:    "an index the column leads, written before it",
		engines: bothMySQLEngines,
		schema: `CREATE TABLE p (id int PRIMARY KEY);
CREATE TABLE c (id int PRIMARY KEY, b int, KEY (a, b), a int UNIQUE);`,
		wantIdx: "a(a,b) a_2(a)",
	},
	{
		name:    "a UNIQUE the column leads, written before it",
		engines: bothMySQLEngines,
		schema: `CREATE TABLE p (id int PRIMARY KEY);
CREATE TABLE c (id int PRIMARY KEY, b int, UNIQUE KEY (a, b), a int UNIQUE);`,
		wantIdx: "a(a,b) a_2(a)",
	},
	{
		name:    "an index named after the column, written before it",
		engines: bothMySQLEngines,
		schema: `CREATE TABLE p (id int PRIMARY KEY);
CREATE TABLE c (id int PRIMARY KEY, b int, KEY a (b), a int UNIQUE);`,
		wantIdx: "a(b) a_2(a)",
	},
	{
		name:    "an index over the column on both sides of it",
		engines: bothMySQLEngines,
		schema: `CREATE TABLE p (id int PRIMARY KEY);
CREATE TABLE c (id int PRIMARY KEY, KEY (a), a int UNIQUE, KEY (a));`,
		wantIdx: "a(a) a_2(a) a_3(a)",
	},
	{
		name:    "indexes on both sides of the column",
		engines: bothMySQLEngines,
		schema: `CREATE TABLE p (id int PRIMARY KEY);
CREATE TABLE c (id int PRIMARY KEY, b int, KEY (b), a int UNIQUE, KEY (a), KEY (a));`,
		wantIdx: "a(a) a_2(a) a_3(a) b(b)",
	},
	{
		name:    "control: the column written first",
		engines: bothMySQLEngines,
		schema: `CREATE TABLE p (id int PRIMARY KEY);
CREATE TABLE c (id int PRIMARY KEY, a int UNIQUE, KEY (a));`,
		wantIdx: "a(a) a_2(a)",
	},
	{
		name:    "a column's REFERENCES after an index named after the column",
		engines: mariaDBOnly,
		schema: `CREATE TABLE p (id int PRIMARY KEY);
CREATE TABLE c (id int PRIMARY KEY, b int, KEY a (b), a int REFERENCES p(id));`,
		wantIdx: "a(b) a_2(a)",
	},
	{
		name:    "a column's UNIQUE REFERENCES after an index named after the column",
		engines: mariaDBOnly,
		schema: `CREATE TABLE p (id int PRIMARY KEY);
CREATE TABLE c (id int PRIMARY KEY, b int, KEY a (b), a int UNIQUE REFERENCES p(id));`,
		wantIdx: "a(b) a_2(a)",
	},
	{
		name:    "a column's REFERENCES after an equal table key, which is dropped",
		engines: mariaDBOnly,
		schema: `CREATE TABLE p (id int PRIMARY KEY);
CREATE TABLE c (id int PRIMARY KEY, CONSTRAINT fk FOREIGN KEY (a) REFERENCES p(id), a int REFERENCES p(id));
ALTER TABLE c DROP FOREIGN KEY fk;`,
		wantIdx: "a(a)",
	},
	{
		name:    "control: a column's REFERENCES before an equal table key, which is dropped",
		engines: mariaDBOnly,
		schema: `CREATE TABLE p (id int PRIMARY KEY);
CREATE TABLE c (id int PRIMARY KEY, a int REFERENCES p(id), CONSTRAINT fk FOREIGN KEY (a) REFERENCES p(id));
ALTER TABLE c DROP FOREIGN KEY fk;`,
		wantIdx: "fk(a)",
	},
}

// TestMigrateDiffFindsAColumnKeyOrderSyncedE2E replays a directory whose one
// migration is the schema file itself and diffs: the directory is synced.
// Applied to an empty database, the directory builds the indexes the row
// names, which is what the file is compared with.
func TestMigrateDiffFindsAColumnKeyOrderSyncedE2E(t *testing.T) {
	for _, test := range columnKeyOrder {
		for _, engine := range test.engines {
			t.Run(engine.String()+"/"+test.name, func(t *testing.T) {
				c := qt.New(t)
				scratch := newMySQLFamilyScratch(c, engine)
				dir, schema := writeRewrittenMigrationProject(c, test.schema, test.schema)
				_, dev := scratch.database(c, "colkey_dev")
				targetName, target := scratch.database(c, "colkey_target")

				plan, err := runCompatVerb("migrate", "diff", "--dir", "file://"+dir,
					"--to", "file://"+schema, "--dev-url", dev, "--dry-run")
				c.Assert(err, qt.IsNil, qt.Commentf("%s", plan))
				out, err := runCompatVerb("migrate", "apply", "--url", target, "--dir", "file://"+dir)
				c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
				_, indexes := scratch.keysOfC(c, targetName)

				c.Assert(plan, qt.Contains, "The migration directory is synced with the desired state")
				c.Assert(indexes, qt.Equals, test.wantIdx)
			})
		}
	}
}

// TestSchemaApplyFindsAColumnKeyOrderSyncedE2E plans the native apply of the
// schema file against the database the same file built, with nothing of
// Ptah's in between: nothing is planned.
func TestSchemaApplyFindsAColumnKeyOrderSyncedE2E(t *testing.T) {
	for _, test := range columnKeyOrder {
		for _, engine := range test.engines {
			t.Run(engine.String()+"/"+test.name, func(t *testing.T) {
				c := qt.New(t)
				scratch := newMySQLFamilyScratch(c, engine)
				_, schema := writeRewrittenMigrationProject(c, test.schema, test.schema)
				targetName, target := scratch.builtFrom(c, test.schema)

				plan := runPtahNative(c, "schema", "apply", "--db-url", target, "--schema-file", schema, "--dry-run")
				_, indexes := scratch.keysOfC(c, targetName)

				c.Assert(indexes, qt.Equals, test.wantIdx)
				c.Assert(plan, qt.Contains, "Schema is synced")
			})
		}
	}
}
