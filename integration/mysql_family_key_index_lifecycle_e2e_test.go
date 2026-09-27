//go:build integration

package integration_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/dbtarget"
)

// keyIndexLifecycle are schema files in which the index a MySQL or MariaDB
// server builds for a foreign key outlives the key, or gives way to another
// index, with the secondary indexes of `c` the server built from each.
//
// The server keeps that index after `DROP FOREIGN KEY` (stokaro/ptah#3763),
// and drops it once an added index begins with its columns -- the index a
// MySQL `FOREIGN KEY name (columns)` clause names included
// (stokaro/ptah#3766). A file whose model says otherwise plans to drop or add
// an index against the database it built. Every row was measured on MySQL
// 8.4.11 and 26.7.0 and MariaDB 11.8.9 and 12.3.3, where it runs, and Atlas
// CE v1.3.0 reports each synced. A row that drops an unnamed key by
// `c_ibfk_1` runs on MySQL only: MariaDB 12.1 and later name that key `1`.
var keyIndexLifecycle = []struct {
	name    string
	engines []dbtarget.Engine
	schema  string
	wantIdx string
}{
	{
		name:    "a named key's index outlives the key",
		engines: bothMySQLEngines,
		schema: `CREATE TABLE p (id int PRIMARY KEY);
CREATE TABLE c (id int PRIMARY KEY, a int, CONSTRAINT fk FOREIGN KEY (a) REFERENCES p(id));
ALTER TABLE c DROP FOREIGN KEY fk;`,
		wantIdx: "fk(a)",
	},
	{
		name:    "a key named the way the server names one",
		engines: bothMySQLEngines,
		schema: `CREATE TABLE p (id int PRIMARY KEY);
CREATE TABLE c (id int PRIMARY KEY, a int, CONSTRAINT c_ibfk_1 FOREIGN KEY (a) REFERENCES p(id));
ALTER TABLE c DROP FOREIGN KEY c_ibfk_1;`,
		wantIdx: "c_ibfk_1(a)",
	},
	{
		name:    "an unnamed key's index outlives the key",
		engines: []dbtarget.Engine{dbtarget.MySQLAdmin},
		schema: `CREATE TABLE p (id int PRIMARY KEY);
CREATE TABLE c (id int PRIMARY KEY, a int, b int, KEY a (b), FOREIGN KEY (a) REFERENCES p(id));
ALTER TABLE c DROP FOREIGN KEY c_ibfk_1;`,
		wantIdx: "a(b) a_2(a)",
	},
	{
		name:    "DROP CONSTRAINT leaves the index too",
		engines: bothMySQLEngines,
		schema: `CREATE TABLE p (id int PRIMARY KEY);
CREATE TABLE c (id int PRIMARY KEY, a int, CONSTRAINT fk FOREIGN KEY (a) REFERENCES p(id));
ALTER TABLE c DROP CONSTRAINT fk;`,
		wantIdx: "fk(a)",
	},
	{
		name:    "DROP FOREIGN KEY keeps an index of the same name",
		engines: bothMySQLEngines,
		schema: `CREATE TABLE p (id int PRIMARY KEY);
CREATE TABLE c (id int PRIMARY KEY, a int, KEY fk (a), CONSTRAINT fk FOREIGN KEY (a) REFERENCES p(id));
ALTER TABLE c DROP FOREIGN KEY fk;`,
		wantIdx: "fk(a)",
	},
	{
		name:    "the kept index outlives an index over other columns",
		engines: bothMySQLEngines,
		schema: `CREATE TABLE p (id int PRIMARY KEY);
CREATE TABLE c (id int PRIMARY KEY, a int, b int, CONSTRAINT fk FOREIGN KEY (a) REFERENCES p(id));
ALTER TABLE c DROP FOREIGN KEY fk;
ALTER TABLE c ADD KEY (b);`,
		wantIdx: "b(b) fk(a)",
	},
	{
		name:    "the kept index gives way to an index that begins with its columns",
		engines: bothMySQLEngines,
		schema: `CREATE TABLE p (id int PRIMARY KEY);
CREATE TABLE c (id int PRIMARY KEY, a int, b int, CONSTRAINT fk FOREIGN KEY (a) REFERENCES p(id));
ALTER TABLE c DROP FOREIGN KEY fk;
ALTER TABLE c ADD KEY kab (a, b);`,
		wantIdx: "kab(a,b)",
	},
	{
		name:    "a longer key's index replaces the shorter key's and outlives it",
		engines: bothMySQLEngines,
		schema: `CREATE TABLE p (id int PRIMARY KEY, x int, y int, UNIQUE KEY (x, y));
CREATE TABLE c (id int PRIMARY KEY, a int, b int, CONSTRAINT fk FOREIGN KEY (a) REFERENCES p(id));
ALTER TABLE c ADD CONSTRAINT fk2 FOREIGN KEY (a, b) REFERENCES p(x, y);
ALTER TABLE c DROP FOREIGN KEY fk2;`,
		wantIdx: "fk2(a,b)",
	},
	{
		name:    "a shorter key reuses a longer key's index, which outlives the longer key",
		engines: bothMySQLEngines,
		schema: `CREATE TABLE p (id int PRIMARY KEY, x int, y int, UNIQUE KEY (x, y));
CREATE TABLE c (id int PRIMARY KEY, a int, b int, CONSTRAINT fk FOREIGN KEY (a, b) REFERENCES p(x, y));
ALTER TABLE c ADD CONSTRAINT fk2 FOREIGN KEY (a) REFERENCES p(id);
ALTER TABLE c DROP FOREIGN KEY fk;`,
		wantIdx: "fk(a,b)",
	},
	{
		name:    "a shorter key that reused a longer key's index leaves nothing when dropped",
		engines: bothMySQLEngines,
		schema: `CREATE TABLE p (id int PRIMARY KEY, x int, y int, UNIQUE KEY (x, y));
CREATE TABLE c (id int PRIMARY KEY, a int, b int, CONSTRAINT fk FOREIGN KEY (a, b) REFERENCES p(x, y));
ALTER TABLE c ADD CONSTRAINT fk2 FOREIGN KEY (a) REFERENCES p(id);
ALTER TABLE c DROP FOREIGN KEY fk2;`,
		wantIdx: "fk(a,b)",
	},
	{
		name:    "the later of two identical keys builds the index, which outlives it",
		engines: bothMySQLEngines,
		schema: `CREATE TABLE p (id int PRIMARY KEY);
CREATE TABLE c (id int PRIMARY KEY, a int, CONSTRAINT fk FOREIGN KEY (a) REFERENCES p(id),
  CONSTRAINT fk2 FOREIGN KEY (a) REFERENCES p(id));
ALTER TABLE c DROP FOREIGN KEY fk2;`,
		wantIdx: "fk2(a)",
	},
	{
		name:    "a clause's index gives way to a UNIQUE, which takes its place",
		engines: bothMySQLEngines,
		schema: `CREATE TABLE p (id int PRIMARY KEY);
CREATE TABLE c (id int PRIMARY KEY, a int, FOREIGN KEY idx (a) REFERENCES p(id));
ALTER TABLE c ADD UNIQUE (a);`,
		wantIdx: "a(a)",
	},
	{
		name:    "a clause's index gives way to a named index",
		engines: bothMySQLEngines,
		schema: `CREATE TABLE p (id int PRIMARY KEY);
CREATE TABLE c (id int PRIMARY KEY, a int, FOREIGN KEY idx (a) REFERENCES p(id));
ALTER TABLE c ADD KEY idx2 (a);`,
		wantIdx: "idx2(a)",
	},
	{
		name:    "an index the body declares covers the clause's key",
		engines: bothMySQLEngines,
		schema: `CREATE TABLE p (id int PRIMARY KEY);
CREATE TABLE c (id int PRIMARY KEY, a int, KEY k (a), FOREIGN KEY idx (a) REFERENCES p(id));`,
		wantIdx: "k(a)",
	},
	{
		name:    "a longer key's index takes the clause index's place",
		engines: bothMySQLEngines,
		schema: `CREATE TABLE p (id int PRIMARY KEY, x int, y int, UNIQUE KEY (x, y));
CREATE TABLE c (id int PRIMARY KEY, a int, b int, FOREIGN KEY idx (a) REFERENCES p(id),
  FOREIGN KEY (a, b) REFERENCES p(x, y));`,
		wantIdx: "a(a,b)",
	},
	{
		name:    "a clause's index outlives its key on MySQL",
		engines: []dbtarget.Engine{dbtarget.MySQLAdmin},
		schema: `CREATE TABLE p (id int PRIMARY KEY);
CREATE TABLE c (id int PRIMARY KEY, a int, FOREIGN KEY idx (a) REFERENCES p(id));
ALTER TABLE c DROP FOREIGN KEY c_ibfk_1;`,
		wantIdx: "idx(a)",
	},
}

// TestMigrateDiffFindsAKeyIndexLifecycleSyncedE2E replays a directory whose one
// migration is the schema file itself and diffs: the directory is synced.
// Applied to an empty database, the directory builds the indexes the row
// names, which is what the file is compared with.
func TestMigrateDiffFindsAKeyIndexLifecycleSyncedE2E(t *testing.T) {
	for _, test := range keyIndexLifecycle {
		for _, engine := range test.engines {
			t.Run(engine.String()+"/"+test.name, func(t *testing.T) {
				c := qt.New(t)
				scratch := newMySQLFamilyScratch(c, engine)
				dir, schema := writeRewrittenMigrationProject(c, test.schema, test.schema)
				_, dev := scratch.database(c, "keyidx_dev")
				targetName, target := scratch.database(c, "keyidx_target")

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

// TestSchemaApplyFindsAKeyIndexLifecycleSyncedE2E plans the native apply of
// the schema file against the database the same file built, with nothing of
// Ptah's in between: nothing is planned.
func TestSchemaApplyFindsAKeyIndexLifecycleSyncedE2E(t *testing.T) {
	for _, test := range keyIndexLifecycle {
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
