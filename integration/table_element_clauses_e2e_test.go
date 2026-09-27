//go:build integration

package integration_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
)

// clausesThatChangeNothing are schema files, one per engine, whose table
// elements carry clauses that build what the elements build without them:
// `ENFORCED`, `NOT VALID` in CREATE TABLE, `MATCH SIMPLE`, `VISIBLE`,
// MariaDB's `NOT IGNORED`, and `USING` after a key's parts. Unread, each file
// is refused as `unexpected <word> after a table element`
// (stokaro/ptah#3826). Each builds on its server, and the file compared with
// the database it built is synced.
var clausesThatChangeNothing = map[string]string{
	"PostgreSQL": `CREATE TABLE p (id int PRIMARY KEY);
CREATE TABLE c (
  id int PRIMARY KEY,
  a int CHECK (a > 0) ENFORCED,
  b int REFERENCES p (id) MATCH SIMPLE ON DELETE CASCADE ENFORCED,
  CONSTRAINT c_b_ck CHECK (b > 0) NOT VALID,
  FOREIGN KEY (a) REFERENCES p (id) NOT VALID
);`,
	"MySQL": `CREATE TABLE p (id int PRIMARY KEY);
CREATE TABLE c (
  id int PRIMARY KEY,
  a int,
  b int,
  CONSTRAINT c_a_ck CHECK (a > 0) ENFORCED,
  UNIQUE KEY c_a (a) USING BTREE VISIBLE,
  KEY c_b (b) USING HASH,
  FOREIGN KEY (b) REFERENCES p (id) MATCH SIMPLE ON DELETE CASCADE
);`,
	"MariaDB": `CREATE TABLE p (id int PRIMARY KEY);
CREATE TABLE c (
  id int PRIMARY KEY,
  a int,
  b int,
  CONSTRAINT c_a_ck CHECK (a > 0),
  UNIQUE KEY c_a (a) USING HASH NOT IGNORED,
  KEY c_b (b) VISIBLE,
  FOREIGN KEY (b) REFERENCES p (id) MATCH SIMPLE ON DELETE CASCADE
);`,
}

// TestSchemaApplyReadsClausesThatChangeNothingSyncedE2E plans the native
// apply of each file against the database it built: nothing is planned.
func TestSchemaApplyReadsClausesThatChangeNothingSyncedE2E(t *testing.T) {
	for _, engine := range uniqueEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			sql := clausesThatChangeNothing[engine.name]
			_, schema := writeRewrittenMigrationProject(c, sql, sql)
			target := engine.builtDB(c, sql)

			plan := runPtahNative(c, "schema", "apply", "--db-url", target, "--schema-file", schema, "--dry-run")

			c.Assert(plan, qt.Contains, "Schema is synced")
		})
	}
}

// TestMigrateDiffReadsClausesThatChangeNothingSyncedE2E replays a directory
// whose one migration is the file, and diffs it with the file.
func TestMigrateDiffReadsClausesThatChangeNothingSyncedE2E(t *testing.T) {
	for _, engine := range uniqueEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			sql := clausesThatChangeNothing[engine.name]
			dir, schema := writeRewrittenMigrationProject(c, sql, sql)

			out, err := runCompatVerb("migrate", "diff", "--dir", "file://"+dir,
				"--to", "file://"+schema, "--dev-url", engine.emptyDB(c), "--dry-run")

			c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
			c.Assert(out, qt.Contains, "The migration directory is synced with the desired state")
		})
	}
}

// clauseChanges are a database and a file that differs from it only in what a
// clause after a table element reads into. The plan carries the difference,
// so the clause is read into the element rather than stepped over.
var clauseChanges = []struct {
	engine     string
	name       string
	migration  string
	schema     string
	wantInPlan string
}{
	{
		engine:     "PostgreSQL",
		name:       "ON DELETE after MATCH SIMPLE",
		migration:  `CREATE TABLE p (id int PRIMARY KEY); CREATE TABLE c (a int, FOREIGN KEY (a) REFERENCES p (id));`,
		schema:     `CREATE TABLE p (id int PRIMARY KEY); CREATE TABLE c (a int, FOREIGN KEY (a) REFERENCES p (id) MATCH SIMPLE ON DELETE CASCADE);`,
		wantInPlan: "ON DELETE CASCADE",
	},
	{
		// MySQL 8.4 and 26.7 keep the action beside MATCH SIMPLE.
		engine:     "MySQL",
		name:       "ON DELETE after MATCH SIMPLE",
		migration:  `CREATE TABLE p (id int PRIMARY KEY); CREATE TABLE c (a int, FOREIGN KEY (a) REFERENCES p (id));`,
		schema:     `CREATE TABLE p (id int PRIMARY KEY); CREATE TABLE c (a int, FOREIGN KEY (a) REFERENCES p (id) MATCH SIMPLE ON DELETE CASCADE);`,
		wantInPlan: "ON DELETE CASCADE",
	},
	{
		engine:     "MariaDB",
		name:       "ON DELETE after MATCH SIMPLE",
		migration:  `CREATE TABLE p (id int PRIMARY KEY); CREATE TABLE c (a int, FOREIGN KEY (a) REFERENCES p (id));`,
		schema:     `CREATE TABLE p (id int PRIMARY KEY); CREATE TABLE c (a int, FOREIGN KEY (a) REFERENCES p (id) MATCH SIMPLE ON DELETE CASCADE);`,
		wantInPlan: "ON DELETE CASCADE",
	},
	{
		// MariaDB 11.8 and 12.3 record the method a key asks for. MySQL's
		// InnoDB records BTREE whatever it is asked, so MySQL has no row.
		engine:     "MariaDB",
		name:       "USING HASH after a key's parts",
		migration:  `CREATE TABLE c (a int, KEY c_a (a));`,
		schema:     `CREATE TABLE c (a int, KEY c_a (a) USING HASH);`,
		wantInPlan: "USING HASH",
	},
}

// uniqueEngineNamed answers the engine of uniqueEngines called name.
func uniqueEngineNamed(c *qt.C, name string) uniqueEngine {
	c.Helper()
	for _, engine := range uniqueEngines {
		if engine.name == name {
			return engine
		}
	}
	c.Fatalf("no engine %q", name)
	return uniqueEngine{}
}

// TestSchemaApplyPlansAClauseAfterATableElementE2E is the control for the
// synced rows: what a clause reads into reaches the plan.
func TestSchemaApplyPlansAClauseAfterATableElementE2E(t *testing.T) {
	for _, test := range clauseChanges {
		t.Run(test.engine+"/"+test.name, func(t *testing.T) {
			c := qt.New(t)
			engine := uniqueEngineNamed(c, test.engine)
			_, schema := writeRewrittenMigrationProject(c, test.migration, test.schema)
			target := engine.builtDB(c, test.migration)

			plan := runPtahNative(c, "schema", "apply", "--db-url", target, "--schema-file", schema, "--dry-run")

			c.Assert(plan, qt.Contains, test.wantInPlan)
		})
	}
}
