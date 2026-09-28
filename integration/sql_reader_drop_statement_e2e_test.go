//go:build integration

package integration_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
)

// schemaFilesThatDrop are schema files that create an object and then drop it
// with a DROP TABLE or DROP INDEX statement. Measured on MySQL 8.4.11 and
// PostgreSQL 18.6, each applies, and Atlas CE v1.3.0 reports each file synced
// with the database it builds. Read as though the drop were not there, the
// comparison plans the dropped table or index back (stokaro/ptah#3876).
var schemaFilesThatDrop = []struct {
	name    string
	engines []uniqueEngine
	sql     string
}{
	{
		name:    "DROP INDEX ON",
		engines: mysqlFamilyUniqueEngines,
		sql: `CREATE TABLE c (id int PRIMARY KEY, x int);
CREATE INDEX ix ON c (x);
DROP INDEX ix ON c;`,
	},
	{
		name:    "DROP INDEX ON of a UNIQUE constraint",
		engines: mysqlFamilyUniqueEngines,
		sql: `CREATE TABLE c (id int PRIMARY KEY, x int, CONSTRAINT uq UNIQUE (x));
DROP INDEX uq ON c;`,
	},
	{
		name:    "DROP INDEX ON of a column's own UNIQUE",
		engines: mysqlFamilyUniqueEngines,
		sql: `CREATE TABLE c (id int PRIMARY KEY, x int UNIQUE);
DROP INDEX x ON c;`,
	},
	{
		name:    "DROP INDEX",
		engines: []uniqueEngine{postgresUniqueEngine},
		sql: `CREATE TABLE c (id int PRIMARY KEY, x int);
CREATE INDEX ix ON c (x);
CREATE UNIQUE INDEX ux ON c (x);
DROP INDEX ix;
DROP INDEX IF EXISTS ux;`,
	},
	{
		name:    "DROP INDEX of an index in a schema",
		engines: []uniqueEngine{postgresUniqueEngine},
		sql: `CREATE SCHEMA app;
CREATE TABLE app.c (id int PRIMARY KEY, x int);
CREATE INDEX ix ON app.c (x);
DROP INDEX app.ix;`,
	},
	{
		name:    "DROP TABLE",
		engines: uniqueEngines,
		sql: `CREATE TABLE a (id int PRIMARY KEY);
CREATE TABLE b (id int PRIMARY KEY);
CREATE TABLE c (id int PRIMARY KEY);
DROP TABLE a;
DROP TABLE IF EXISTS b, nope;`,
	},
	{
		name:    "DROP TABLE of a table and the table referring to it",
		engines: uniqueEngines,
		sql: `CREATE TABLE p (id int PRIMARY KEY);
CREATE TABLE c (id int PRIMARY KEY, p_id int, CONSTRAINT fk FOREIGN KEY (p_id) REFERENCES p (id));
CREATE TABLE other (id int PRIMARY KEY);
DROP TABLE c, p;`,
	},
	{
		name:    "DROP TABLE and CREATE TABLE again",
		engines: uniqueEngines,
		sql: `CREATE TABLE a (id int PRIMARY KEY, x int);
DROP TABLE a;
CREATE TABLE a (id int PRIMARY KEY, y int);`,
	},
	{
		name:    "DROP TABLE of a table with row security and a comment",
		engines: []uniqueEngine{postgresUniqueEngine},
		sql: `CREATE TABLE a (id serial PRIMARY KEY);
CREATE TABLE c (id int PRIMARY KEY);
ALTER TABLE a ENABLE ROW LEVEL SECURITY;
CREATE POLICY pol ON a USING (id > 0);
COMMENT ON TABLE a IS 'dropped';
DROP TABLE a;`,
	},
}

// TestMigrateDiffFindsASchemaFileThatDropsSyncedE2E replays a directory whose
// one migration is the schema file itself and diffs.
func TestMigrateDiffFindsASchemaFileThatDropsSyncedE2E(t *testing.T) {
	for _, test := range schemaFilesThatDrop {
		for _, engine := range test.engines {
			t.Run(engine.name+"/"+test.name, func(t *testing.T) {
				c := qt.New(t)
				dir, schema := writeRewrittenMigrationProject(c, test.sql, test.sql)

				plan, err := runCompatVerb("migrate", "diff", "--dir", "file://"+dir,
					"--to", "file://"+schema, "--dev-url", engine.emptyDB(c), "--dry-run")

				c.Assert(err, qt.IsNil, qt.Commentf("%s", plan))
				c.Assert(plan, qt.Contains, "The migration directory is synced with the desired state")
			})
		}
	}
}

// TestSchemaApplyFindsASchemaFileThatDropsSyncedE2E plans the native apply of
// the file against the database the file built.
func TestSchemaApplyFindsASchemaFileThatDropsSyncedE2E(t *testing.T) {
	for _, test := range schemaFilesThatDrop {
		for _, engine := range test.engines {
			t.Run(engine.name+"/"+test.name, func(t *testing.T) {
				c := qt.New(t)
				_, schema := writeRewrittenMigrationProject(c, test.sql, test.sql)
				target := engine.builtDB(c, test.sql)

				plan := runPtahNative(c, "schema", "apply", "--db-url", target, "--schema-file", schema, "--dry-run")

				c.Assert(plan, qt.Contains, "Schema is synced")
			})
		}
	}
}
