//go:build integration

package integration_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
)

// indexesRenamed are PostgreSQL schema files that rename an index with ALTER
// INDEX: a declared index, the index behind a UNIQUE, an EXCLUDE or the
// primary key, which renames the constraint too, and a column's own UNIQUE.
// Measured on PostgreSQL 18.6, every file applies, and Atlas CE v1.3.0 reports
// each one synced with the database it builds. Without the reader, the parser
// refused each file with `unsupported ALTER target: INDEX`
// (stokaro/ptah#3879).
var indexesRenamed = []struct {
	name string
	sql  string
}{
	{
		name: "a declared index",
		sql: `CREATE TABLE c (id int PRIMARY KEY, b int);
CREATE INDEX ix ON c (b);
ALTER INDEX ix RENAME TO other;`,
	},
	{
		name: "IF EXISTS, and an index nothing declares",
		sql: `CREATE TABLE c (id int PRIMARY KEY, b int);
CREATE INDEX ix ON c (b);
ALTER INDEX IF EXISTS ix RENAME TO other;
ALTER INDEX IF EXISTS nope RENAME TO other2;`,
	},
	{
		name: "an index of another schema",
		sql: `CREATE SCHEMA app;
CREATE TABLE app.c (id int PRIMARY KEY, b int);
CREATE INDEX ix ON app.c (b);
ALTER INDEX app.ix RENAME TO other;`,
	},
	{
		name: "a column's own UNIQUE",
		sql: `CREATE TABLE c (id int PRIMARY KEY, b int UNIQUE);
ALTER INDEX c_b_key RENAME TO other;`,
	},
	{
		name: "a column's own UNIQUE, then dropped under its new name",
		sql: `CREATE TABLE c (id int PRIMARY KEY, b int UNIQUE, d int UNIQUE);
ALTER INDEX c_b_key RENAME TO other;
ALTER TABLE c DROP CONSTRAINT other;`,
	},
	{
		name: "a named UNIQUE",
		sql: `CREATE TABLE c (id int PRIMARY KEY, b int, CONSTRAINT uq UNIQUE (b));
ALTER INDEX uq RENAME TO other;`,
	},
	{
		name: "the primary key",
		sql: `CREATE TABLE c (id int PRIMARY KEY, b int);
ALTER INDEX c_pkey RENAME TO c_pk;`,
	},
	{
		name: "the old name taken again",
		sql: `CREATE TABLE c (id int PRIMARY KEY, b int);
CREATE INDEX ix ON c (b);
ALTER INDEX ix RENAME TO other;
CREATE INDEX ix ON c (id, b);`,
	},
}

// TestMigrateDiffFindsAnIndexRenamedSyncedE2E replays a directory whose one
// migration is the schema file itself and diffs.
func TestMigrateDiffFindsAnIndexRenamedSyncedE2E(t *testing.T) {
	for _, test := range indexesRenamed {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			dir, schema := writeRewrittenMigrationProject(c, test.sql, test.sql)

			plan, err := runCompatVerb("migrate", "diff", "--dir", "file://"+dir,
				"--to", "file://"+schema, "--dev-url", postgresUniqueEngine.emptyDB(c), "--dry-run")

			c.Assert(err, qt.IsNil, qt.Commentf("%s", plan))
			c.Assert(plan, qt.Contains, "The migration directory is synced with the desired state")
		})
	}
}

// TestSchemaApplyFindsAnIndexRenamedSyncedE2E plans the native apply of the
// file against the database the file built.
func TestSchemaApplyFindsAnIndexRenamedSyncedE2E(t *testing.T) {
	for _, test := range indexesRenamed {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			_, schema := writeRewrittenMigrationProject(c, test.sql, test.sql)
			target := postgresUniqueEngine.builtDB(c, test.sql)

			plan := runPtahNative(c, "schema", "apply", "--db-url", target, "--schema-file", schema, "--dry-run")

			c.Assert(plan, qt.Contains, "Schema is synced")
		})
	}
}
