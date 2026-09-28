//go:build integration

package integration_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
)

// columnKeyDroppedOrRenamed are schema files that drop or rename a column's own
// UNIQUE by the name the server gives it: the column's name on MySQL and
// MariaDB, with `_2` where another key holds it, and `<table>_<column>_key` on
// PostgreSQL, with `1` where a relation holds it. Measured on MySQL 8.4.11 and
// 26.7.0, MariaDB 11.8.9 and 12.3.3 and PostgreSQL 18.6, every statement
// applies, and Atlas CE v1.3.0 reports each file synced with the database it
// builds. Without the reader finding the column's key under that name, it
// refuses each file, answering that the schema does not declare the
// constraint, and RENAME INDEX does not parse (stokaro/ptah#3860).
var columnKeyDroppedOrRenamed = []struct {
	name    string
	engines []uniqueEngine
	sql     string
}{
	{
		name:    "DROP INDEX under the column's name",
		engines: mysqlFamilyUniqueEngines,
		sql: `CREATE TABLE c (id int PRIMARY KEY, b int UNIQUE);
ALTER TABLE c DROP INDEX b;`,
	},
	{
		name:    "DROP CONSTRAINT under the column's name",
		engines: mysqlFamilyUniqueEngines,
		sql: `CREATE TABLE c (id int PRIMARY KEY, b int UNIQUE);
ALTER TABLE c DROP CONSTRAINT b;`,
	},
	{
		name:    "DROP KEY under the next name",
		engines: mysqlFamilyUniqueEngines,
		sql: `CREATE TABLE c (id int PRIMARY KEY, y int, KEY b (y));
ALTER TABLE c ADD COLUMN b int UNIQUE;
ALTER TABLE c DROP KEY b_2;`,
	},
	{
		name:    "RENAME INDEX",
		engines: mysqlFamilyUniqueEngines,
		sql: `CREATE TABLE c (id int PRIMARY KEY, b int UNIQUE);
ALTER TABLE c RENAME INDEX b TO other;`,
	},
	{
		name:    "RENAME KEY under the next name",
		engines: mysqlFamilyUniqueEngines,
		sql: `CREATE TABLE c (id int PRIMARY KEY, y int, KEY b (y));
ALTER TABLE c ADD COLUMN b int UNIQUE;
ALTER TABLE c RENAME KEY b_2 TO other;`,
	},
	{
		name:    "RENAME INDEX and then DROP INDEX",
		engines: mysqlFamilyUniqueEngines,
		sql: `CREATE TABLE c (id int PRIMARY KEY, b int UNIQUE);
ALTER TABLE c RENAME INDEX b TO other;
ALTER TABLE c DROP INDEX other;`,
	},
	{
		name:    "DROP CONSTRAINT under the table's name for the column",
		engines: []uniqueEngine{postgresUniqueEngine},
		sql: `CREATE TABLE c (id int PRIMARY KEY, b int UNIQUE);
ALTER TABLE c DROP CONSTRAINT c_b_key;`,
	},
	{
		name:    "DROP CONSTRAINT IF EXISTS under the table's name for the column",
		engines: []uniqueEngine{postgresUniqueEngine},
		sql: `CREATE TABLE c (id int PRIMARY KEY, b int UNIQUE);
ALTER TABLE c DROP CONSTRAINT IF EXISTS c_b_key;`,
	},
	{
		name:    "DROP CONSTRAINT under the next name",
		engines: []uniqueEngine{postgresUniqueEngine},
		sql: `CREATE TABLE c (id int PRIMARY KEY, y int);
CREATE UNIQUE INDEX c_b_key ON c (y);
ALTER TABLE c ADD COLUMN b int UNIQUE;
ALTER TABLE c DROP CONSTRAINT c_b_key1;`,
	},
	{
		name:    "RENAME CONSTRAINT",
		engines: []uniqueEngine{postgresUniqueEngine},
		sql: `CREATE TABLE c (id int PRIMARY KEY, b int UNIQUE);
ALTER TABLE c RENAME CONSTRAINT c_b_key TO other;`,
	},
	{
		name:    "RENAME CONSTRAINT and then DROP CONSTRAINT",
		engines: []uniqueEngine{postgresUniqueEngine},
		sql: `CREATE TABLE c (id int PRIMARY KEY, b int UNIQUE);
ALTER TABLE c RENAME CONSTRAINT c_b_key TO other;
ALTER TABLE c DROP CONSTRAINT other;`,
	},
}

// TestMigrateDiffFindsAColumnKeyDroppedOrRenamedByNameSyncedE2E replays a
// directory whose one migration is the schema file itself and diffs.
func TestMigrateDiffFindsAColumnKeyDroppedOrRenamedByNameSyncedE2E(t *testing.T) {
	for _, test := range columnKeyDroppedOrRenamed {
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

// TestSchemaApplyFindsAColumnKeyDroppedOrRenamedByNameSyncedE2E plans the
// native apply of the file against the database the file built.
func TestSchemaApplyFindsAColumnKeyDroppedOrRenamedByNameSyncedE2E(t *testing.T) {
	for _, test := range columnKeyDroppedOrRenamed {
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
