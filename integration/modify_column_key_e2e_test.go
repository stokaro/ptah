//go:build integration

package integration_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
)

// columnKeyThroughModify are MySQL-family schema files that MODIFY a column
// with a UNIQUE of its own. The server keeps the key through a MODIFY that
// omits UNIQUE, and a MODIFY that restates it adds a second key under the
// next free name. Measured on MySQL 8.4.11 and 26.7.0 and MariaDB 11.8.9 and
// 12.3.3, every file applies, and Atlas CE v1.3.0 reports each one synced with
// the database it builds. Without the reader keeping the key, each file
// planned a DROP INDEX of a key it keeps, or was refused for dropping x_2, a
// key the reader did not know (stokaro/ptah#3875).
var columnKeyThroughModify = []struct {
	name string
	sql  string
}{
	{
		name: "a MODIFY that omits UNIQUE",
		sql: `CREATE TABLE c (id bigint PRIMARY KEY, x int UNIQUE);
ALTER TABLE c MODIFY COLUMN x bigint;`,
	},
	{
		name: "a MODIFY that restates UNIQUE",
		sql: `CREATE TABLE c (id bigint PRIMARY KEY, x int UNIQUE);
ALTER TABLE c MODIFY COLUMN x bigint UNIQUE;`,
	},
	{
		name: "a MODIFY that restates UNIQUE where another column's key holds x_2",
		sql: `CREATE TABLE c (id bigint PRIMARY KEY, x int UNIQUE, x_2 int UNIQUE);
ALTER TABLE c MODIFY COLUMN x bigint UNIQUE;`,
	},
	{
		name: "UNIQUE restated twice",
		sql: `CREATE TABLE c (id bigint PRIMARY KEY, x int UNIQUE);
ALTER TABLE c MODIFY COLUMN x bigint UNIQUE;
ALTER TABLE c MODIFY COLUMN x bigint UNIQUE;`,
	},
	{
		name: "the column's own key dropped beside the added one",
		sql: `CREATE TABLE c (id bigint PRIMARY KEY, x int UNIQUE);
ALTER TABLE c MODIFY COLUMN x bigint UNIQUE;
ALTER TABLE c DROP INDEX x;`,
	},
	{
		name: "the added key dropped by its name",
		sql: `CREATE TABLE c (id bigint PRIMARY KEY, x int UNIQUE);
ALTER TABLE c MODIFY COLUMN x bigint UNIQUE;
ALTER TABLE c DROP INDEX x_2;`,
	},
}

// TestMigrateDiffFindsAColumnKeyThroughModifySyncedE2E replays a directory
// whose one migration is the schema file itself and diffs.
func TestMigrateDiffFindsAColumnKeyThroughModifySyncedE2E(t *testing.T) {
	for _, test := range columnKeyThroughModify {
		for _, engine := range mysqlFamilyUniqueEngines {
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

// TestSchemaApplyFindsAColumnKeyThroughModifySyncedE2E plans the native apply
// of the file against the database the file built.
func TestSchemaApplyFindsAColumnKeyThroughModifySyncedE2E(t *testing.T) {
	for _, test := range columnKeyThroughModify {
		for _, engine := range mysqlFamilyUniqueEngines {
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
