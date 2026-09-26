//go:build integration

package integration_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
)

// mysqlKeysWithoutSymbol are MySQL schema files that write a primary or unique
// key after a CONSTRAINT keyword with no symbol, with the unique indexes of `c`
// MySQL 8.4.11 built from each, read back from information_schema. The server
// builds the same keys, under the same names, as it builds from the clause
// written without the keyword. Read with the kind taken for the symbol,
// `CONSTRAINT UNIQUE KEY uq (b)` is a plain index `uq` and the file plans to
// make it one (stokaro/ptah#3730). Atlas CE v1.3.0 reports each row synced.
//
// The foreign keys written this way are rows of mysqlUnnamedForeignKeys, where
// the numbering they share with the other unnamed keys is asserted.
var mysqlKeysWithoutSymbol = []struct {
	name       string
	schema     string
	wantUnique string
}{
	{
		name:       "a primary key and two unique keys",
		schema:     "CREATE TABLE c (id int, a int, b int, CONSTRAINT PRIMARY KEY (id), CONSTRAINT UNIQUE (a), CONSTRAINT UNIQUE KEY uq (b));",
		wantUnique: "PRIMARY(id) a(a) uq(b)",
	},
	{
		name:       "a second unique key on one column",
		schema:     "CREATE TABLE c (id int PRIMARY KEY, a int, UNIQUE (a), CONSTRAINT UNIQUE (a));",
		wantUnique: "PRIMARY(id) a(a) a_2(a)",
	},
	{
		name: "a primary key ALTER TABLE adds",
		schema: `CREATE TABLE c (id int, a int);
ALTER TABLE c ADD CONSTRAINT PRIMARY KEY (id);`,
		wantUnique: "PRIMARY(id)",
	},
}

// uniqueKeysOfC answers the unique indexes of `c`, the primary key included, as
// `name(columns)` in the byte order of their names, which puts PRIMARY first.
func (s mysqlScratch) uniqueKeysOfC(c *qt.C, database string) string {
	c.Helper()
	var keys string
	err := s.admin.QueryRowContext(c.Context(), `
		SELECT COALESCE(GROUP_CONCAT(CONCAT(i.name, '(', i.cols, ')') ORDER BY CAST(i.name AS BINARY) SEPARATOR ' '), '')
		FROM (SELECT INDEX_NAME AS name, GROUP_CONCAT(COLUMN_NAME ORDER BY SEQ_IN_INDEX) AS cols
		      FROM information_schema.STATISTICS
		      WHERE TABLE_SCHEMA = ? AND TABLE_NAME = 'c' AND NON_UNIQUE = 0
		      GROUP BY INDEX_NAME) i`,
		database,
	).Scan(&keys)
	c.Assert(err, qt.IsNil)
	return keys
}

// TestMigrateDiffFindsAMySQLFileWithKeysWithoutSymbolSyncedE2E replays a
// directory whose one migration is the schema file itself and diffs: the
// directory is synced. Applied to an empty database, the directory builds the
// unique keys the row names, which is what the file is compared with.
func TestMigrateDiffFindsAMySQLFileWithKeysWithoutSymbolSyncedE2E(t *testing.T) {
	for _, test := range mysqlKeysWithoutSymbol {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			scratch := newMySQLScratch(c)
			dir, schema := writeRewrittenMigrationProject(c, test.schema, test.schema)
			_, dev := scratch.database(c, "nosym_dev")
			targetName, target := scratch.database(c, "nosym_target")

			plan, err := runCompatVerb("migrate", "diff", "--dir", "file://"+dir,
				"--to", "file://"+schema, "--dev-url", dev, "--dry-run")
			c.Assert(err, qt.IsNil, qt.Commentf("%s", plan))
			out, err := runCompatVerb("migrate", "apply", "--url", target, "--dir", "file://"+dir)
			c.Assert(err, qt.IsNil, qt.Commentf("%s", out))

			c.Assert(plan, qt.Contains, "The migration directory is synced with the desired state")
			c.Assert(scratch.uniqueKeysOfC(c, targetName), qt.Equals, test.wantUnique)
		})
	}
}

// TestSchemaApplyFindsAMySQLFileWithKeysWithoutSymbolSyncedE2E plans the native
// apply of the schema file against the database the same file built: nothing
// is planned.
func TestSchemaApplyFindsAMySQLFileWithKeysWithoutSymbolSyncedE2E(t *testing.T) {
	for _, test := range mysqlKeysWithoutSymbol {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			scratch := newMySQLScratch(c)
			_, schema := writeRewrittenMigrationProject(c, test.schema, test.schema)
			targetName, target := scratch.builtFrom(c, test.schema)

			plan := runPtahNative(c, "schema", "apply", "--db-url", target, "--schema-file", schema, "--dry-run")

			c.Assert(scratch.uniqueKeysOfC(c, targetName), qt.Equals, test.wantUnique)
			c.Assert(plan, qt.Contains, "Schema is synced")
		})
	}
}
