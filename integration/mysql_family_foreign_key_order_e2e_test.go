//go:build integration

package integration_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/dbtarget"
)

// mysqlFamilyForeignKeyOrder are schema files whose unnamed foreign keys the
// server numbers, with the number it gave each key of the row's table.
//
// The server numbers a table's unnamed keys in the order the body writes them,
// a column's `REFERENCES` among the table-level keys (stokaro/ptah#3765), and
// keeps a derived name only up to a length (stokaro/ptah#3762). A file whose
// model numbers or names a key otherwise plans to drop and add it against the
// database it built. Every row was measured on MySQL 8.4.11 and 26.7.0 and
// MariaDB 11.8.9 and 12.3.3, where it runs, and Atlas CE v1.3.0 reports each
// synced. A column's `REFERENCES` runs on MariaDB only: MySQL 8.4 builds
// nothing from it, and Ptah refuses it under `--dialect mysql`.
var mysqlFamilyForeignKeyOrder = []struct {
	name     string
	engines  []dbtarget.Engine
	schema   string
	table    string
	wantKeys string
}{
	{
		name:    "a table's key written before a column's",
		engines: mariaDBOnly,
		schema: `CREATE TABLE p (id int PRIMARY KEY);
CREATE TABLE c (id int PRIMARY KEY, FOREIGN KEY (b) REFERENCES p(id), a int REFERENCES p(id), b int);`,
		table:    "c",
		wantKeys: "1(b) 2(a)",
	},
	{
		name:    "a table's key between two columns' keys",
		engines: mariaDBOnly,
		schema: `CREATE TABLE p (id int PRIMARY KEY);
CREATE TABLE c (id int PRIMARY KEY, a int REFERENCES p(id), b int, FOREIGN KEY (b) REFERENCES p(id), d int REFERENCES p(id));`,
		table:    "c",
		wantKeys: "1(a) 2(b) 3(d)",
	},
	{
		name:    "a named key between them does not move the count",
		engines: mariaDBOnly,
		schema: `CREATE TABLE p (id int PRIMARY KEY);
CREATE TABLE c (id int PRIMARY KEY, b int, FOREIGN KEY (b) REFERENCES p(id), CONSTRAINT fx FOREIGN KEY (b) REFERENCES p(id),
  a int REFERENCES p(id), d int, FOREIGN KEY (d) REFERENCES p(id));`,
		table:    "c",
		wantKeys: "1(b) 2(a) 3(d) fx(b)",
	},
	{
		name:    "a 63-character name in CREATE TABLE",
		engines: bothMySQLEngines,
		schema: "CREATE TABLE p (id int PRIMARY KEY);\n" +
			"CREATE TABLE " + strings.Repeat("t", 55) + " (id int PRIMARY KEY, x1 int, x2 int, x3 int, x4 int, " +
			"x5 int, x6 int, x7 int, x8 int, x9 int, x10 int, FOREIGN KEY (x1) REFERENCES p(id), " +
			"FOREIGN KEY (x2) REFERENCES p(id), FOREIGN KEY (x3) REFERENCES p(id), FOREIGN KEY (x4) REFERENCES p(id), " +
			"FOREIGN KEY (x5) REFERENCES p(id), FOREIGN KEY (x6) REFERENCES p(id), FOREIGN KEY (x7) REFERENCES p(id), " +
			"FOREIGN KEY (x8) REFERENCES p(id), FOREIGN KEY (x9) REFERENCES p(id), FOREIGN KEY (x10) REFERENCES p(id));",
		table:    strings.Repeat("t", 55),
		wantKeys: "1(x1) 10(x10) 2(x2) 3(x3) 4(x4) 5(x5) 6(x6) 7(x7) 8(x8) 9(x9)",
	},
	{
		name:    "a 64-character name in ALTER TABLE",
		engines: bothMySQLEngines,
		schema: "CREATE TABLE p (id int PRIMARY KEY);\n" +
			"CREATE TABLE " + strings.Repeat("t", 57) + " (id int PRIMARY KEY, a int);\n" +
			"ALTER TABLE " + strings.Repeat("t", 57) + " ADD FOREIGN KEY (a) REFERENCES p(id);",
		table:    strings.Repeat("t", 57),
		wantKeys: "1(a)",
	},
}

// foreignKeyNumbersOf answers the foreign keys of table as `n(columns)`, in
// name order, where n is what follows the last underscore of the key's name.
// That is the number the server gave an unnamed key on every line: MariaDB
// 12.1 and later write `<n>` where earlier lines write `<table>_ibfk_<n>`.
func (s mysqlScratch) foreignKeyNumbersOf(c *qt.C, database, table string) string {
	c.Helper()
	var keys string
	err := s.admin.QueryRowContext(c.Context(), `
		SELECT COALESCE(GROUP_CONCAT(CONCAT(k.n, '(', k.cols, ')') ORDER BY k.n SEPARATOR ' '), '')
		FROM (SELECT SUBSTRING_INDEX(CONSTRAINT_NAME, '_', -1) AS n,
		             GROUP_CONCAT(COLUMN_NAME ORDER BY ORDINAL_POSITION) AS cols
		      FROM information_schema.KEY_COLUMN_USAGE
		      WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND REFERENCED_TABLE_NAME IS NOT NULL
		      GROUP BY CONSTRAINT_NAME) k`,
		database, table,
	).Scan(&keys)
	c.Assert(err, qt.IsNil)
	return keys
}

// TestMigrateDiffFindsAMySQLFamilyForeignKeyOrderSyncedE2E replays a directory
// whose one migration is the schema file itself and diffs: the directory is
// synced. Applied to an empty database, the directory builds the keys the row
// names, which is what the file is compared with.
func TestMigrateDiffFindsAMySQLFamilyForeignKeyOrderSyncedE2E(t *testing.T) {
	for _, test := range mysqlFamilyForeignKeyOrder {
		for _, engine := range test.engines {
			t.Run(engine.String()+"/"+test.name, func(t *testing.T) {
				c := qt.New(t)
				scratch := newMySQLFamilyScratch(c, engine)
				dir, schema := writeRewrittenMigrationProject(c, test.schema, test.schema)
				_, dev := scratch.database(c, "fkorder_dev")
				targetName, target := scratch.database(c, "fkorder_target")

				plan, err := runCompatVerb("migrate", "diff", "--dir", "file://"+dir,
					"--to", "file://"+schema, "--dev-url", dev, "--dry-run")
				c.Assert(err, qt.IsNil, qt.Commentf("%s", plan))
				out, err := runCompatVerb("migrate", "apply", "--url", target, "--dir", "file://"+dir)
				c.Assert(err, qt.IsNil, qt.Commentf("%s", out))

				c.Assert(plan, qt.Contains, "The migration directory is synced with the desired state")
				c.Assert(scratch.foreignKeyNumbersOf(c, targetName, test.table), qt.Equals, test.wantKeys)
			})
		}
	}
}

// TestSchemaApplyFindsAMySQLFamilyForeignKeyOrderSyncedE2E plans the native
// apply of the schema file against the database the same file built, with
// nothing of Ptah's in between: nothing is planned.
func TestSchemaApplyFindsAMySQLFamilyForeignKeyOrderSyncedE2E(t *testing.T) {
	for _, test := range mysqlFamilyForeignKeyOrder {
		for _, engine := range test.engines {
			t.Run(engine.String()+"/"+test.name, func(t *testing.T) {
				c := qt.New(t)
				scratch := newMySQLFamilyScratch(c, engine)
				_, schema := writeRewrittenMigrationProject(c, test.schema, test.schema)
				targetName, target := scratch.builtFrom(c, test.schema)

				plan := runPtahNative(c, "schema", "apply", "--db-url", target, "--schema-file", schema, "--dry-run")

				c.Assert(scratch.foreignKeyNumbersOf(c, targetName, test.table), qt.Equals, test.wantKeys)
				c.Assert(plan, qt.Contains, "Schema is synced")
			})
		}
	}
}
