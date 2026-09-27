//go:build integration

package integration_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
)

// columnKeyUnderAnotherName are a migration whose key over x has a name the
// server does not give x's own UNIQUE, and a schema file that writes `x int
// UNIQUE`. Measured on MySQL 8.4.11 and 26.7.0, MariaDB 11.8.9 and 12.3.3 and
// PostgreSQL 18.6, Atlas CE v1.3.0 drops the key and adds the column's under
// the server's name: `x` on MySQL and MariaDB, `c_x_key` on PostgreSQL
// (stokaro/ptah#3723). Where another key the plan drops holds that name, it
// drops that key first. want are the statements each engine is planned.
var columnKeyUnderAnotherName = []struct {
	name      string
	engines   []uniqueEngine
	migration string
	schema    string
	want      map[string][]string
}{
	{
		name:      "a name of the author's",
		engines:   uniqueEngines,
		migration: `CREATE TABLE c (id int PRIMARY KEY, x int, CONSTRAINT c_x_uq UNIQUE (x));`,
		schema:    `CREATE TABLE c (id int PRIMARY KEY, x int UNIQUE);`,
		want: map[string][]string{
			"MySQL":      {"MODIFY COLUMN `x` int UNIQUE", "DROP INDEX `c_x_uq`"},
			"MariaDB":    {"MODIFY COLUMN `x` int UNIQUE", "DROP INDEX IF EXISTS `c_x_uq`"},
			"PostgreSQL": {`ADD CONSTRAINT "c_x_key" UNIQUE ("x")`, `DROP CONSTRAINT IF EXISTS "c_x_uq"`},
		},
	},
	{
		name:      "PostgreSQL's name on MySQL and MariaDB",
		engines:   mysqlFamilyUniqueEngines,
		migration: `CREATE TABLE c (id int PRIMARY KEY, x int, CONSTRAINT c_x_key UNIQUE (x));`,
		schema:    `CREATE TABLE c (id int PRIMARY KEY, x int UNIQUE);`,
		want: map[string][]string{
			"MySQL":   {"MODIFY COLUMN `x` int UNIQUE", "DROP INDEX `c_x_key`"},
			"MariaDB": {"MODIFY COLUMN `x` int UNIQUE", "DROP INDEX IF EXISTS `c_x_key`"},
		},
	},
	{
		name:      "MySQL's name on PostgreSQL",
		engines:   []uniqueEngine{postgresUniqueEngine},
		migration: `CREATE TABLE c (id int PRIMARY KEY, x int, CONSTRAINT x UNIQUE (x));`,
		schema:    `CREATE TABLE c (id int PRIMARY KEY, x int UNIQUE);`,
		want: map[string][]string{
			"PostgreSQL": {`ADD CONSTRAINT "c_x_key" UNIQUE ("x")`, `DROP CONSTRAINT IF EXISTS "x"`},
		},
	},
	{
		name:    "a unique index",
		engines: uniqueEngines,
		migration: `CREATE TABLE c (id int PRIMARY KEY, x int);
CREATE UNIQUE INDEX ux ON c (x);`,
		schema: `CREATE TABLE c (id int PRIMARY KEY, x int UNIQUE);`,
		want: map[string][]string{
			"MySQL":      {"MODIFY COLUMN `x` int UNIQUE", "DROP INDEX `ux`"},
			"MariaDB":    {"MODIFY COLUMN `x` int UNIQUE", "DROP INDEX IF EXISTS `ux`"},
			"PostgreSQL": {`ADD CONSTRAINT "c_x_key" UNIQUE ("x")`, `DROP INDEX IF EXISTS "ux"`},
		},
	},
	{
		name:      "a dropped UNIQUE over another column holds the column's name",
		engines:   mysqlFamilyUniqueEngines,
		migration: `CREATE TABLE c (id int PRIMARY KEY, x int, y int, CONSTRAINT x UNIQUE (y), CONSTRAINT c_x_uq UNIQUE (x));`,
		schema:    `CREATE TABLE c (id int PRIMARY KEY, x int UNIQUE, y int);`,
		want: map[string][]string{
			"MySQL":   {"DROP INDEX `x`", "MODIFY COLUMN `x` int UNIQUE", "DROP INDEX `c_x_uq`"},
			"MariaDB": {"DROP INDEX IF EXISTS `x`", "MODIFY COLUMN `x` int UNIQUE", "DROP INDEX IF EXISTS `c_x_uq`"},
		},
	},
	{
		name:      "a dropped index over another column holds the column's name",
		engines:   mysqlFamilyUniqueEngines,
		migration: `CREATE TABLE c (id int PRIMARY KEY, x int, y int, KEY x (y), CONSTRAINT c_x_uq UNIQUE (x));`,
		schema:    `CREATE TABLE c (id int PRIMARY KEY, x int UNIQUE, y int);`,
		want: map[string][]string{
			"MySQL":   {"DROP INDEX `x` ON `c`", "MODIFY COLUMN `x` int UNIQUE"},
			"MariaDB": {"DROP INDEX IF EXISTS `x` ON `c`", "MODIFY COLUMN `x` int UNIQUE"},
		},
	},
	{
		name:      "a dropped UNIQUE over another column holds the table's name for the column",
		engines:   []uniqueEngine{postgresUniqueEngine},
		migration: `CREATE TABLE c (id int PRIMARY KEY, x int, y int, CONSTRAINT c_x_uq UNIQUE (x), CONSTRAINT c_x_key UNIQUE (y));`,
		schema:    `CREATE TABLE c (id int PRIMARY KEY, x int UNIQUE, y int);`,
		want: map[string][]string{
			"PostgreSQL": {`DROP CONSTRAINT IF EXISTS "c_x_key"`, `ADD CONSTRAINT "c_x_key" UNIQUE ("x")`},
		},
	},
	{
		name:      "a column added beside a dropped UNIQUE that holds its name",
		engines:   mysqlFamilyUniqueEngines,
		migration: `CREATE TABLE c (id int PRIMARY KEY, y int, CONSTRAINT x UNIQUE (y));`,
		schema:    `CREATE TABLE c (id int PRIMARY KEY, y int, x int UNIQUE);`,
		want: map[string][]string{
			"MySQL":   {"DROP INDEX `x`", "ADD COLUMN `x` int UNIQUE"},
			"MariaDB": {"DROP INDEX IF EXISTS `x`", "ADD COLUMN `x` int UNIQUE"},
		},
	},
	{
		name:      "a column added beside a dropped UNIQUE that holds the table's name for it",
		engines:   []uniqueEngine{postgresUniqueEngine},
		migration: `CREATE TABLE c (id int PRIMARY KEY, y int, CONSTRAINT c_x_key UNIQUE (y));`,
		schema:    `CREATE TABLE c (id int PRIMARY KEY, y int, x int UNIQUE);`,
		want: map[string][]string{
			"PostgreSQL": {`DROP CONSTRAINT IF EXISTS "c_x_key"`, `ADD COLUMN "x" int UNIQUE`},
		},
	},
}

// TestMigrateDiffRenamesAColumnKeyUnderAnotherNameE2E plans the directory
// against the schema file, writes the plan into the directory, applies the
// directory to an empty database and diffs again: the directory the plan
// completes is synced, so the column's key took the name the server gives it.
func TestMigrateDiffRenamesAColumnKeyUnderAnotherNameE2E(t *testing.T) {
	for _, test := range columnKeyUnderAnotherName {
		for _, engine := range test.engines {
			t.Run(engine.name+"/"+test.name, func(t *testing.T) {
				c := qt.New(t)
				dir, schema := writeRewrittenMigrationProject(c, test.migration, test.schema)

				plan, err := runCompatVerb("migrate", "diff", "--dir", "file://"+dir,
					"--to", "file://"+schema, "--dev-url", engine.emptyDB(c), "--dry-run")
				c.Assert(err, qt.IsNil, qt.Commentf("%s", plan))
				written, err := runCompatVerb("migrate", "diff", "--dir", "file://"+dir,
					"--to", "file://"+schema, "--dev-url", engine.emptyDB(c))
				c.Assert(err, qt.IsNil, qt.Commentf("%s", written))
				out, err := runCompatVerb("migrate", "apply", "--url", engine.emptyDB(c), "--dir", "file://"+dir)
				c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
				again, err := runCompatVerb("migrate", "diff", "--dir", "file://"+dir,
					"--to", "file://"+schema, "--dev-url", engine.emptyDB(c), "--dry-run")
				c.Assert(err, qt.IsNil, qt.Commentf("%s", again))

				for _, statement := range test.want[engine.name] {
					c.Assert(plan, qt.Contains, statement)
				}
				c.Assert(again, qt.Contains, "The migration directory is synced with the desired state")
			})
		}
	}
}

// TestSchemaApplyRenamesAColumnKeyUnderAnotherNameE2E applies the schema file
// natively to the database the migration built, and plans it again: nothing
// is left to do.
func TestSchemaApplyRenamesAColumnKeyUnderAnotherNameE2E(t *testing.T) {
	for _, test := range columnKeyUnderAnotherName {
		for _, engine := range test.engines {
			t.Run(engine.name+"/"+test.name, func(t *testing.T) {
				c := qt.New(t)
				_, schema := writeRewrittenMigrationProject(c, test.migration, test.schema)
				target := engine.builtDB(c, test.migration)

				applied := runPtahNative(c, "schema", "apply", "--db-url", target, "--schema-file", schema, "--auto-approve")
				again := runPtahNative(c, "schema", "apply", "--db-url", target, "--schema-file", schema, "--dry-run")

				for _, statement := range test.want[engine.name] {
					c.Assert(applied, qt.Contains, statement)
				}
				c.Assert(again, qt.Contains, "Schema is synced")
			})
		}
	}
}

// columnKeyUnderTheServersName are a migration whose key over x has the name
// the server gives x's own UNIQUE, and a schema file that writes `x int
// UNIQUE`, or a schema file compared with the database it builds itself.
// Atlas CE v1.3.0 reports each synced, measured on the servers above, except
// `X` on MySQL and MariaDB: Atlas CE renames it to `x`, and Ptah keeps it,
// because the server compares index names without case.
var columnKeyUnderTheServersName = []struct {
	name      string
	engines   []uniqueEngine
	migration string
	schema    string
}{
	{
		name:      "MySQL's name",
		engines:   mysqlFamilyUniqueEngines,
		migration: `CREATE TABLE c (id int PRIMARY KEY, x int, CONSTRAINT x UNIQUE (x));`,
		schema:    `CREATE TABLE c (id int PRIMARY KEY, x int UNIQUE);`,
	},
	{
		name:      "MySQL's name in another case",
		engines:   mysqlFamilyUniqueEngines,
		migration: `CREATE TABLE c (id int PRIMARY KEY, x int, CONSTRAINT X UNIQUE (x));`,
		schema:    `CREATE TABLE c (id int PRIMARY KEY, x int UNIQUE);`,
	},
	{
		name:      "PostgreSQL's name",
		engines:   []uniqueEngine{postgresUniqueEngine},
		migration: `CREATE TABLE c (id int PRIMARY KEY, x int, CONSTRAINT c_x_key UNIQUE (x));`,
		schema:    `CREATE TABLE c (id int PRIMARY KEY, x int UNIQUE);`,
	},
	{
		name:    "the next name where an index holds the first",
		engines: uniqueEngines,
		migration: `CREATE TABLE c (id int PRIMARY KEY, y int);
CREATE UNIQUE INDEX x ON c (y);
CREATE UNIQUE INDEX c_x_key ON c (y);
ALTER TABLE c ADD COLUMN x int UNIQUE;`,
		schema: `CREATE TABLE c (id int PRIMARY KEY, y int);
CREATE UNIQUE INDEX x ON c (y);
CREATE UNIQUE INDEX c_x_key ON c (y);
ALTER TABLE c ADD COLUMN x int UNIQUE;`,
	},
	{
		name:    "a UNIQUE added beside the column's own",
		engines: uniqueEngines,
		migration: `CREATE TABLE c (id int PRIMARY KEY, a int);
ALTER TABLE c ADD COLUMN b int UNIQUE, ADD UNIQUE (b);`,
		schema: `CREATE TABLE c (id int PRIMARY KEY, a int);
ALTER TABLE c ADD COLUMN b int UNIQUE, ADD UNIQUE (b);`,
	},
}

// TestMigrateDiffFindsAColumnKeyUnderTheServersNameSyncedE2E replays the
// directory and diffs it against the schema file.
func TestMigrateDiffFindsAColumnKeyUnderTheServersNameSyncedE2E(t *testing.T) {
	for _, test := range columnKeyUnderTheServersName {
		for _, engine := range test.engines {
			t.Run(engine.name+"/"+test.name, func(t *testing.T) {
				c := qt.New(t)
				dir, schema := writeRewrittenMigrationProject(c, test.migration, test.schema)

				plan, err := runCompatVerb("migrate", "diff", "--dir", "file://"+dir,
					"--to", "file://"+schema, "--dev-url", engine.emptyDB(c), "--dry-run")

				c.Assert(err, qt.IsNil, qt.Commentf("%s", plan))
				c.Assert(plan, qt.Contains, "The migration directory is synced with the desired state")
			})
		}
	}
}

// TestSchemaApplyFindsAColumnKeyUnderTheServersNameSyncedE2E plans the native
// apply of the schema file against the database the migration built.
func TestSchemaApplyFindsAColumnKeyUnderTheServersNameSyncedE2E(t *testing.T) {
	for _, test := range columnKeyUnderTheServersName {
		for _, engine := range test.engines {
			t.Run(engine.name+"/"+test.name, func(t *testing.T) {
				c := qt.New(t)
				_, schema := writeRewrittenMigrationProject(c, test.migration, test.schema)
				target := engine.builtDB(c, test.migration)

				plan := runPtahNative(c, "schema", "apply", "--db-url", target, "--schema-file", schema, "--dry-run")

				c.Assert(plan, qt.Contains, "Schema is synced")
			})
		}
	}
}
