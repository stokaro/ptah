//go:build integration

package integration_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/dbtarget"
)

// uniqueEngine is one server the rows below run on: how to get an empty
// database there, and a database built from SQL with nothing of Ptah's in
// between.
type uniqueEngine struct {
	name    string
	emptyDB func(c *qt.C) string
	builtDB func(c *qt.C, sql string) string
}

// uniqueEngines are MySQL, MariaDB and PostgreSQL, which share the comparison
// these rows exercise.
var uniqueEngines = []uniqueEngine{
	{
		name:    "MySQL",
		emptyDB: func(c *qt.C) string { return mysqlFamilyEmptyDatabase(c, dbtarget.MySQLAdmin) },
		builtDB: func(c *qt.C, sql string) string { return mysqlFamilyBuiltDatabase(c, dbtarget.MySQLAdmin, sql) },
	},
	{
		name:    "MariaDB",
		emptyDB: func(c *qt.C) string { return mysqlFamilyEmptyDatabase(c, dbtarget.MariaDBAdmin) },
		builtDB: func(c *qt.C, sql string) string { return mysqlFamilyBuiltDatabase(c, dbtarget.MariaDBAdmin, sql) },
	},
	{
		name: "PostgreSQL",
		emptyDB: func(c *qt.C) string {
			url, _ := scratchReplayDatabase(c)
			return url
		},
		builtDB: databaseBuiltFrom,
	},
}

func mysqlFamilyEmptyDatabase(c *qt.C, engine dbtarget.Engine) string {
	c.Helper()
	_, url := newMySQLFamilyScratch(c, engine).database(c, "uq")
	return url
}

func mysqlFamilyBuiltDatabase(c *qt.C, engine dbtarget.Engine, sql string) string {
	c.Helper()
	_, url := newMySQLFamilyScratch(c, engine).builtFrom(c, sql)
	return url
}

// columnUniqueBesideAnotherUnique are schema files with a column-level UNIQUE
// and another UNIQUE over the same column. Measured on MySQL 8.4.11 and
// 26.7.0, MariaDB 11.8.9 and 12.3.3 and PostgreSQL 18, each builds two keys,
// and Atlas CE v1.3.0 reports the file synced with the database it built.
// Read as the column's own, the second key is planned as an ADD on every run
// (stokaro/ptah#3764).
var columnUniqueBesideAnotherUnique = []struct {
	name string
	sql  string
}{
	{name: "a key led by the column", sql: `CREATE TABLE c (id int PRIMARY KEY, a int UNIQUE, b int, UNIQUE (a, b));`},
	{name: "a named key led by the column", sql: `CREATE TABLE c (id int PRIMARY KEY, a int UNIQUE, b int, CONSTRAINT uq_ab UNIQUE (a, b));`},
	{name: "a named key over the column alone", sql: `CREATE TABLE c (id int PRIMARY KEY, a int UNIQUE, CONSTRAINT uq_a UNIQUE (a));`},
	{name: "ALTER TABLE adds both", sql: `CREATE TABLE c (id int PRIMARY KEY, a int);
ALTER TABLE c ADD COLUMN b int UNIQUE;
ALTER TABLE c ADD CONSTRAINT uq_b UNIQUE (b);`},
}

// TestMigrateDiffFindsAColumnUniqueBesideAnotherUniqueSyncedE2E replays a
// directory whose one migration is the schema file itself and diffs.
func TestMigrateDiffFindsAColumnUniqueBesideAnotherUniqueSyncedE2E(t *testing.T) {
	for _, engine := range uniqueEngines {
		for _, test := range columnUniqueBesideAnotherUnique {
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

// TestSchemaApplyFindsAColumnUniqueBesideAnotherUniqueSyncedE2E plans the
// native apply of the file against the database the file built.
func TestSchemaApplyFindsAColumnUniqueBesideAnotherUniqueSyncedE2E(t *testing.T) {
	for _, engine := range uniqueEngines {
		for _, test := range columnUniqueBesideAnotherUnique {
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

// uniqueTheFileRemoves are a migration and a schema file that keeps the
// column's UNIQUE and removes another key over the column. Atlas CE v1.3.0
// plans each drop, measured on the servers above; read as the column's own,
// the key is never dropped. want is the statement each engine is planned.
var uniqueTheFileRemoves = []struct {
	name      string
	migration string
	want      map[string]string
}{
	{
		name: "a second key over the column alone",
		migration: `CREATE TABLE c (id int PRIMARY KEY, a int UNIQUE);
ALTER TABLE c ADD CONSTRAINT uq_a UNIQUE (a);`,
		want: map[string]string{
			"MySQL": "DROP INDEX `uq_a`", "MariaDB": "DROP INDEX IF EXISTS `uq_a`",
			"PostgreSQL": `DROP CONSTRAINT IF EXISTS "uq_a"`,
		},
	},
	{
		name:      "a key led by the column, where the column has no key of its own",
		migration: `CREATE TABLE c (id int PRIMARY KEY, a int, b int, CONSTRAINT uq_ab UNIQUE (a, b));`,
		want: map[string]string{
			"MySQL": "DROP INDEX `uq_ab`", "MariaDB": "DROP INDEX IF EXISTS `uq_ab`",
			"PostgreSQL": `DROP CONSTRAINT IF EXISTS "uq_ab"`,
		},
	},
	{
		name:      "a key led by the column",
		migration: `CREATE TABLE c (id int PRIMARY KEY, a int UNIQUE, b int, CONSTRAINT uq_ab UNIQUE (a, b));`,
		want: map[string]string{
			"MySQL": "DROP INDEX `uq_ab`", "MariaDB": "DROP INDEX IF EXISTS `uq_ab`",
			"PostgreSQL": `DROP CONSTRAINT IF EXISTS "uq_ab"`,
		},
	},
}

// TestMigrateDiffDropsAUniqueBesideAColumnUniqueE2E plans the directory
// against a schema file that keeps only `a int UNIQUE`.
func TestMigrateDiffDropsAUniqueBesideAColumnUniqueE2E(t *testing.T) {
	const schema = `CREATE TABLE c (id int PRIMARY KEY, a int UNIQUE, b int);`
	for _, engine := range uniqueEngines {
		for _, test := range uniqueTheFileRemoves {
			t.Run(engine.name+"/"+test.name, func(t *testing.T) {
				c := qt.New(t)
				dir, schemaPath := writeRewrittenMigrationProject(c, test.migration, schema)

				plan, err := runCompatVerb("migrate", "diff", "--dir", "file://"+dir,
					"--to", "file://"+schemaPath, "--dev-url", engine.emptyDB(c), "--dry-run")

				c.Assert(err, qt.IsNil, qt.Commentf("%s", plan))
				c.Assert(plan, qt.Contains, test.want[engine.name])
			})
		}
	}
}
