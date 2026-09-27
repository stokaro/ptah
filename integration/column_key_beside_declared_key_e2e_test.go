//go:build integration

package integration_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
)

// mysqlFamilyUniqueEngines and postgresUniqueEngine are the entries of
// [uniqueEngines], split by how a column's UNIQUE beside a named UNIQUE
// builds.
var (
	mysqlFamilyUniqueEngines = uniqueEngines[:2]
	postgresUniqueEngine     = uniqueEngines[2]
)

// columnKeyBesideDeclaredKey are a migration that builds a declared UNIQUE and
// a schema file that also declares the column's own. Measured on MySQL 8.4.11
// and 26.7.0 and MariaDB 11.8.9 and 12.3.3, the file builds two keys, and
// Atlas CE v1.3.0 plans `ADD UNIQUE INDEX a (a)` against the database the
// migration built (stokaro/ptah#3784). PostgreSQL 18.6 builds two keys where
// the declared one is a unique index or comes from a later statement, and
// Atlas CE plans `ADD CONSTRAINT c_a_key` there (stokaro/ptah#3812); one
// CREATE TABLE that declares both builds the named key alone, which
// [TestMigrateDiffFindsAColumnUniqueBesideANamedUniqueSyncedOnPostgresE2E]
// covers. want is the statement each engine is planned.
var columnKeyBesideDeclaredKey = []struct {
	name      string
	engines   []uniqueEngine
	migration string
	schema    string
	want      map[string]string
}{
	{
		name:      "a named UNIQUE",
		engines:   mysqlFamilyUniqueEngines,
		migration: `CREATE TABLE c (id int PRIMARY KEY, a int, CONSTRAINT uq_a UNIQUE (a));`,
		schema:    `CREATE TABLE c (id int PRIMARY KEY, a int UNIQUE, CONSTRAINT uq_a UNIQUE (a));`,
		want:      map[string]string{"MySQL": "MODIFY COLUMN `a` int UNIQUE", "MariaDB": "MODIFY COLUMN `a` int UNIQUE"},
	},
	{
		name:    "a unique index",
		engines: uniqueEngines,
		migration: `CREATE TABLE c (id int PRIMARY KEY, a int);
CREATE UNIQUE INDEX ux ON c (a);`,
		schema: `CREATE TABLE c (id int PRIMARY KEY, a int UNIQUE);
CREATE UNIQUE INDEX ux ON c (a);`,
		want: map[string]string{
			"MySQL": "MODIFY COLUMN `a` int UNIQUE", "MariaDB": "MODIFY COLUMN `a` int UNIQUE",
			"PostgreSQL": `ADD CONSTRAINT "c_a_key" UNIQUE ("a")`,
		},
	},
	{
		name:    "a named UNIQUE a later statement adds",
		engines: uniqueEngines,
		migration: `CREATE TABLE c (id int PRIMARY KEY, a int);
ALTER TABLE c ADD CONSTRAINT uq_a UNIQUE (a);`,
		schema: `CREATE TABLE c (id int PRIMARY KEY, a int UNIQUE);
ALTER TABLE c ADD CONSTRAINT uq_a UNIQUE (a);`,
		want: map[string]string{
			"MySQL": "MODIFY COLUMN `a` int UNIQUE", "MariaDB": "MODIFY COLUMN `a` int UNIQUE",
			"PostgreSQL": `ADD CONSTRAINT "c_a_key" UNIQUE ("a")`,
		},
	},
	{
		name:    "two columns, each beside a named UNIQUE a later statement adds",
		engines: []uniqueEngine{postgresUniqueEngine},
		migration: `CREATE TABLE c (id int PRIMARY KEY, a int, b int);
ALTER TABLE c ADD CONSTRAINT uq_a UNIQUE (a), ADD CONSTRAINT uq_b UNIQUE (b);`,
		schema: `CREATE TABLE c (id int PRIMARY KEY, a int UNIQUE, b int UNIQUE);
ALTER TABLE c ADD CONSTRAINT uq_a UNIQUE (a), ADD CONSTRAINT uq_b UNIQUE (b);`,
		want: map[string]string{"PostgreSQL": `ADD CONSTRAINT "c_b_key" UNIQUE ("b")`},
	},
	{
		name:      "two columns, each beside a named UNIQUE",
		engines:   mysqlFamilyUniqueEngines,
		migration: `CREATE TABLE c (id int PRIMARY KEY, a int, b int, CONSTRAINT uq_a UNIQUE (a), CONSTRAINT uq_b UNIQUE (b));`,
		schema: `CREATE TABLE c (id int PRIMARY KEY, a int UNIQUE, b int UNIQUE,
  CONSTRAINT uq_a UNIQUE (a), CONSTRAINT uq_b UNIQUE (b));`,
		want: map[string]string{"MySQL": "MODIFY COLUMN `b` int UNIQUE", "MariaDB": "MODIFY COLUMN `b` int UNIQUE"},
	},
}

// TestMigrateDiffAddsAColumnKeyBesideADeclaredKeyE2E plans the directory
// against the schema file, writes the plan into the directory, applies the
// directory to an empty database and diffs again: the directory the plan
// completes is synced.
func TestMigrateDiffAddsAColumnKeyBesideADeclaredKeyE2E(t *testing.T) {
	for _, test := range columnKeyBesideDeclaredKey {
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

				c.Assert(plan, qt.Contains, test.want[engine.name])
				c.Assert(again, qt.Contains, "The migration directory is synced with the desired state")
			})
		}
	}
}

// TestSchemaApplyAddsAColumnKeyBesideADeclaredKeyE2E applies the schema file
// natively to the database the migration built, and plans it again: nothing
// is left to do.
func TestSchemaApplyAddsAColumnKeyBesideADeclaredKeyE2E(t *testing.T) {
	for _, test := range columnKeyBesideDeclaredKey {
		for _, engine := range test.engines {
			t.Run(engine.name+"/"+test.name, func(t *testing.T) {
				c := qt.New(t)
				_, schema := writeRewrittenMigrationProject(c, test.migration, test.schema)
				target := engine.builtDB(c, test.migration)

				applied := runPtahNative(c, "schema", "apply", "--db-url", target, "--schema-file", schema, "--auto-approve")
				again := runPtahNative(c, "schema", "apply", "--db-url", target, "--schema-file", schema, "--dry-run")

				c.Assert(applied, qt.Contains, test.want[engine.name])
				c.Assert(again, qt.Contains, "Schema is synced")
			})
		}
	}
}

// TestMigrateDiffFindsAColumnUniqueBesideANamedUniqueSyncedOnPostgresE2E is
// the PostgreSQL side of the first row: `CREATE TABLE c (a int UNIQUE,
// CONSTRAINT uq_a UNIQUE (a))` builds `uq_a` alone on PostgreSQL 18, so the
// database the migration built already holds the file's keys, as Atlas CE
// v1.3.0 reports.
func TestMigrateDiffFindsAColumnUniqueBesideANamedUniqueSyncedOnPostgresE2E(t *testing.T) {
	c := qt.New(t)
	dir, schema := writeRewrittenMigrationProject(c,
		`CREATE TABLE c (id int PRIMARY KEY, a int, CONSTRAINT uq_a UNIQUE (a));`,
		`CREATE TABLE c (id int PRIMARY KEY, a int UNIQUE, CONSTRAINT uq_a UNIQUE (a));`)

	plan, err := runCompatVerb("migrate", "diff", "--dir", "file://"+dir,
		"--to", "file://"+schema, "--dev-url", postgresUniqueEngine.emptyDB(c), "--dry-run")

	c.Assert(err, qt.IsNil, qt.Commentf("%s", plan))
	c.Assert(plan, qt.Contains, "The migration directory is synced with the desired state")
}
