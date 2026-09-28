//go:build integration

package integration_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
)

// namedNotNulls are PostgreSQL schema files that name a column's NOT NULL,
// with the column line `ptah db read` prints for it. PostgreSQL 18 keeps the
// name as a NOT NULL constraint in pg_constraint, which information_schema
// lists as a CHECK. Read as a CHECK the file does not declare, the comparison
// planned `DROP CONSTRAINT <name>` against the database the file built, and
// applying the plan dropped the NOT NULL (stokaro/ptah#3927). PostgreSQL 17
// accepts the name and keeps none, so the name in want is optional: the
// assertion is that the column is still NOT NULL after the apply.
var namedNotNulls = []struct {
	name string
	sql  string
	want string
}{
	{
		name: "a named NOT NULL",
		sql:  `CREATE TABLE c (id int PRIMARY KEY, d int CONSTRAINT nn NOT NULL);`,
		want: `"d" integer (CONSTRAINT "nn" )?NOT NULL`,
	},
	{
		name: "two named NOT NULLs",
		sql:  `CREATE TABLE c (id int PRIMARY KEY, d int CONSTRAINT nn NOT NULL, e text CONSTRAINT nn2 NOT NULL);`,
		want: `"e" text (CONSTRAINT "nn2" )?NOT NULL`,
	},
	{
		name: "a named NOT NULL beside a CHECK",
		sql:  `CREATE TABLE c (id int PRIMARY KEY, d int CONSTRAINT nn NOT NULL CONSTRAINT ck CHECK (d > 0));`,
		want: `"d" integer (CONSTRAINT "nn" )?NOT NULL`,
	},
	{
		name: "a named NOT NULL on the primary key column",
		sql:  `CREATE TABLE c (id int CONSTRAINT idnn NOT NULL PRIMARY KEY, d int);`,
		want: `"id" integer PRIMARY KEY (CONSTRAINT "idnn" )?NOT NULL`,
	},
	{
		name: "a named NOT NULL on a column ALTER TABLE adds",
		sql: `CREATE TABLE c (id int PRIMARY KEY);
ALTER TABLE c ADD COLUMN d int CONSTRAINT nn NOT NULL;`,
		want: `"d" integer (CONSTRAINT "nn" )?NOT NULL`,
	},
}

// TestSchemaApplyKeepsANamedNotNullE2E applies the file natively to the
// database it built, then reads the column back.
func TestSchemaApplyKeepsANamedNotNullE2E(t *testing.T) {
	for _, test := range namedNotNulls {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			_, schema := writeRewrittenMigrationProject(c, test.sql, test.sql)
			target := postgresUniqueEngine.builtDB(c, test.sql)

			applied := runPtahNative(c, "schema", "apply", "--db-url", target, "--schema-file", schema, "--auto-approve")

			c.Assert(applied, qt.Not(qt.Contains), "DROP CONSTRAINT")
			c.Assert(runPtahNative(c, "db", "read", "--db-url", target), qt.Matches, `(?s).*`+test.want+`.*`)
		})
	}
}

// TestCompatSchemaApplyKeepsANamedNotNullE2E is the same apply through
// ptah-compat, which reads the file on an empty dev database.
func TestCompatSchemaApplyKeepsANamedNotNullE2E(t *testing.T) {
	for _, test := range namedNotNulls {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			_, schema := writeRewrittenMigrationProject(c, test.sql, test.sql)
			target := postgresUniqueEngine.builtDB(c, test.sql)

			applied, err := runCompatVerb("schema", "apply", "-u", target, "--to", "file://"+schema,
				"--dev-url", postgresUniqueEngine.emptyDB(c), "--auto-approve")

			c.Assert(err, qt.IsNil, qt.Commentf("%s", applied))
			c.Assert(applied, qt.Not(qt.Contains), "DROP CONSTRAINT")
			c.Assert(runPtahNative(c, "db", "read", "--db-url", target), qt.Matches, `(?s).*`+test.want+`.*`)
		})
	}
}
