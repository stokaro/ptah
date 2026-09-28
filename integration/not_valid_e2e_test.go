//go:build integration

package integration_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
)

// notValidEngine is one server of the PostgreSQL family the rows below run
// on: how to get a scratch database there with statements run on it, one at a
// time, and with nothing of Ptah's in between.
type notValidEngine struct {
	name  string
	built func(c *qt.C, statements []string) string
}

// notValidEngines are the servers that take NOT VALID and VALIDATE
// CONSTRAINT. Measured on PostgreSQL 18.6, CockroachDB v26.3.2 and YugabyteDB
// 2026.1.2, each records a constraint added NOT VALID as unvalidated and prints
// the clause in pg_get_constraintdef (stokaro/ptah#3853).
var notValidEngines = []notValidEngine{
	{name: "PostgreSQL", built: func(c *qt.C, statements []string) string {
		return databaseBuiltFrom(c, strings.Join(statements, ";\n"))
	}},
	{name: "CockroachDB", built: func(c *qt.C, statements []string) string {
		return devDialectDatabaseBuiltFrom(c, cockroachDevDatabase(c), statements)
	}},
	{name: "YugabyteDB", built: func(c *qt.C, statements []string) string {
		return devDialectDatabaseBuiltFrom(c, yugabyteDevDatabase(c), statements)
	}},
}

// devDialectDatabaseBuiltFrom runs statements on the scratch database one at a
// time, because CockroachDB refuses a schema change in a batch that wrote a
// row, and returns its URL.
func devDialectDatabaseBuiltFrom(c *qt.C, dev devDialectDatabase, statements []string) string {
	c.Helper()
	for _, statement := range statements {
		_, err := dev.conn.ExecContext(c.Context(), statement)
		c.Assert(err, qt.IsNil, qt.Commentf("%s", statement))
	}
	return dev.url
}

// notValidTables are the tables the rows below declare, before any
// constraint. The columns are bigint because CockroachDB v26.3.2 builds `int`
// as INT8, which a declared `int` does not compare equal to
// (stokaro/ptah#3922).
var notValidTables = []string{
	`CREATE TABLE np (id bigint PRIMARY KEY)`,
	`CREATE TABLE nc (id bigint PRIMARY KEY, n bigint, p bigint)`,
}

// notValidAdded adds a CHECK and a foreign key to nc NOT VALID, and
// validatedAdded adds them validated.
var (
	notValidAdded = []string{
		`ALTER TABLE nc ADD CONSTRAINT nc_n_positive CHECK (n > 0) NOT VALID`,
		`ALTER TABLE nc ADD CONSTRAINT nc_p_fkey FOREIGN KEY (p) REFERENCES np (id) NOT VALID`,
	}
	validatedAdded = []string{
		`ALTER TABLE nc ADD CONSTRAINT nc_n_positive CHECK (n > 0)`,
		`ALTER TABLE nc ADD CONSTRAINT nc_p_fkey FOREIGN KEY (p) REFERENCES np (id)`,
	}
)

// notValidReadBack describes each CHECK and foreign key of nc the way the
// server prints it.
const notValidReadBack = `SELECT conname || ' ' || pg_get_constraintdef(oid) FROM pg_constraint
WHERE conrelid = 'nc'::regclass AND contype IN ('c', 'f') ORDER BY 1`

// statementsOf joins groups of statements into one list.
func statementsOf(groups ...[]string) []string {
	var statements []string
	for _, group := range groups {
		statements = append(statements, group...)
	}
	return statements
}

// schemaFileOf is statements as a schema file.
func schemaFileOf(statements []string) string {
	return strings.Join(statements, ";\n") + ";"
}

// notValidConstraints answers the read-back over the database at url.
func notValidConstraints(c *qt.C, url string) []string {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(c.Context(), url)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	rows, err := conn.QueryContext(c.Context(), notValidReadBack)
	c.Assert(err, qt.IsNil)
	defer rows.Close()
	var described []string
	for rows.Next() {
		var line string
		c.Assert(rows.Scan(&line), qt.IsNil)
		described = append(described, line)
	}
	c.Assert(rows.Err(), qt.IsNil)
	return described
}

// TestSchemaApplyAddsAConstraintNotValidOverRowsThatBreakItE2E applies a file
// that adds a CHECK and a foreign key NOT VALID to a table holding a row both
// reject. Added validated, either would be refused. The constraints read back
// as the file's own SQL builds them, and the next apply is synced.
func TestSchemaApplyAddsAConstraintNotValidOverRowsThatBreakItE2E(t *testing.T) {
	for _, engine := range notValidEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			file := statementsOf(notValidTables, notValidAdded)
			want := notValidConstraints(c, engine.built(c, file))
			target := engine.built(c, statementsOf(notValidTables, []string{`INSERT INTO nc VALUES (1, -1, 7)`}))
			_, schema := writeRewrittenMigrationProject(c, schemaFileOf(file), schemaFileOf(file))

			runPtahNative(c, "schema", "apply", "--db-url", target, "--schema-file", schema, "--auto-approve")

			c.Assert(notValidConstraints(c, target), qt.DeepEquals, want)
			plan := runPtahNative(c, "schema", "apply", "--db-url", target, "--schema-file", schema, "--dry-run")
			c.Assert(plan, qt.Contains, "Schema is synced")
		})
	}
}

// TestSchemaApplyValidatesWhatTheFileHoldsValidatedE2E applies a file that
// declares validated the constraints the database holds NOT VALID. The plan
// validates each rather than recreating it, the constraints read back as the
// file's own SQL builds them, and the next apply is synced.
func TestSchemaApplyValidatesWhatTheFileHoldsValidatedE2E(t *testing.T) {
	for _, engine := range notValidEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			file := statementsOf(notValidTables, validatedAdded)
			want := notValidConstraints(c, engine.built(c, file))
			target := engine.built(c, statementsOf(notValidTables, notValidAdded))
			_, schema := writeRewrittenMigrationProject(c, schemaFileOf(file), schemaFileOf(file))

			plan := runPtahNative(c, "schema", "apply", "--db-url", target, "--schema-file", schema, "--dry-run")
			c.Assert(plan, qt.Contains, "VALIDATE CONSTRAINT")
			c.Assert(plan, qt.Not(qt.Contains), "DROP CONSTRAINT")
			runPtahNative(c, "schema", "apply", "--db-url", target, "--schema-file", schema, "--auto-approve")

			c.Assert(notValidConstraints(c, target), qt.DeepEquals, want)
			plan = runPtahNative(c, "schema", "apply", "--db-url", target, "--schema-file", schema, "--dry-run")
			c.Assert(plan, qt.Contains, "Schema is synced")
		})
	}
}

// TestSchemaApplyKeepsValidatedWhatTheFileAllowsNotValidE2E applies a file
// that allows NOT VALID to a database holding the constraints validated:
// nothing is planned.
func TestSchemaApplyKeepsValidatedWhatTheFileAllowsNotValidE2E(t *testing.T) {
	for _, engine := range notValidEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			file := schemaFileOf(statementsOf(notValidTables, notValidAdded))
			target := engine.built(c, statementsOf(notValidTables, validatedAdded))
			_, schema := writeRewrittenMigrationProject(c, file, file)

			plan := runPtahNative(c, "schema", "apply", "--db-url", target, "--schema-file", schema, "--dry-run")

			c.Assert(plan, qt.Contains, "Schema is synced")
		})
	}
}

// TestSchemaDiffFromADatabaseWithUnvalidatedConstraintsToItselfIsSyncedE2E
// diffs a database holding NOT VALID constraints with itself: nothing is
// validated.
func TestSchemaDiffFromADatabaseWithUnvalidatedConstraintsToItselfIsSyncedE2E(t *testing.T) {
	for _, engine := range notValidEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			source := engine.built(c, statementsOf(notValidTables, notValidAdded))

			out := runPtahNative(c, "schema", "diff", "--from", source, "--to", source)

			c.Assert(out, qt.Contains, "Schemas are synced")
		})
	}
}

// TestMigrateDiffFindsUnvalidatedConstraintsSyncedE2E replays a directory
// whose one migration is the schema file and diffs: the NOT VALID constraints
// the replay builds pair with the file's.
func TestMigrateDiffFindsUnvalidatedConstraintsSyncedE2E(t *testing.T) {
	for _, engine := range notValidEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			file := schemaFileOf(statementsOf(notValidTables, notValidAdded))
			dir, schema := writeRewrittenMigrationProject(c, file, file)

			plan, err := runCompatVerb("migrate", "diff", "--dir", "file://"+dir,
				"--to", "file://"+schema, "--dev-url", engine.built(c, nil), "--dry-run")

			c.Assert(err, qt.IsNil, qt.Commentf("%s", plan))
			c.Assert(plan, qt.Contains, "The migration directory is synced with the desired state")
		})
	}
}
