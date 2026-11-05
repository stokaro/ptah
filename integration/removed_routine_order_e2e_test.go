//go:build integration

package integration_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/clirun"
)

const (
	// removedRoutineKeep is what every row below declares.
	removedRoutineKeep = "CREATE TABLE keep (id bigint PRIMARY KEY);\n"
	// removedRoutineCycle is two functions whose SQL-standard bodies call each
	// other, which CREATE OR REPLACE allows.
	removedRoutineCycle = "CREATE FUNCTION a_fn() RETURNS int LANGUAGE SQL RETURN 1;\n" +
		"CREATE FUNCTION b_fn() RETURNS int LANGUAGE SQL RETURN a_fn() + 1;\n" +
		"CREATE OR REPLACE FUNCTION a_fn() RETURNS int LANGUAGE SQL RETURN b_fn() + 1;\n"
	// removedRoutineChain is b_fn calling a_fn in a SQL-standard body.
	removedRoutineChain = "CREATE FUNCTION a_fn() RETURNS int LANGUAGE SQL RETURN 1;\n" +
		"CREATE FUNCTION b_fn() RETURNS int LANGUAGE SQL RETURN a_fn() + 1;\n"
	// removedRoutineCount counts the routines public holds.
	removedRoutineCount = "SELECT count(*) FROM pg_proc WHERE pronamespace = 'public'::regnamespace"
)

// removedRoutineRows are routines a plan removes together. PostgreSQL records
// what each body and parameter default below calls, so a drop in name order
// stopped at `cannot drop function a_fn() because other objects depend on it`,
// in both statement styles.
var removedRoutineRows = []struct{ name, built string }{
	{name: "a RETURN body", built: removedRoutineChain},
	{name: "a schema-qualified call", built: "CREATE FUNCTION a_fn() RETURNS int LANGUAGE SQL RETURN 1;\n" +
		"CREATE FUNCTION b_fn() RETURNS int LANGUAGE SQL RETURN public.a_fn() + 1;\n"},
	{name: "a quoted name", built: "CREATE FUNCTION \"A fn\"() RETURNS int LANGUAGE SQL RETURN 1;\n" +
		"CREATE FUNCTION b_fn() RETURNS int LANGUAGE SQL RETURN \"A fn\"() + 1;\n"},
	{name: "a chain in BEGIN ATOMIC", built: "CREATE FUNCTION a_fn() RETURNS int LANGUAGE SQL BEGIN ATOMIC SELECT 1; END;\n" +
		"CREATE FUNCTION m_fn() RETURNS int LANGUAGE SQL BEGIN ATOMIC SELECT a_fn(); END;\n" +
		"CREATE FUNCTION z_fn() RETURNS int LANGUAGE SQL BEGIN ATOMIC SELECT m_fn(); END;\n"},
	{name: "a procedure calls a function", built: "CREATE FUNCTION a_fn() RETURNS int LANGUAGE SQL RETURN 1;\n" +
		"CREATE PROCEDURE z_proc() LANGUAGE SQL BEGIN ATOMIC SELECT a_fn(); END;\n"},
	{name: "a parameter default under a quoted body", built: "CREATE FUNCTION a_fn() RETURNS int LANGUAGE SQL AS 'select 1';\n" +
		"CREATE FUNCTION b_fn(x int DEFAULT a_fn()) RETURNS int LANGUAGE plpgsql AS 'begin return x; end';\n"},
	{name: "an overload calls its namesake", built: "CREATE FUNCTION f(int) RETURNS int LANGUAGE SQL RETURN $1;\n" +
		"CREATE FUNCTION f(text) RETURNS int LANGUAGE SQL RETURN f(length($1));\n"},
	{name: "a removed domain calls the caller", built: "CREATE FUNCTION a_fn(int) RETURNS boolean LANGUAGE SQL RETURN $1 > 0;\n" +
		"CREATE FUNCTION b_fn(int) RETURNS boolean LANGUAGE SQL RETURN a_fn($1);\n" +
		"CREATE DOMAIN d AS int CHECK (b_fn(VALUE));\n"},
	{name: "a removed table's generated column calls the caller", built: "CREATE FUNCTION a_fn(int) RETURNS int IMMUTABLE LANGUAGE SQL RETURN $1 + 1;\n" +
		"CREATE FUNCTION b_fn(int) RETURNS int IMMUTABLE LANGUAGE SQL RETURN a_fn($1) + 1;\n" +
		"CREATE TABLE gone (id int, g int GENERATED ALWAYS AS (b_fn(id)) STORED);\n"},
	{name: "two functions that call each other", built: removedRoutineCycle},
}

// removedRoutineApplyAgain runs `schema apply` of removedRoutineKeep over the
// database at target.
func removedRoutineApplyAgain(c *qt.C, target string) clirun.Result {
	c.Helper()
	schema := filepath.Join(c.TempDir(), "schema.sql")
	c.Assert(os.WriteFile(schema, []byte(removedRoutineKeep), 0o600), qt.IsNil)
	return runPtahIn(c, "schema", "apply", "--db-url", target, "--schema-file", schema, "--schemas", "public", "--auto-approve")
}

// TestSchemaApplyRemovedRoutinesE2E_HappyPath reads the routines back from the
// server, and a second run finding nothing proves the plan ended in the declared
// state.
func TestSchemaApplyRemovedRoutinesE2E_HappyPath(t *testing.T) {
	for _, test := range removedRoutineRows {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			target, run := runExactApply(c, removedRoutineKeep+test.built, removedRoutineKeep)

			c.Assert(run.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", run.Stdout, run.Stderr))
			c.Assert(run.Stdout, qt.Not(qt.Contains), "CASCADE")
			c.Assert(readInt(c, target, removedRoutineCount), qt.Equals, 0)
			again := removedRoutineApplyAgain(c, target)
			c.Assert(again.Stdout, qt.Contains, "Schema is synced")
		})
	}
}

// TestSchemaApplyRemovedRoutinesE2E_AnUndeclaredReaderStopsTheDrop gives a_fn a
// view in a schema the run leaves out. The caller's drop runs, the server refuses
// the callee's and names the view, and the plan's one transaction takes the
// caller back. Functions that call each other go in one statement, which the
// server refuses as a whole.
func TestSchemaApplyRemovedRoutinesE2E_AnUndeclaredReaderStopsTheDrop(t *testing.T) {
	tests := []struct {
		name, built, refusal string
		plan                 string
	}{
		{
			name: "a caller and its callee", built: removedRoutineChain,
			plan:    "DROP FUNCTION \"b_fn\"();\nDROP FUNCTION \"a_fn\"();",
			refusal: "cannot drop function a_fn() because other objects depend on it",
		},
		{
			name: "two functions that call each other", built: removedRoutineCycle,
			plan:    "DROP FUNCTION \"a_fn\"(), \"b_fn\"();",
			refusal: "cannot drop desired object(s) because other objects depend on them",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			target, run := runExactApply(c, removedRoutineKeep+test.built+
				"CREATE SCHEMA reporting;\nCREATE VIEW reporting.v AS SELECT public.a_fn();\n", removedRoutineKeep)

			c.Assert(run.ExitCode, qt.Equals, 2, qt.Commentf("stdout:\n%s\nstderr:\n%s", run.Stdout, run.Stderr))
			c.Assert(run.Stdout, qt.Contains, test.plan)
			c.Assert(run.Stderr, qt.Contains, test.refusal)
			c.Assert(readInt(c, target, removedRoutineCount), qt.Equals, 2)
		})
	}
}

// TestMigrationsGenerateRemovedRoutinesE2E_HappyPath runs the plan through a
// migration file. The one-statement drop reaches the server through the file's
// statement split like any other.
func TestMigrationsGenerateRemovedRoutinesE2E_HappyPath(t *testing.T) {
	for _, test := range []struct{ name, built string }{
		{name: "a caller and its callee", built: removedRoutineChain},
		{name: "two functions that call each other", built: removedRoutineCycle},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			target := databaseBuiltFrom(c, removedRoutineKeep+test.built)
			dir := exactGeneratedFile(c, target, removedRoutineKeep)

			up := runPtahIn(c, "migrations", "up", "--db-url", target, "--migrations-dir", dir, "--allow-destructive")

			c.Assert(up.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", up.Stdout, up.Stderr))
			c.Assert(readInt(c, target, removedRoutineCount), qt.Equals, 0)
		})
	}
}

// TestSchemaApplyRemovedRoutinesE2E_OnCockroachDBABodyHoldsItsCallee is the
// engine that records what a quoted body calls: a drop in name order stopped at
// `cannot drop function "a_fn" because other objects ([db.public.b_fn]) still
// depend on it`.
func TestSchemaApplyRemovedRoutinesE2E_OnCockroachDBABodyHoldsItsCallee(t *testing.T) {
	tests := []struct{ name, built string }{
		{name: "a quoted SQL body", built: "CREATE FUNCTION a_fn() RETURNS INT8 LANGUAGE SQL AS $$ SELECT 1 $$;\n" +
			"CREATE FUNCTION b_fn() RETURNS INT8 LANGUAGE SQL AS $$ SELECT a_fn() + 1 $$;\n"},
		{name: "a PL/pgSQL body", built: "CREATE FUNCTION a_fn() RETURNS INT8 LANGUAGE PLPGSQL AS $$ BEGIN RETURN 1; END $$;\n" +
			"CREATE FUNCTION b_fn() RETURNS INT8 LANGUAGE PLPGSQL AS $$ BEGIN RETURN a_fn() + 1; END $$;\n"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			target, _ := cockroachVisibilityDatabase(c, c.Context(), "CREATE TABLE keep (id INT8 PRIMARY KEY);\n"+test.built)
			schema := filepath.Join(c.TempDir(), "schema.sql")
			c.Assert(os.WriteFile(schema, []byte("CREATE TABLE keep (id INT8 PRIMARY KEY);\n"), 0o600), qt.IsNil)

			run := runPtahIn(c, "schema", "apply", "--db-url", target, "--schema-file", schema, "--schemas", "public", "--auto-approve")

			c.Assert(run.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", run.Stdout, run.Stderr))
			c.Assert(run.Stdout, qt.Contains, "DROP FUNCTION \"b_fn\"();\nDROP FUNCTION \"a_fn\"();")
			c.Assert(readInt(c, target, "SELECT count(*) FROM information_schema.routines WHERE routine_schema = 'public'"), qt.Equals, 0)
			again := runPtahIn(c, "schema", "apply", "--db-url", target, "--schema-file", schema, "--schemas", "public", "--auto-approve")
			c.Assert(again.Stdout, qt.Contains, "Schema is synced")
		})
	}
}
