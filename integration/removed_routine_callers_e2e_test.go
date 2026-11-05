//go:build integration

package integration_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"
)

const (
	// removedCallerGone is the table each row removes.
	removedCallerGone = "CREATE TABLE gone (id bigint PRIMARY KEY);\n"
	// removedCallerReader reads gone in a SQL-standard body, so the server
	// records the dependency.
	removedCallerReader = "CREATE FUNCTION a_fn() RETURNS bigint LANGUAGE SQL RETURN (SELECT count(*) FROM gone);\n"
	// removedCallerUndeclared is a reader of gone in a schema the run leaves out.
	removedCallerUndeclared = "CREATE SCHEMA reporting;\nCREATE VIEW reporting.v AS SELECT id FROM public.gone;\nINSERT INTO public.gone VALUES (7);\n"
	// removedCallerRelations counts what public holds besides keep.
	removedCallerRelations = "SELECT count(*) FROM pg_class WHERE relnamespace = 'public'::regnamespace AND relkind IN ('r', 'v', 'm') AND relname <> 'keep'"
)

// removedCallerRows are removals of a routine that has something calling it, and
// that a removed table, view or column depends on. Native `schema apply` planned
// each with CASCADE on the first drop the routine held back, which took a view
// in a schema the run leaves out and exited 0.
var removedCallerRows = []struct{ name, built, plan string }{
	{
		name: "a function calls the reader", built: removedCallerReader + "CREATE FUNCTION b_fn() RETURNS bigint LANGUAGE SQL RETURN a_fn();\n",
		plan: "DROP FUNCTION \"b_fn\"();\nDROP FUNCTION \"a_fn\"();\nDROP TABLE \"gone\";",
	},
	{
		name: "a chain of callers", built: removedCallerReader + "CREATE FUNCTION b_fn() RETURNS bigint LANGUAGE SQL RETURN a_fn();\n" +
			"CREATE FUNCTION c_fn() RETURNS bigint LANGUAGE SQL RETURN b_fn();\n",
		plan: "DROP FUNCTION \"c_fn\"();\nDROP FUNCTION \"b_fn\"();\nDROP FUNCTION \"a_fn\"();\nDROP TABLE \"gone\";",
	},
	{
		name: "a procedure calls the reader", built: removedCallerReader + "CREATE PROCEDURE p_proc() LANGUAGE SQL BEGIN ATOMIC SELECT a_fn(); END;\n",
		plan: "DROP PROCEDURE \"p_proc\"();\nDROP FUNCTION \"a_fn\"();\nDROP TABLE \"gone\";",
	},
	{
		name: "a routine reads two removed tables", built: "CREATE TABLE gone2 (id bigint PRIMARY KEY);\n" +
			"CREATE FUNCTION a_fn() RETURNS bigint LANGUAGE SQL RETURN (SELECT count(*) FROM gone) + (SELECT count(*) FROM gone2);\n",
		plan: "DROP FUNCTION \"a_fn\"();\nDROP TABLE \"gone\";",
	},
	{
		name: "a routine reads a removed view and a removed table", built: "CREATE VIEW gv AS SELECT id FROM gone;\n" +
			"CREATE FUNCTION a_fn() RETURNS bigint LANGUAGE SQL RETURN (SELECT count(*) FROM gv) + (SELECT count(*) FROM gone);\n",
		plan: "DROP FUNCTION \"a_fn\"();\nDROP VIEW \"gv\";\nDROP TABLE \"gone\";",
	},
	{
		name: "two readers call each other", built: removedCallerReader + "CREATE FUNCTION b_fn() RETURNS bigint LANGUAGE SQL RETURN a_fn();\n" +
			"CREATE OR REPLACE FUNCTION a_fn() RETURNS bigint LANGUAGE SQL RETURN (SELECT count(*) FROM gone) + b_fn();\n",
		plan: "DROP FUNCTION \"a_fn\"(), \"b_fn\"();\nDROP TABLE \"gone\";",
	},
	{
		name: "a function calls a routine that takes the row", built: "CREATE FUNCTION a_fn(g gone) RETURNS bigint LANGUAGE SQL RETURN g.id;\n" +
			"CREATE FUNCTION b_fn() RETURNS bigint LANGUAGE SQL RETURN a_fn(NULL::gone);\n",
		plan: "DROP FUNCTION \"b_fn\"();\nDROP FUNCTION \"a_fn\"(g gone);\nDROP TABLE \"gone\";",
	},
	{
		name: "a removed table's default calls the reader", built: removedCallerReader + "CREATE TABLE zzz (id bigint PRIMARY KEY, n bigint DEFAULT a_fn());\n",
		plan: "DROP TABLE \"zzz\";\nDROP FUNCTION \"a_fn\"();\nDROP TABLE \"gone\";",
	},
	{
		name: "a removed view calls the reader", built: removedCallerReader + "CREATE VIEW cv AS SELECT a_fn() AS n;\n",
		plan: "DROP VIEW \"cv\";\nDROP FUNCTION \"a_fn\"();\nDROP TABLE \"gone\";",
	},
	{
		name: "a removed view calls the reader of another removed view", built: "CREATE VIEW gv AS SELECT id FROM gone;\n" +
			"CREATE FUNCTION a_fn() RETURNS bigint LANGUAGE SQL RETURN (SELECT count(*) FROM gv);\nCREATE VIEW zv AS SELECT a_fn() AS n;\n",
		plan: "DROP VIEW \"zv\";\nDROP FUNCTION \"a_fn\"();\nDROP VIEW \"gv\";\nDROP TABLE \"gone\";",
	},
	{
		name: "a materialized view calls the reader of a removed view", built: "CREATE VIEW gv AS SELECT id FROM gone;\n" +
			"CREATE FUNCTION a_fn() RETURNS bigint LANGUAGE SQL RETURN (SELECT count(*) FROM gv);\nCREATE MATERIALIZED VIEW mv AS SELECT a_fn() AS n;\n",
		plan: "DROP MATERIALIZED VIEW \"mv\";\nDROP FUNCTION \"a_fn\"();\nDROP VIEW \"gv\";\nDROP TABLE \"gone\";",
	},
	{
		name: "a dropped column's default calls the reader", built: removedCallerReader + "ALTER TABLE keep ADD COLUMN n bigint DEFAULT a_fn();\n",
		plan: "ALTER TABLE \"keep\" DROP COLUMN \"n\";\nDROP FUNCTION \"a_fn\"();\nDROP TABLE \"gone\";",
	},
	{
		name: "a policy of the removed table calls the reader", built: removedCallerReader +
			"ALTER TABLE gone ENABLE ROW LEVEL SECURITY;\nCREATE POLICY p ON gone USING (a_fn() > 0);\n",
		plan: "DROP POLICY \"p\" ON \"gone\";\nDROP FUNCTION \"a_fn\"();\nDROP TABLE \"gone\";",
	},
}

// TestSchemaApplyRemovedRoutineCallersE2E_HappyPath is the control for the rows:
// with nothing outside the declaration reading the table, the plan ends in the
// declared state. A second run finding nothing proves it.
func TestSchemaApplyRemovedRoutineCallersE2E_HappyPath(t *testing.T) {
	for _, test := range removedCallerRows {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			target, run := runExactApply(c, removedRoutineKeep+removedCallerGone+test.built, removedRoutineKeep)

			c.Assert(run.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", run.Stdout, run.Stderr))
			c.Assert(run.Stdout, qt.Not(qt.Contains), "CASCADE")
			c.Assert(run.Stdout, qt.Contains, test.plan)
			c.Assert(readInt(c, target, removedCallerRelations), qt.Equals, 0)
			c.Assert(readInt(c, target, removedRoutineCount), qt.Equals, 0)
			again := removedRoutineApplyAgain(c, target)
			c.Assert(again.Stdout, qt.Contains, "Schema is synced")
		})
	}
}

// TestSchemaApplyRemovedRoutineCallersE2E_AnUndeclaredReaderStopsThePlan gives the
// removed table a view in a schema the run leaves out. The server refuses the
// drop and names it, the view keeps its row, and the plan's one transaction
// takes back what ran before the refusal.
func TestSchemaApplyRemovedRoutineCallersE2E_AnUndeclaredReaderStopsThePlan(t *testing.T) {
	for _, test := range removedCallerRows {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			built := removedRoutineKeep + removedCallerGone + test.built + removedCallerUndeclared
			target := databaseBuiltFrom(c, built)
			routines := readInt(c, target, removedRoutineCount)
			relations := readInt(c, target, removedCallerRelations)

			run := removedRoutineApplyAgain(c, target)

			c.Assert(run.ExitCode, qt.Equals, 2, qt.Commentf("stdout:\n%s\nstderr:\n%s", run.Stdout, run.Stderr))
			c.Assert(run.Stdout, qt.Not(qt.Contains), "CASCADE")
			c.Assert(run.Stderr, qt.Contains, "cannot drop table gone because other objects depend on it")
			c.Assert(readInt(c, target, "SELECT count(*) FROM reporting.v"), qt.Equals, 1)
			c.Assert(readInt(c, target, removedRoutineCount), qt.Equals, routines)
			c.Assert(readInt(c, target, removedCallerRelations), qt.Equals, relations)
		})
	}
}

// TestSchemaApplyRemovedRoutineCallersE2E_AReaderOfADroppedColumn is the same
// shape over a column of a table the plan keeps.
func TestSchemaApplyRemovedRoutineCallersE2E_AReaderOfADroppedColumn(t *testing.T) {
	const (
		built = "CREATE TABLE keep (id bigint PRIMARY KEY, b int);\n" +
			"CREATE FUNCTION a_fn() RETURNS bigint LANGUAGE SQL RETURN (SELECT count(b) FROM keep);\n" +
			"CREATE FUNCTION b_fn() RETURNS bigint LANGUAGE SQL RETURN a_fn();\n"
		plan    = "DROP FUNCTION \"b_fn\"();\nDROP FUNCTION \"a_fn\"();\nALTER TABLE \"keep\" DROP COLUMN \"b\";"
		columnB = "SELECT count(*) FROM pg_attribute WHERE attrelid = 'public.keep'::regclass AND attname = 'b'"
	)
	t.Run("the plan ends in the declared state", func(t *testing.T) {
		c := qt.New(t)

		target, run := runExactApply(c, built, removedRoutineKeep)

		c.Assert(run.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", run.Stdout, run.Stderr))
		c.Assert(run.Stdout, qt.Contains, plan)
		c.Assert(readInt(c, target, columnB), qt.Equals, 0)
		c.Assert(readInt(c, target, removedRoutineCount), qt.Equals, 0)
	})
	t.Run("an undeclared reader stops it", func(t *testing.T) {
		c := qt.New(t)

		target, run := runExactApply(c, built+"CREATE SCHEMA reporting;\nCREATE VIEW reporting.v AS SELECT b FROM public.keep;\n", removedRoutineKeep)

		c.Assert(run.ExitCode, qt.Equals, 2, qt.Commentf("stdout:\n%s\nstderr:\n%s", run.Stdout, run.Stderr))
		c.Assert(run.Stdout, qt.Not(qt.Contains), "CASCADE")
		c.Assert(run.Stderr, qt.Contains, "cannot drop column b of table keep because other objects depend on it")
		c.Assert(readInt(c, target, "SELECT count(*) FROM pg_class WHERE relname = 'v'"), qt.Equals, 1)
		c.Assert(readInt(c, target, columnB), qt.Equals, 1)
		c.Assert(readInt(c, target, removedRoutineCount), qt.Equals, 2)
	})
}

// TestSchemaApplyRemovedRoutineCallersE2E_FailurePath is the shapes no order of
// drops serves. The plan is refused before a statement runs, so the database
// reads back whole, undeclared view and all.
func TestSchemaApplyRemovedRoutineCallersE2E_FailurePath(t *testing.T) {
	tests := []struct{ name, built, refusal string }{
		{
			name:    "a table's default calls its own reader",
			built:   removedCallerReader + "ALTER TABLE gone ADD COLUMN n bigint DEFAULT a_fn();\n",
			refusal: "table gone calls routine a_fn, which depends on table gone",
		},
		{
			name: "a removed domain calls the reader",
			built: "CREATE FUNCTION a_fn(int) RETURNS boolean LANGUAGE SQL RETURN (SELECT count(*) FROM gone) >= $1;\n" +
				"CREATE DOMAIN d AS int CHECK (a_fn(VALUE));\n",
			refusal: "domain d calls routine a_fn, which depends on table gone",
		},
		{
			name: "a removed table's default calls the reader of a removed view",
			built: "CREATE VIEW gv AS SELECT id FROM gone;\nCREATE FUNCTION a_fn() RETURNS bigint LANGUAGE SQL RETURN (SELECT count(*) FROM gv);\n" +
				"CREATE TABLE zzz (id bigint PRIMARY KEY, n bigint DEFAULT a_fn());\n",
			refusal: "table zzz calls routine a_fn, which depends on view gv",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			target := databaseBuiltFrom(c, removedRoutineKeep+removedCallerGone+test.built+removedCallerUndeclared)
			routines := readInt(c, target, removedRoutineCount)
			relations := readInt(c, target, removedCallerRelations)

			run := removedRoutineApplyAgain(c, target)

			c.Assert(run.ExitCode, qt.Equals, 2, qt.Commentf("stdout:\n%s\nstderr:\n%s", run.Stdout, run.Stderr))
			c.Assert(run.Stderr, qt.Contains, test.refusal)
			c.Assert(run.Stderr, qt.Contains, "the plan has no order that drops them all without CASCADE")
			c.Assert(run.Stdout, qt.Not(qt.Contains), "DROP ")
			c.Assert(readInt(c, target, "SELECT count(*) FROM reporting.v"), qt.Equals, 1)
			c.Assert(readInt(c, target, removedRoutineCount), qt.Equals, routines)
			c.Assert(readInt(c, target, removedCallerRelations), qt.Equals, relations)
		})
	}
}

// TestSchemaApplyRemovedRoutineCallersE2E_OnCockroachDB is the engine that also
// records what a quoted body reads and calls, and that refuses a drop on what it
// records.
func TestSchemaApplyRemovedRoutineCallersE2E_OnCockroachDB(t *testing.T) {
	const (
		keep  = "CREATE TABLE keep (id INT8 PRIMARY KEY);\n"
		table = "CREATE TABLE gone (id INT8 PRIMARY KEY);\n"
	)
	tests := []struct{ name, built, plan string }{
		{
			name: "a function calls the reader",
			built: "CREATE FUNCTION a_fn() RETURNS INT8 LANGUAGE SQL AS $$ SELECT count(*) FROM gone $$;\n" +
				"CREATE FUNCTION b_fn() RETURNS INT8 LANGUAGE SQL AS $$ SELECT a_fn() $$;\n",
			plan: "DROP FUNCTION \"b_fn\"();\nDROP FUNCTION \"a_fn\"();\nDROP TABLE \"gone\";",
		},
		{
			name: "a removed view calls the reader of another removed view",
			built: "CREATE VIEW gv AS SELECT id FROM gone;\n" +
				"CREATE FUNCTION a_fn() RETURNS INT8 LANGUAGE SQL AS $$ SELECT count(*) FROM gv $$;\n" +
				"CREATE VIEW zv AS SELECT a_fn() AS n;\n",
			plan: "DROP VIEW \"zv\";\nDROP FUNCTION \"a_fn\"();\nDROP VIEW \"gv\";\nDROP TABLE \"gone\";",
		},
		{
			name: "a removed table's default calls the reader",
			built: "CREATE FUNCTION a_fn() RETURNS INT8 LANGUAGE SQL AS $$ SELECT count(*) FROM gone $$;\n" +
				"CREATE TABLE zzz (id INT8 PRIMARY KEY, n INT8 DEFAULT a_fn());\n",
			plan: "DROP TABLE \"zzz\";\nDROP FUNCTION \"a_fn\"();\nDROP TABLE \"gone\";",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			target, _ := cockroachVisibilityDatabase(c, c.Context(), keep+table+test.built)
			schema := filepath.Join(c.TempDir(), "schema.sql")
			c.Assert(os.WriteFile(schema, []byte(keep), 0o600), qt.IsNil)

			run := runPtahIn(c, "schema", "apply", "--db-url", target, "--schema-file", schema, "--schemas", "public", "--auto-approve")

			c.Assert(run.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", run.Stdout, run.Stderr))
			c.Assert(run.Stdout, qt.Not(qt.Contains), "CASCADE")
			c.Assert(run.Stdout, qt.Contains, test.plan)
			c.Assert(readInt(c, target, "SELECT count(*) FROM information_schema.routines WHERE routine_schema = 'public'"), qt.Equals, 0)
			c.Assert(readInt(c, target, "SELECT count(*) FROM information_schema.tables WHERE table_schema = 'public' AND table_name <> 'keep'"), qt.Equals, 0)
			again := runPtahIn(c, "schema", "apply", "--db-url", target, "--schema-file", schema, "--schemas", "public", "--auto-approve")
			c.Assert(again.Stdout, qt.Contains, "Schema is synced")
		})
	}
}
