//go:build integration

package integration_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
)

const (
	// typeHoldersKeep is what every row declares.
	typeHoldersKeep = "CREATE TABLE keep (id bigint PRIMARY KEY);\n"
	// typeHoldersGone is a table, and a type that names its row type.
	typeHoldersGone = "CREATE TABLE gone (id bigint PRIMARY KEY);\nCREATE TYPE wrap AS (r gone);\n"
	// typeHoldersUndeclared is a reader of gone in a schema the run leaves out.
	typeHoldersUndeclared = "CREATE SCHEMA reporting;\nCREATE VIEW reporting.v AS SELECT id FROM public.gone;\nINSERT INTO public.gone VALUES (7);\n"
	// typeHoldersLeft counts the relations, types and routines public holds
	// besides keep and the record schema apply keeps of its runs, which a run
	// that fails still writes.
	typeHoldersLeft = "SELECT (SELECT count(*) FROM pg_class WHERE relnamespace = 'public'::regnamespace AND relkind IN ('r', 'v', 'm', 'c') AND relname NOT IN ('keep', 'keep_pkey', 'ptah_schema_apply_log')) + " +
		"(SELECT count(*) FROM pg_type WHERE typnamespace = 'public'::regnamespace AND typtype = 'd') + " +
		"(SELECT count(*) FROM pg_proc WHERE pronamespace = 'public'::regnamespace)"
)

// typeHoldersRows remove a table, a type over its row type, and what uses the type.
// That use kept the type until after the tables, so the table's drop took CASCADE, and
// the server dropped a view in a schema the run leaves out with exit 0.
var typeHoldersRows = []struct{ name, built, plan string }{
	{
		name: "a removed table's column", built: "CREATE TABLE holder (id bigint PRIMARY KEY, w wrap);\n",
		plan: "DROP TABLE \"holder\";\nDROP TYPE \"wrap\";\nDROP TABLE IF EXISTS \"gone\";",
	},
	{
		name: "an array column", built: "CREATE TABLE holder (id bigint PRIMARY KEY, w wrap[]);\n",
		plan: "DROP TABLE \"holder\";\nDROP TYPE \"wrap\";\nDROP TABLE IF EXISTS \"gone\";",
	},
	{
		name: "a type over the type", built: "CREATE TYPE outer_t AS (w wrap);\nCREATE TABLE holder (id bigint PRIMARY KEY, w outer_t);\n",
		plan: "DROP TABLE \"holder\";\nDROP TYPE \"outer_t\";\nDROP TYPE \"wrap\";\nDROP TABLE IF EXISTS \"gone\";",
	},
	{
		name: "a domain over the type", built: "CREATE DOMAIN dw AS wrap;\nCREATE TABLE holder (id bigint PRIMARY KEY, w dw);\n",
		plan: "DROP TABLE \"holder\";\nDROP DOMAIN \"dw\";\nDROP TYPE \"wrap\";\nDROP TABLE IF EXISTS \"gone\";",
	},
	{
		name: "a removed view over the table that holds it", built: "CREATE TABLE holder (id bigint PRIMARY KEY, w wrap);\nCREATE VIEW hv AS SELECT id FROM holder;\n",
		plan: "DROP VIEW \"hv\";\nDROP TABLE \"holder\";\nDROP TYPE \"wrap\";\nDROP TABLE IF EXISTS \"gone\";",
	},
	{
		name: "a foreign key from the table that holds it", built: "CREATE TABLE holder (id bigint PRIMARY KEY, g bigint REFERENCES gone (id), w wrap);\n",
		plan: "DROP TABLE \"holder\";\nDROP TYPE \"wrap\";\nDROP TABLE IF EXISTS \"gone\";",
	},
	{
		name: "a routine that takes it", built: "CREATE FUNCTION f(x wrap) RETURNS int LANGUAGE SQL RETURN 1;\n",
		plan: "DROP FUNCTION \"f\"(x wrap);\nDROP TYPE \"wrap\";\nDROP TABLE IF EXISTS \"gone\";",
	},
	{
		name: "a routine that returns it", built: "CREATE FUNCTION f() RETURNS wrap LANGUAGE SQL RETURN NULL::wrap;\n",
		plan: "DROP FUNCTION \"f\"();\nDROP TYPE \"wrap\";\nDROP TABLE IF EXISTS \"gone\";",
	},
	{
		name: "a caller of a routine that takes it", built: "CREATE FUNCTION f(x wrap) RETURNS int LANGUAGE SQL RETURN 1;\n" +
			"CREATE FUNCTION g() RETURNS int LANGUAGE SQL RETURN f(NULL::wrap);\n",
		plan: "DROP FUNCTION \"g\"();\nDROP FUNCTION \"f\"(x wrap);\nDROP TYPE \"wrap\";\nDROP TABLE IF EXISTS \"gone\";",
	},
}

// TestSchemaApplyRemovedTypeHoldersE2E_HappyPath is the control for the rows: with
// nothing outside the declaration reading the table, the plan ends in the declared
// state, and a second run finds nothing to do.
func TestSchemaApplyRemovedTypeHoldersE2E_HappyPath(t *testing.T) {
	for _, test := range typeHoldersRows {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			target := databaseBuiltFrom(c, typeHoldersKeep+typeHoldersGone+test.built)

			run := keyedApply(c, target, typeHoldersKeep)

			c.Assert(run.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", run.Stdout, run.Stderr))
			c.Assert(run.Stdout, qt.Not(qt.Contains), "CASCADE")
			c.Assert(run.Stdout, qt.Contains, test.plan)
			c.Assert(readInt(c, target, typeHoldersLeft), qt.Equals, 0)
			again := keyedApply(c, target, typeHoldersKeep)
			c.Assert(again.Stdout, qt.Contains, "Schema is synced")
		})
	}
}

// TestSchemaApplyRemovedTypeHoldersE2E_AnUndeclaredReaderStopsThePlan gives the removed
// table a view in a schema the run leaves out. The server refuses the drop and names
// it, the view keeps its row, and the plan's one transaction takes back what ran
// before the refusal.
func TestSchemaApplyRemovedTypeHoldersE2E_AnUndeclaredReaderStopsThePlan(t *testing.T) {
	for _, test := range typeHoldersRows {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			target := databaseBuiltFrom(c, typeHoldersKeep+typeHoldersGone+test.built+typeHoldersUndeclared)
			left := readInt(c, target, typeHoldersLeft)

			run := keyedApply(c, target, typeHoldersKeep)

			c.Assert(run.ExitCode, qt.Equals, 2, qt.Commentf("stdout:\n%s\nstderr:\n%s", run.Stdout, run.Stderr))
			c.Assert(run.Stdout, qt.Not(qt.Contains), "CASCADE")
			c.Assert(run.Stderr, qt.Contains, "cannot drop table gone because other objects depend on it")
			c.Assert(readInt(c, target, "SELECT count(*) FROM reporting.v"), qt.Equals, 1)
			c.Assert(readInt(c, target, typeHoldersLeft), qt.Equals, left)
		})
	}
}
