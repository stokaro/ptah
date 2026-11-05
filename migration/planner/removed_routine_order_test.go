package planner_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/planext"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/feature/pgtype"
	"ptah.run/migration/planner"
	"ptah.run/migration/schemadiff"
)

// quotedRoutine is a function whose body is a string in language.
func quotedRoutine(name, parameters, language, body string) catalog.Function {
	routine := styleRoutine(name, "", "integer", body)
	routine.Parameters, routine.Language = parameters, language
	return routine
}

// defaultCallingRoutine is b_fn(x integer DEFAULT a_fn()), whose body is a string
// that names nothing.
func defaultCallingRoutine() catalog.Function {
	routine := quotedRoutine("b_fn", "x integer DEFAULT a_fn()", "plpgsql", "begin return x; end")
	routine.IdentityArguments = new("x integer")
	return routine
}

// heldRoutinesPlan plans the removal of routines on dialect in style, with
// nothing else to change.
func heldRoutinesPlan(c *qt.C, dialect string, style planext.StatementStyle, routines ...catalog.Function) []string {
	c.Helper()
	runtime := must.Must(builtin.New())
	held := styleHeldWith(func(h *catalog.Database) { h.Functions = routines })
	diff, err := schemadiff.CompareWithDatabaseInfo(c.Context(), styleDeclared(), held,
		catalog.ServerInfo{Dialect: dialect, Schema: "public"}, nil, runtime)
	c.Assert(err, qt.IsNil)
	statements, err := planner.GenerateSchemaDiffSQLStatementsWithOptions(c.Context(), runtime, diff, dialect,
		planext.Options{Capabilities: capability.ForDialect(dialect), Statements: style})
	c.Assert(err, qt.IsNil)
	return statements
}

// TestRemovedRoutineOrder_HappyPath pins the order the server requires. A name
// sort drops a callee first, which PostgreSQL answers with `cannot drop function
// a_fn() because other objects depend on it`. The statements carry no guard in
// the exact style and no CASCADE in either.
func TestRemovedRoutineOrder_HappyPath(t *testing.T) {
	procedure := styleRoutine("z_proc", "", "", "BEGIN ATOMIC SELECT a_fn(); END")
	procedure.Kind = schemamodel.FunctionKindProcedure
	tests := []struct {
		name     string
		routines []catalog.Function
		exact    []string
	}{
		{
			name: "a RETURN body calls a function sorted ahead of it",
			routines: []catalog.Function{
				styleRoutine("a_fn", "", "integer", "RETURN 1"),
				styleRoutine("b_fn", "", "integer", "RETURN a_fn() + 1"),
			},
			exact: []string{`DROP FUNCTION "public"."b_fn"()`, `DROP FUNCTION "public"."a_fn"()`},
		},
		{
			name: "a schema-qualified call",
			routines: []catalog.Function{
				styleRoutine("a_fn", "", "integer", "RETURN 1"),
				styleRoutine("b_fn", "", "integer", "RETURN public.a_fn() + 1"),
			},
			exact: []string{`DROP FUNCTION "public"."b_fn"()`, `DROP FUNCTION "public"."a_fn"()`},
		},
		{
			name: "a chain in BEGIN ATOMIC",
			routines: []catalog.Function{
				styleRoutine("a_fn", "", "integer", "BEGIN ATOMIC SELECT 1; END"),
				styleRoutine("m_fn", "", "integer", "BEGIN ATOMIC SELECT a_fn(); END"),
				styleRoutine("z_fn", "", "integer", "BEGIN ATOMIC SELECT m_fn(); END"),
			},
			exact: []string{`DROP FUNCTION "public"."z_fn"()`, `DROP FUNCTION "public"."m_fn"()`, `DROP FUNCTION "public"."a_fn"()`},
		},
		{
			name: "a procedure calls a function",
			routines: []catalog.Function{
				styleRoutine("a_fn", "", "integer", "RETURN 1"),
				procedure,
			},
			exact: []string{`DROP PROCEDURE "public"."z_proc"()`, `DROP FUNCTION "public"."a_fn"()`},
		},
		{
			name: "a parameter default calls a function under a quoted body",
			routines: []catalog.Function{
				quotedRoutine("a_fn", "", "sql", "select 1"),
				defaultCallingRoutine(),
			},
			exact: []string{`DROP FUNCTION "public"."b_fn"(x integer)`, `DROP FUNCTION "public"."a_fn"()`},
		},
		{
			name: "functions that call each other go in one statement",
			routines: []catalog.Function{
				styleRoutine("a_fn", "", "integer", "RETURN b_fn() + 1"),
				styleRoutine("b_fn", "", "integer", "RETURN a_fn() + 1"),
				styleRoutine("c_fn", "", "integer", "RETURN a_fn() + 1"),
			},
			exact: []string{`DROP FUNCTION "public"."c_fn"()`, `DROP FUNCTION "public"."a_fn"(), "public"."b_fn"()`},
		},
		{
			name: "a quoted body keeps the sorted order, since PostgreSQL records nothing for it",
			routines: []catalog.Function{
				quotedRoutine("a_fn", "", "sql", "select 1"),
				quotedRoutine("b_fn", "", "sql", "select a_fn() + 1"),
			},
			exact: []string{`DROP FUNCTION "public"."a_fn"()`, `DROP FUNCTION "public"."b_fn"()`},
		},
		{
			name: "routines that call nothing keep the sorted order",
			routines: []catalog.Function{
				styleRoutine("a_fn", "", "integer", "RETURN 1"),
				styleRoutine("b_fn", "", "integer", "RETURN 2"),
			},
			exact: []string{`DROP FUNCTION "public"."a_fn"()`, `DROP FUNCTION "public"."b_fn"()`},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(heldRoutinesPlan(c, platform.Postgres, planext.StatementStyleExact, test.routines...), qt.DeepEquals, test.exact)
		})
	}
}

// TestRemovedRoutineOrder_TheAnnotatedStyleOrdersTheSame holds the other style
// to the order: IF EXISTS does not help a drop the server refuses for a caller.
func TestRemovedRoutineOrder_TheAnnotatedStyleOrdersTheSame(t *testing.T) {
	c := qt.New(t)

	got := heldRoutinesPlan(c, platform.Postgres, planext.StatementStyleAnnotated,
		styleRoutine("a_fn", "", "integer", "RETURN 1"),
		styleRoutine("b_fn", "", "integer", "RETURN a_fn() + 1"))

	c.Assert(got, qt.DeepEquals, []string{
		"-- WARNING: Ensure no other objects depend on this function\nDROP FUNCTION IF EXISTS \"public\".\"b_fn\"()",
		"-- WARNING: Ensure no other objects depend on this function\nDROP FUNCTION IF EXISTS \"public\".\"a_fn\"()",
	})
}

// TestRemovedRoutineOrder_OnCockroachDBAQuotedBodyHoldsItsCallee reads a body in
// quotes as a call on the engine that records one: CockroachDB answers a drop of
// the callee with `cannot drop function "a_fn" because other objects
// ([db.public.b_fn]) still depend on it`.
func TestRemovedRoutineOrder_OnCockroachDBAQuotedBodyHoldsItsCallee(t *testing.T) {
	for _, language := range []string{"sql", "plpgsql"} {
		t.Run(language, func(t *testing.T) {
			c := qt.New(t)

			got := heldRoutinesPlan(c, platform.CockroachDB, planext.StatementStyleExact,
				quotedRoutine("a_fn", "", language, "select 1"),
				quotedRoutine("b_fn", "", language, "select a_fn() + 1"))

			c.Assert(drops(got), qt.DeepEquals, []string{`DROP FUNCTION "public"."b_fn"()`, `DROP FUNCTION "public"."a_fn"()`})
		})
	}
}

// TestRemovedRoutineOrder_ADomainHoldsTheFunctionsItsRoutineCalls removes a
// domain whose CHECK calls b_fn, which calls a_fn. The domain goes before b_fn,
// so a_fn has to wait for the type step as well, ahead of which it would meet
// its caller.
func TestRemovedRoutineOrder_ADomainHoldsTheFunctionsItsRoutineCalls(t *testing.T) {
	c := qt.New(t)
	runtime := must.Must(builtin.New())
	declared := styleDeclared()
	declared.FeatureCoverage = must.Must(pgtype.CompleteCoverage(schemaext.Desired))
	held := styleHeldWith(func(h *catalog.Database) {
		h.Functions = []catalog.Function{
			styleRoutine("a_fn", "n integer", "boolean", "RETURN n > 0"),
			styleRoutine("b_fn", "n integer", "boolean", "RETURN a_fn(n)"),
		}
		h.FeatureObjects = must.Must(schemaext.NewObjects(schemaext.Object{
			Ref: pgtype.Ref(pgtype.DomainKind, "public", "d"), Value: &pgtype.ObservedDomain{BaseType: "integer", Check: "b_fn(VALUE)"},
		}))
		h.FeatureCoverage = must.Must(pgtype.CompleteCoverage(schemaext.Observed))
	})

	diff, err := schemadiff.CompareWithDatabaseInfo(c.Context(), declared, held,
		catalog.ServerInfo{Dialect: platform.Postgres, Schema: "public"}, nil, runtime)
	c.Assert(err, qt.IsNil)
	got, err := planner.GenerateSchemaDiffSQLStatementsWithOptions(c.Context(), runtime, diff, platform.Postgres,
		planext.Options{Capabilities: capability.ForDialect(platform.Postgres), Statements: planext.StatementStyleExact})

	c.Assert(err, qt.IsNil)
	c.Assert(got, qt.DeepEquals, []string{
		"-- WARNING: Make sure no columns use this domain!\nDROP DOMAIN \"public\".\"d\"",
		`DROP FUNCTION IF EXISTS "public"."b_fn"(n integer)`,
		`DROP FUNCTION IF EXISTS "public"."a_fn"(n integer)`,
	})
}
