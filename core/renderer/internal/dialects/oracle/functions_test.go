package oracle_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/renderer"
	"ptah.run/core/schemamodel"
)

// plsqlFunction is the declaration every case below starts from: the shape an
// annotation produces once schemamodel.Function.Canonicalize has run on it.
func plsqlFunction(name string) *ast.CreateFunctionNode {
	return ast.NewCreateFunction(name).
		SetParameters("p IN NUMBER").
		SetReturns("NUMBER").
		SetLanguage("plsql").
		SetSecurity("DEFINER").
		SetVolatility("VOLATILE").
		SetBody("BEGIN\n  RETURN p * 2;\nEND;")
}

// TestCreateFunction_RendersTheHeaderOracleStores pins the statement, and the
// clause order is a measurement rather than a guess.
//
// On 23.26.2.0.0 the header
//
//	FUNCTION fn_det(p IN NUMBER) RETURN NUMBER DETERMINISTIC AUTHID CURRENT_USER IS
//
// is what ALL_SOURCE reports back after creating a function written that way,
// so the order below is the order the server keeps (stokaro/ptah#1920).
func TestCreateFunction_RendersTheHeaderOracleStores(t *testing.T) {
	tests := []struct {
		name string
		node *ast.CreateFunctionNode
		want string
	}{
		{
			name: "a function",
			node: plsqlFunction("fn_double"),
			want: "CREATE OR REPLACE FUNCTION fn_double(p IN NUMBER) RETURN NUMBER AUTHID DEFINER IS\n" +
				"BEGIN\n  RETURN p * 2;\nEND;\n",
		},
		{
			name: "invoker rights",
			node: plsqlFunction("fn_double").SetSecurity("INVOKER"),
			want: "CREATE OR REPLACE FUNCTION fn_double(p IN NUMBER) RETURN NUMBER " +
				"AUTHID CURRENT_USER IS\nBEGIN\n  RETURN p * 2;\nEND;\n",
		},
		{
			name: "immutable, which is the DETERMINISTIC clause",
			node: plsqlFunction("fn_double").SetVolatility("IMMUTABLE"),
			want: "CREATE OR REPLACE FUNCTION fn_double(p IN NUMBER) RETURN NUMBER DETERMINISTIC " +
				"AUTHID DEFINER IS\nBEGIN\n  RETURN p * 2;\nEND;\n",
		},
		{
			name: "a parameterless function takes no parentheses",
			node: plsqlFunction("fn_one").SetParameters("").SetBody("BEGIN\n  RETURN 1;\nEND;"),
			want: "CREATE OR REPLACE FUNCTION fn_one RETURN NUMBER AUTHID DEFINER IS\n" +
				"BEGIN\n  RETURN 1;\nEND;\n",
		},
		{
			name: "a procedure has no RETURN clause",
			node: plsqlFunction("pr_touch").SetKind(schemamodel.FunctionKindProcedure).
				SetReturns("").SetBody("BEGIN\n  NULL;\nEND;"),
			want: "CREATE OR REPLACE PROCEDURE pr_touch(p IN NUMBER) AUTHID DEFINER IS\n" +
				"BEGIN\n  NULL;\nEND;\n",
		},
		{
			name: "a declaration section, which is body text like any other",
			node: plsqlFunction("fn_dec").SetBody("  x NUMBER := 0;\nBEGIN\n  RETURN x;\nEND;"),
			want: "CREATE OR REPLACE FUNCTION fn_dec(p IN NUMBER) RETURN NUMBER AUTHID DEFINER IS\n" +
				"x NUMBER := 0;\nBEGIN\n  RETURN x;\nEND;\n",
		},
		{
			name: "a comment",
			node: plsqlFunction("fn_double").SetComment("doubles it"),
			want: "-- doubles it\nCREATE OR REPLACE FUNCTION fn_double(p IN NUMBER) RETURN NUMBER " +
				"AUTHID DEFINER IS\nBEGIN\n  RETURN p * 2;\nEND;\n",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			out, err := renderer.RenderSQLWithCapabilities(platform.Oracle, capability.Oracle23(), test.node)

			c.Assert(err, qt.IsNil)
			c.Assert(out, qt.Equals, test.want)
		})
	}
}

// TestCreateFunction_TheHeaderIsTheSameOnBothLines states what the two presets
// do NOT differ about.
//
// PL/SQL is not a 23 feature. The header, ALL_PROCEDURES, ALL_ARGUMENTS and
// ALL_SOURCE answered identically on 21.3.0.0.0 and on 23.26.2.0.0, and the
// only Oracle key that separates the two lines for a routine is the existence
// guard on the DROP.
func TestCreateFunction_TheHeaderIsTheSameOnBothLines(t *testing.T) {
	c := qt.New(t)
	want := "CREATE OR REPLACE FUNCTION fn_double(p IN NUMBER) RETURN NUMBER AUTHID DEFINER IS\n" +
		"BEGIN\n  RETURN p * 2;\nEND;\n"

	on23, err := renderer.RenderSQLWithCapabilities(platform.Oracle, capability.Oracle23(), plsqlFunction("fn_double"))
	c.Assert(err, qt.IsNil)
	c.Assert(on23, qt.Equals, want)

	on21, err := renderer.RenderSQLWithCapabilities(platform.Oracle, capability.Oracle21(), plsqlFunction("fn_double"))
	c.Assert(err, qt.IsNil)
	c.Assert(on21, qt.Equals, want)
}

// TestCreateFunction_SkipsALanguageThisTargetDoesNotRun keeps a cross-dialect
// schema working, and says which declaration was left alone.
//
// plpgsql is the value that actually arrives: Canonicalize defaults an
// annotation without `language=` to it, so an ordinary declaration reaches this
// branch rather than an exotic one.
func TestCreateFunction_SkipsALanguageThisTargetDoesNotRun(t *testing.T) {
	c := qt.New(t)
	out, err := renderer.RenderSQLWithCapabilities(platform.Oracle, capability.Oracle23(),
		plsqlFunction("fn_double").SetLanguage("plpgsql"))

	c.Assert(err, qt.IsNil)
	c.Assert(out, qt.Contains, "declares language plpgsql, which this target does not run; skipped")
	c.Assert(out, qt.Contains, `declare language="plsql"`)
	c.Assert(out, qt.Not(qt.Contains), "CREATE OR REPLACE")
}

// TestCreateFunction_NamesAParameterDefaultAndCreatesNothing pins the refusal
// whose reason is the catalog rather than the grammar.
//
// Oracle accepts the statement. ALL_ARGUMENTS then reports DEFAULTED = 'Y' and
// never the value, so the routine would be reported as differing from its own
// declaration on every run.
func TestCreateFunction_NamesAParameterDefaultAndCreatesNothing(t *testing.T) {
	tests := []struct {
		name       string
		parameters string
	}{
		{name: "the DEFAULT keyword", parameters: "p IN NUMBER DEFAULT 1"},
		{name: "the assignment spelling", parameters: "p IN NUMBER := 1"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			out, err := renderer.RenderSQLWithCapabilities(platform.Oracle, capability.Oracle23(),
				plsqlFunction("fn_default").SetParameters(test.parameters))

			c.Assert(err, qt.IsNil)
			c.Assert(out, qt.Contains, "declares a parameter default")
			c.Assert(out, qt.Not(qt.Contains), "CREATE OR REPLACE")
		})
	}
}

// TestCreateFunction_RefusesSTABLEBecauseOracleCannotReportItBack is the one
// declared property this target has no cell for.
//
// The refusal is an error rather than a skip comment, because the two available
// answers are both wrong in a way the operator would not see: DETERMINISTIC is
// a promise a function-based index may be built on, and no clause reads back as
// VOLATILE and plans the same replacement forever.
func TestCreateFunction_RefusesSTABLEBecauseOracleCannotReportItBack(t *testing.T) {
	c := qt.New(t)
	out, err := renderer.RenderSQLWithCapabilities(platform.Oracle, capability.Oracle23(),
		plsqlFunction("fn_stable").SetVolatility("STABLE"))

	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Contains, "STABLE")
	c.Assert(out, qt.Equals, "")
}

// TestCreateFunction_RefusesASecurityModeItCannotWrite keeps a typo from
// silently getting definer rights.
func TestCreateFunction_RefusesASecurityModeItCannotWrite(t *testing.T) {
	c := qt.New(t)
	_, err := renderer.RenderSQLWithCapabilities(platform.Oracle, capability.Oracle23(),
		plsqlFunction("fn_typo").SetSecurity("INVKOER"))

	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Contains, "INVKOER")
}

// TestRoutines_FollowTheirOwnCapabilityKeys pins that the two keys are separate
// gates on one statement pair.
//
// A target could host one and not the other, and refusing a procedure under the
// function key would say the wrong thing about which one is missing.
func TestRoutines_FollowTheirOwnCapabilityKeys(t *testing.T) {
	tests := []struct {
		name string
		caps capability.Capabilities
		node ast.Node
		want string
	}{
		{
			name: "a function without the function key",
			caps: capability.Oracle23().With(capability.Functions, false),
			node: plsqlFunction("fn_double"),
			want: "-- ORACLE: CREATE FUNCTION \"fn_double\" is not supported\n",
		},
		{
			name: "a procedure keeps its own key when functions are off",
			caps: capability.Oracle23().With(capability.Functions, false),
			node: plsqlFunction("pr_touch").SetKind(schemamodel.FunctionKindProcedure).
				SetReturns("").SetBody("BEGIN\n  NULL;\nEND;"),
			want: "CREATE OR REPLACE PROCEDURE pr_touch(p IN NUMBER) AUTHID DEFINER IS\n" +
				"BEGIN\n  NULL;\nEND;\n",
		},
		{
			name: "a procedure without the procedure key",
			caps: capability.Oracle23().With(capability.Procedures, false),
			node: plsqlFunction("pr_touch").SetKind(schemamodel.FunctionKindProcedure).
				SetReturns("").SetBody("BEGIN\n  NULL;\nEND;"),
			want: "-- ORACLE: CREATE PROCEDURE \"pr_touch\" is not supported\n",
		},
		{
			name: "a function keeps its own key when procedures are off",
			caps: capability.Oracle23().With(capability.Procedures, false),
			node: plsqlFunction("fn_double"),
			want: "CREATE OR REPLACE FUNCTION fn_double(p IN NUMBER) RETURN NUMBER AUTHID DEFINER IS\n" +
				"BEGIN\n  RETURN p * 2;\nEND;\n",
		},
		{
			name: "dropping a function without the function key",
			caps: capability.Oracle23().With(capability.Functions, false),
			node: ast.NewDropFunction("fn_double"),
			want: "-- ORACLE: DROP FUNCTION \"fn_double\" is not supported\n",
		},
		{
			name: "dropping a procedure without the procedure key",
			caps: capability.Oracle23().With(capability.Procedures, false),
			node: ast.NewDropFunction("pr_touch").SetKind(schemamodel.FunctionKindProcedure),
			want: "-- ORACLE: DROP PROCEDURE \"pr_touch\" is not supported\n",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			out, err := renderer.RenderSQLWithCapabilities(platform.Oracle, test.caps, test.node)

			c.Assert(err, qt.IsNil)
			c.Assert(out, qt.Equals, test.want)
		})
	}
}

// TestDropFunction_NamesTheObjectItIsDropping pins the verb and the guard.
//
// The verb has to match: `DROP FUNCTION p` on a procedure is ORA-04043, so a
// drop that guessed would fail and leave the routine in place. The guard
// follows the line, because `DROP FUNCTION IF EXISTS` is accepted on 23 and is
// ORA-00933 on 21.
func TestDropFunction_NamesTheObjectItIsDropping(t *testing.T) {
	tests := []struct {
		name string
		caps capability.Capabilities
		node *ast.DropFunctionNode
		want string
	}{
		{
			name: "a function",
			caps: capability.Oracle23(),
			node: ast.NewDropFunction("fn_double"),
			want: "DROP FUNCTION fn_double;\n",
		},
		{
			name: "a procedure",
			caps: capability.Oracle23(),
			node: ast.NewDropFunction("pr_touch").SetKind(schemamodel.FunctionKindProcedure),
			want: "DROP PROCEDURE pr_touch;\n",
		},
		{
			name: "guarded on 23",
			caps: capability.Oracle23(),
			node: ast.NewDropFunction("fn_double").SetIfExists(),
			want: "DROP FUNCTION IF EXISTS fn_double;\n",
		},
		{
			name: "unguarded on 21, where the clause is ORA-00933",
			caps: capability.Oracle21(),
			node: ast.NewDropFunction("fn_double").SetIfExists(),
			want: "DROP FUNCTION fn_double;\n",
		},
		{
			name: "a comment",
			caps: capability.Oracle23(),
			node: ast.NewDropFunction("fn_double").SetComment("no longer declared"),
			want: "-- no longer declared\nDROP FUNCTION fn_double;\n",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			out, err := renderer.RenderSQLWithCapabilities(platform.Oracle, test.caps, test.node)

			c.Assert(err, qt.IsNil)
			c.Assert(out, qt.Equals, test.want)
		})
	}
}
