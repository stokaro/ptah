package renderer_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/atlascompat"
	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/core/renderer"
)

// firstLine renders nodes for PostgreSQL and returns the first line, which is
// where a routine's verb is.
func firstLine(c *qt.C, nodes ...ast.Node) string {
	c.Helper()
	sql, err := renderer.RenderSQL(platform.Postgres, nodes...)
	c.Assert(err, qt.IsNil)
	line, _, _ := strings.Cut(sql, "\n")
	return line
}

// A routine is created with a plain CREATE and replaced with CREATE OR
// REPLACE, so a plan says which of the two it does. A plain CREATE of a routine
// that exists fails with SQLSTATE 42723 rather than overwriting it, measured on
// PostgreSQL 18.6. A trigger's own function follows the trigger.
func TestRenderSQL_PostgreSQLRoutineVerbFollowsReplace(t *testing.T) {
	function := func() *ast.CreateFunctionNode {
		return ast.NewCreateFunction("add_one").
			SetParameters("n integer").
			SetReturns("integer").
			SetLanguage("sql").
			SetBody("SELECT n + 1")
	}
	procedure := func() *ast.CreateFunctionNode {
		return ast.NewCreateFunction("reset").SetKind("procedure").SetLanguage("sql").SetBody("SELECT 1")
	}
	trigger := func() *ast.CreateTriggerNode {
		return ast.NewCreateTrigger("touch", "users").SetTiming("BEFORE").SetEvent("UPDATE").SetBody("RETURN NEW;")
	}
	tests := []struct {
		name string
		node ast.Node
		want string
	}{
		{name: "a new function", node: function(), want: `CREATE FUNCTION "add_one"(n integer) RETURNS integer AS $$`},
		{
			name: "a replaced function",
			node: function().SetReplace(),
			want: `CREATE OR REPLACE FUNCTION "add_one"(n integer) RETURNS integer AS $$`,
		},
		{name: "a new procedure", node: procedure(), want: `CREATE PROCEDURE "reset"() AS $$`},
		{name: "a replaced procedure", node: procedure().SetReplace(), want: `CREATE OR REPLACE PROCEDURE "reset"() AS $$`},
		{name: "a new trigger's function", node: trigger(), want: `CREATE FUNCTION "ptah_trigger_users_touch"()`},
		{
			name: "a replaced trigger's function",
			node: trigger().SetReplace(),
			want: `CREATE OR REPLACE FUNCTION "ptah_trigger_users_touch"()`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(firstLine(c, test.node), qt.Equals, test.want)
		})
	}
}

// A routine parsed from SQL keeps the verb it was written with. Without that,
// CREATE OR REPLACE read back through the parser renders as a plain CREATE,
// which fails on the routine it was written to replace. The parser keeps a
// procedure as the text it read, so the verb survives there by construction.
func TestParseSQL_PostgreSQLRoutineKeepsItsVerb(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want string
	}{
		{
			name: "CREATE FUNCTION",
			sql:  `CREATE FUNCTION add_one(n integer) RETURNS integer LANGUAGE sql AS $$ SELECT n + 1 $$;`,
			want: `CREATE FUNCTION "?add_one"?\(n integer\).*`,
		},
		{
			name: "CREATE OR REPLACE FUNCTION",
			sql:  `CREATE OR REPLACE FUNCTION add_one(n integer) RETURNS integer LANGUAGE sql AS $$ SELECT n + 1 $$;`,
			want: `CREATE OR REPLACE FUNCTION "?add_one"?\(n integer\).*`,
		},
		{
			name: "CREATE OR REPLACE PROCEDURE",
			sql:  `CREATE OR REPLACE PROCEDURE reset() LANGUAGE sql AS $$ SELECT 1 $$;`,
			want: `CREATE OR REPLACE PROCEDURE "?reset"?\(\).*`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			parsed, err := atlascompat.ParseSQL(test.sql, atlascompat.ParseSQLOptions{Dialect: platform.Postgres})

			c.Assert(err, qt.IsNil)
			c.Assert(firstLine(c, parsed.Statements...), qt.Matches, test.want)
		})
	}
}
