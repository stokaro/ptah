package parser_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/ast"
	"go.5x5.cz/ptah/internal/parser"
)

// TestParse_RoutineSettings pins that a routine header may carry SET.
//
// It could not. All three spellings gave `unsupported CREATE FUNCTION clause:
// SET`, so following the advice of PRV02 -- pin the search_path -- made the
// statement unparsable, and migration lint drops a statement it cannot model:
// the function left the change model entirely and the run still exited 0
// (stokaro/ptah#2356).
func TestParse_RoutineSettings(t *testing.T) {
	rows := []struct {
		name string
		sql  string
		want []string
	}{
		{
			name: "name = value",
			sql:  "CREATE FUNCTION f() RETURNS int LANGUAGE sql SET search_path = pg_catalog AS $$ SELECT 1 $$;",
			want: []string{"search_path=pg_catalog"},
		},
		{
			// A catalog reports one spelling for both, so both fold onto it.
			name: "name TO value",
			sql:  "CREATE FUNCTION f() RETURNS int LANGUAGE sql SET search_path TO pg_catalog AS $$ SELECT 1 $$;",
			want: []string{"search_path=pg_catalog"},
		},
		{
			// Keeps its own words: the server resolves it when the routine is
			// defined, so no declared form can equal what the catalog reports.
			name: "name FROM CURRENT",
			sql:  "CREATE FUNCTION f() RETURNS int LANGUAGE sql SET search_path FROM CURRENT AS $$ SELECT 1 $$;",
			want: []string{"search_path FROM CURRENT"},
		},
		{
			// A value is comma-separated, so it runs to the next clause rather
			// than to the next token.
			name: "a list value",
			sql:  "CREATE FUNCTION f() RETURNS int LANGUAGE sql SET search_path = pg_catalog, pg_temp AS $$ SELECT 1 $$;",
			want: []string{"search_path=pg_catalog,pg_temp"},
		},
		{
			name: "two settings",
			sql:  "CREATE FUNCTION f() RETURNS int LANGUAGE sql SET search_path = pg_catalog SET work_mem = '4MB' AS $$ SELECT 1 $$;",
			want: []string{"search_path=pg_catalog", "work_mem='4MB'"},
		},
		{
			// The control: a routine that sets nothing carries nothing.
			name: "no setting",
			sql:  "CREATE FUNCTION f() RETURNS int LANGUAGE sql AS $$ SELECT 1 $$;",
			want: nil,
		},
	}

	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)

			statements, err := parser.NewParser(row.sql, parser.WithDialect("postgres")).Parse()

			c.Assert(err, qt.IsNil)
			c.Assert(functionOf(c, statements).Settings, qt.DeepEquals, row.want)
		})
	}
}

// functionOf returns the single CREATE FUNCTION a parse produced.
func functionOf(c *qt.C, statements *ast.StatementList) *ast.CreateFunctionNode {
	for _, node := range statements.Statements {
		if function, ok := node.(*ast.CreateFunctionNode); ok {
			return function
		}
	}
	c.Fatalf("no CREATE FUNCTION in %#v", statements.Statements)
	return nil
}
