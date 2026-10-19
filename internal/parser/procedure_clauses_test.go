package parser_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/internal/parser"
)

// procedureOf returns the single PostgreSQL CREATE PROCEDURE a parse produced.
func procedureOf(c *qt.C, statements *ast.StatementList) *ast.RoutineNode {
	c.Helper()
	for _, node := range statements.Statements {
		if routine, ok := node.(*ast.RoutineNode); ok {
			return routine
		}
	}
	c.Fatalf("no CREATE PROCEDURE in %#v", statements.Statements)
	return nil
}

// TestParse_AProcedureCarriesTheClausesAFunctionDoes pins that a PostgreSQL
// procedure keeps its SECURITY and SET clauses, read as a function's are.
//
// A procedure is parsed apart from a function, and that path read only the
// name, the parameters, the language and the body. A procedure declared
// SECURITY DEFINER became one that runs with its caller's rights, and the
// search_path pinned on it was dropped. The two paths read one grammar, so
// each row parses the same header as a function too, and the two must agree:
// a fix to either that the other does not get reddens here.
func TestParse_AProcedureCarriesTheClausesAFunctionDoes(t *testing.T) {
	for _, test := range []struct {
		name         string
		header       string
		wantSecurity string
		wantSettings []string
	}{
		{name: "definer", header: "SECURITY DEFINER", wantSecurity: "DEFINER"},
		{name: "invoker, in lower case", header: "security invoker", wantSecurity: "INVOKER"},
		{name: "the standard spelling", header: "EXTERNAL SECURITY DEFINER", wantSecurity: "DEFINER"},
		{name: "a list value", header: "SECURITY DEFINER SET search_path = pg_catalog, pg_temp",
			wantSecurity: "DEFINER", wantSettings: []string{"search_path=pg_catalog,pg_temp"}},
		{name: "name TO value", header: "SET search_path TO pg_catalog", wantSettings: []string{"search_path=pg_catalog"}},
		{name: "name FROM CURRENT before the mode", header: "SET timezone FROM CURRENT SECURITY DEFINER",
			wantSecurity: "DEFINER", wantSettings: []string{"timezone FROM CURRENT"}},
		{name: "two settings", header: "SET work_mem = '4MB' SET search_path = public",
			wantSettings: []string{"work_mem='4MB'", "search_path=public"}},
		{name: "neither", header: ""},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			const body = " AS $$ BEGIN NULL; END; $$;"

			procedures, err := parser.NewParser("CREATE PROCEDURE p() LANGUAGE plpgsql "+test.header+body,
				parser.WithDialect("postgres")).Parse()
			c.Assert(err, qt.IsNil)
			functions, err := parser.NewParser("CREATE FUNCTION f() RETURNS void LANGUAGE plpgsql "+test.header+body,
				parser.WithDialect("postgres")).Parse()
			c.Assert(err, qt.IsNil)

			procedure, function := procedureOf(c, procedures), functionOf(c, functions)
			c.Assert(procedure.Security, qt.Equals, test.wantSecurity)
			c.Assert(procedure.Settings, qt.DeepEquals, test.wantSettings)
			c.Assert(procedure.Security, qt.Equals, function.Security)
			c.Assert(procedure.Settings, qt.DeepEquals, function.Settings)
		})
	}
}

// TestParse_AProcedureBodyIsNotReadAsAClause keeps the clause reading to the
// header: a body written without quotes holds SET as SQL, not as a setting.
func TestParse_AProcedureBodyIsNotReadAsAClause(t *testing.T) {
	c := qt.New(t)

	statements, err := parser.NewParser("CREATE PROCEDURE p() LANGUAGE sql BEGIN ATOMIC UPDATE t SET c = 1; END;",
		parser.WithDialect("postgres")).Parse()

	c.Assert(err, qt.IsNil)
	c.Assert(procedureOf(c, statements).Settings, qt.IsNil)
	c.Assert(procedureOf(c, statements).Security, qt.Equals, "")
}

// A clause the server would refuse is refused here, with the words that are
// missing, rather than read without them.
func TestParse_AProcedureCarriesTheClausesAFunctionDoes_FailurePath(t *testing.T) {
	for _, test := range []struct {
		name    string
		header  string
		wantErr string
	}{
		{name: "a setting with no value", header: "SET search_path =", wantErr: `.*expected a value for the search_path routine setting.*`},
		{name: "a setting with no operator", header: "SET search_path pg_catalog", wantErr: `.*expected =, TO or FROM CURRENT in the search_path routine setting.*`},
		{name: "FROM without CURRENT", header: "SET timezone FROM LOCAL", wantErr: `.*expected CURRENT after FROM in the timezone routine setting.*`},
		{name: "EXTERNAL alone", header: "EXTERNAL DEFINER", wantErr: `.*expected SECURITY after EXTERNAL.*`},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			statements, err := parser.NewParser("CREATE PROCEDURE p() LANGUAGE plpgsql "+test.header+" AS $$ BEGIN NULL; END; $$;",
				parser.WithDialect("postgres")).Parse()

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(statements, qt.IsNil)
		})
	}
}
