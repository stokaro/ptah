package parser_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/internal/parser"
)

func TestParser_ViewSQLSecurityAfterName(t *testing.T) {
	for _, test := range []struct {
		name, dialect, sql, security string
	}{
		{"Spanner invoker", "spanner", "CREATE VIEW v SQL SECURITY INVOKER AS SELECT 1;", "INVOKER"},
		{"unclassified definer", "", "CREATE OR REPLACE VIEW v SQL SECURITY DEFINER AS SELECT 1;", "DEFINER"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			statements, err := parser.NewParser(test.sql, parser.WithDialect(test.dialect)).Parse()
			c.Assert(err, qt.IsNil)
			c.Assert(statements.Statements, qt.HasLen, 1)
			view, ok := statements.Statements[0].(*ast.CreateViewNode)
			c.Assert(ok, qt.IsTrue)
			c.Assert(view.Security, qt.Equals, test.security)
		})
	}
}

func TestParser_ViewSQLSecurityAfterNameRefusals(t *testing.T) {
	for _, test := range []struct{ name, dialect, sql, message string }{
		{"built-in dialect", platform.Postgres, "CREATE VIEW v SQL SECURITY INVOKER AS SELECT 1;", `(?s).*unsupported CREATE VIEW clause: SQL SECURITY.*`},
		{"unknown mode", "spanner", "CREATE VIEW v SQL SECURITY OWNER AS SELECT 1;", `(?s).*expected DEFINER or INVOKER after SQL SECURITY.*`},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := parser.NewParser(test.sql, parser.WithDialect(test.dialect)).Parse()
			qt.New(t).Assert(err, qt.ErrorMatches, test.message)
		})
	}
}
