package parser_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/internal/parser"
)

func TestParser_ViewSQLSecurity(t *testing.T) {
	for _, test := range []struct {
		name, dialect, sql, security string
		replace                      bool
	}{
		{"MySQL definer", platform.MySQL, "CREATE SQL SECURITY DEFINER VIEW v AS SELECT 1 AS x;", "DEFINER", false},
		{"MariaDB invoker replacement", platform.MariaDB, "create or replace sql /* mode */ security invoker view v as select 1 as x;", "INVOKER", true},
		{"no dialect", "", "CREATE SQL SECURITY INVOKER VIEW v AS SELECT 1 AS x;", "INVOKER", false},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			statements, err := parser.NewParser(test.sql, parser.WithDialect(test.dialect)).Parse()
			c.Assert(err, qt.IsNil)
			c.Assert(statements.Statements, qt.HasLen, 1)
			view, ok := statements.Statements[0].(*ast.CreateViewNode)
			c.Assert(ok, qt.IsTrue)
			c.Assert(view.Security, qt.Equals, test.security)
			c.Assert(view.Replace, qt.Equals, test.replace)
		})
	}
}

func TestParser_ViewSQLSecurityRefusals(t *testing.T) {
	for _, test := range []struct{ name, dialect, sql, message string }{
		{"unsupported engine", platform.Postgres, "CREATE SQL SECURITY INVOKER VIEW v AS SELECT 1;", `(?s).*unsupported CREATE VIEW SQL SECURITY for postgres.*`},
		{"invalid mode", platform.MySQL, "CREATE SQL SECURITY OWNER VIEW v AS SELECT 1;", `(?s).*expected DEFINER or INVOKER after SQL SECURITY.*`},
		{"missing view", platform.MySQL, "CREATE SQL SECURITY INVOKER TABLE t (id int);", `(?s).*expected 'VIEW', got 'TABLE'.*`},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := parser.NewParser(test.sql, parser.WithDialect(test.dialect)).Parse()
			qt.New(t).Assert(err, qt.ErrorMatches, test.message)
		})
	}
}
