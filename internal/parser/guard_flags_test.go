package parser_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/internal/parser"
)

// The parser records the guard a routine's CREATE writes, OR REPLACE or SQL
// Server's OR ALTER, which the reader needs to run a repeat as the server
// does: the later body replaces the earlier one.
func TestParser_RecordsWhetherARoutineReplaces(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		dialect string
		replace bool
	}{
		{name: "OR REPLACE PROCEDURE", sql: "CREATE OR REPLACE PROCEDURE p() LANGUAGE sql AS $$ SELECT 1 $$;", dialect: platform.Postgres, replace: true},
		{name: "a plain PROCEDURE", sql: "CREATE PROCEDURE p() LANGUAGE sql AS $$ SELECT 1 $$;", dialect: platform.Postgres},
		{name: "OR ALTER FUNCTION", sql: "CREATE OR ALTER FUNCTION dbo.f() RETURNS int AS BEGIN RETURN 1; END", dialect: platform.SQLServer, replace: true},
		{name: "OR ALTER PROCEDURE", sql: "CREATE OR ALTER PROCEDURE dbo.p AS BEGIN SELECT 1; END", dialect: platform.SQLServer, replace: true},
		{name: "a plain SQL Server PROCEDURE", sql: "CREATE PROCEDURE dbo.p AS BEGIN SELECT 1; END", dialect: platform.SQLServer},
		{name: "OR REPLACE DEFINER FUNCTION", sql: "CREATE OR REPLACE DEFINER=`root`@`localhost` FUNCTION f() RETURNS int RETURN 1;", dialect: platform.MariaDB, replace: true},
		{name: "a DEFINER FUNCTION", sql: "CREATE DEFINER=`root`@`localhost` FUNCTION f() RETURNS int RETURN 1;", dialect: platform.MariaDB},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			statements, err := parser.NewParser(test.sql, parser.WithDialect(test.dialect)).Parse()

			c.Assert(err, qt.IsNil)
			c.Assert(statements.Statements, qt.HasLen, 1)
			routine, ok := statements.Statements[0].(*ast.RoutineNode)
			c.Assert(ok, qt.IsTrue, qt.Commentf("%T", statements.Statements[0]))
			c.Assert(routine.Replace, qt.Equals, test.replace)
		})
	}
}

// The parser records CREATE MATERIALIZED VIEW IF NOT EXISTS, which the server
// skips when the name is in use.
func TestParser_RecordsAMaterializedViewGuard(t *testing.T) {
	tests := []struct {
		name        string
		sql         string
		ifNotExists bool
	}{
		{name: "IF NOT EXISTS", sql: "CREATE MATERIALIZED VIEW IF NOT EXISTS mv AS SELECT 1 AS a;", ifNotExists: true},
		{name: "no guard", sql: "CREATE MATERIALIZED VIEW mv AS SELECT 1 AS a;"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			statements, err := parser.NewParser(test.sql, parser.WithDialect(platform.Postgres)).Parse()

			c.Assert(err, qt.IsNil)
			c.Assert(statements.Statements, qt.HasLen, 1)
			view, ok := statements.Statements[0].(*ast.CreateMaterializedViewNode)
			c.Assert(ok, qt.IsTrue, qt.Commentf("%T", statements.Statements[0]))
			c.Assert(view.Name, qt.Equals, "mv")
			c.Assert(view.IfNotExists, qt.Equals, test.ifNotExists)
		})
	}
}
