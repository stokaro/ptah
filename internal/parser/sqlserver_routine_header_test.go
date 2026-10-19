package parser_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/internal/parser"
)

// sqlServerRoutineHeader is what a SQL Server routine's header is read as.
type sqlServerRoutineHeader struct {
	Name, Parameters, Returns, Body string
	Options                         []string
}

// sqlServerRoutineOf parses one SQL Server routine and returns its header.
func sqlServerRoutineOf(c *qt.C, sql string) sqlServerRoutineHeader {
	c.Helper()
	statements, err := parser.NewParser(sql, parser.WithDialect("sqlserver")).Parse()
	c.Assert(err, qt.IsNil)
	c.Assert(statements.Statements, qt.HasLen, 1)
	routine, ok := statements.Statements[0].(*ast.RoutineNode)
	c.Assert(ok, qt.IsTrue)
	var options []string
	for _, option := range routine.Options {
		options = append(options, option.Text)
	}
	return sqlServerRoutineHeader{
		Name: routine.Name, Parameters: routine.Parameters, Returns: routine.Returns,
		Body: routine.Body.SQL, Options: options,
	}
}

// TestParse_ASQLServerRoutineHeaderEndsWhereItsBodyBegins pins the AS a SQL
// Server routine's body opens with, and the options its header writes.
//
// The AS of `WITH EXECUTE AS OWNER` comes first. Taken for the
// body's, it would name a procedure `po WITH EXECUTE` and give it the body
// `OWNER AS BEGIN SELECT 1; END`, and make a function's result type `INT WITH
// EXECUTE` (stokaro/ptah#4490). A parameter's optional AS, as in `@a AS INT`,
// is not the body's either.
// Measured on SQL Server 2025 RTM-CU9, every statement here is accepted.
func TestParse_ASQLServerRoutineHeaderEndsWhereItsBodyBegins(t *testing.T) {
	for _, test := range []struct {
		name string
		sql  string
		want sqlServerRoutineHeader
	}{
		{name: "a procedure that runs as its owner",
			sql:  "CREATE PROCEDURE dbo.po WITH EXECUTE AS OWNER AS BEGIN SELECT 1; END",
			want: sqlServerRoutineHeader{Name: "dbo.po", Body: "BEGIN SELECT 1; END", Options: []string{"EXECUTE AS OWNER"}}},
		{name: "a procedure with parameters in parentheses",
			sql:  "CREATE PROCEDURE dbo.pp (@a INT) WITH EXECUTE AS OWNER AS SELECT @a AS a",
			want: sqlServerRoutineHeader{Name: "dbo.pp", Parameters: "@a INT", Body: "SELECT @a AS a", Options: []string{"EXECUTE AS OWNER"}}},
		{name: "a parameter whose type follows AS",
			sql:  "CREATE PROCEDURE dbo.pa @a AS INT AS SELECT @a AS a",
			want: sqlServerRoutineHeader{Name: "dbo.pa", Parameters: "@a AS INT", Body: "SELECT @a AS a"}},
		{name: "two options and a principal",
			sql: "CREATE PROCEDURE dbo.pu @a INT = 1 WITH RECOMPILE, EXECUTE AS 'probe_user' AS SELECT @a AS a",
			want: sqlServerRoutineHeader{Name: "dbo.pu", Parameters: "@a INT = 1", Body: "SELECT @a AS a",
				Options: []string{"RECOMPILE", "EXECUTE AS 'probe_user'"}}},
		{name: "for replication",
			sql:  "CREATE PROCEDURE dbo.pr FOR REPLICATION AS SELECT 1 AS a",
			want: sqlServerRoutineHeader{Name: "dbo.pr", Body: "SELECT 1 AS a", Options: []string{"FOR REPLICATION"}}},
		{name: "lower case",
			sql:  "create procedure dbo.pl with execute as owner as begin select 1; end",
			want: sqlServerRoutineHeader{Name: "dbo.pl", Body: "begin select 1; end", Options: []string{"EXECUTE AS OWNER"}}},
		{name: "a scalar function that runs as its owner",
			sql:  "CREATE FUNCTION dbo.fo() RETURNS INT WITH EXECUTE AS OWNER AS BEGIN RETURN 1; END",
			want: sqlServerRoutineHeader{Name: "dbo.fo", Returns: "INT", Body: "BEGIN RETURN 1; END", Options: []string{"EXECUTE AS OWNER"}}},
		{name: "a schema-bound function",
			sql:  "CREATE FUNCTION dbo.fs() RETURNS INT WITH SCHEMABINDING AS BEGIN RETURN 1; END",
			want: sqlServerRoutineHeader{Name: "dbo.fs", Returns: "INT", Body: "BEGIN RETURN 1; END", Options: []string{"SCHEMABINDING"}}},
		{name: "a table-valued function",
			sql: "CREATE FUNCTION dbo.ft() RETURNS @t TABLE (c INT) WITH EXECUTE AS OWNER AS BEGIN INSERT @t VALUES (1); RETURN; END",
			want: sqlServerRoutineHeader{Name: "dbo.ft", Returns: "@t TABLE (c INT)", Body: "BEGIN INSERT @t VALUES (1); RETURN; END",
				Options: []string{"EXECUTE AS OWNER"}}},
		{name: "a function without a body AS",
			sql:  "CREATE FUNCTION dbo.f() RETURNS INT WITH EXECUTE AS OWNER BEGIN RETURN 1; END",
			want: sqlServerRoutineHeader{Name: "dbo.f", Returns: "INT", Body: "BEGIN RETURN 1; END", Options: []string{"EXECUTE AS OWNER"}}},
		{name: "an inline table function without a body AS",
			sql:  "CREATE FUNCTION dbo.f() RETURNS TABLE WITH SCHEMABINDING RETURN SELECT 1 AS id",
			want: sqlServerRoutineHeader{Name: "dbo.f", Returns: "TABLE", Body: "RETURN SELECT 1 AS id", Options: []string{"SCHEMABINDING"}}},
		{name: "a function without options or a body AS",
			sql:  "CREATE FUNCTION dbo.f() RETURNS INT BEGIN RETURN 1; END",
			want: sqlServerRoutineHeader{Name: "dbo.f", Returns: "INT", Body: "BEGIN RETURN 1; END"}},
		{name: "a return variable named begin",
			sql:  "CREATE FUNCTION dbo.f() RETURNS @begin TABLE (id INT) WITH EXECUTE AS OWNER BEGIN INSERT @begin VALUES (1); RETURN; END",
			want: sqlServerRoutineHeader{Name: "dbo.f", Returns: "@begin TABLE (id INT)", Body: "BEGIN INSERT @begin VALUES (1); RETURN; END", Options: []string{"EXECUTE AS OWNER"}}},
		{name: "no options",
			sql:  "CREATE PROCEDURE dbo.pn AS BEGIN SELECT 1; END",
			want: sqlServerRoutineHeader{Name: "dbo.pn", Body: "BEGIN SELECT 1; END"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got := sqlServerRoutineOf(c, test.sql)

			c.Assert(got, qt.DeepEquals, test.want)
		})
	}
}

// Empty WITH entries must not disappear when the routine is rendered.
func TestParseSQLServerRoutine_EmptyOptions(t *testing.T) {
	for _, options := range []string{"", ", RECOMPILE", "RECOMPILE,", "RECOMPILE,, EXECUTE AS OWNER", "/* empty */"} {
		t.Run(options, func(t *testing.T) {
			c := qt.New(t)
			_, err := parser.NewParser("CREATE PROCEDURE dbo.p WITH "+options+" AS BEGIN SELECT 1; END", parser.WithDialect("sqlserver")).Parse()
			c.Assert(err, qt.ErrorMatches, `.*WITH list contains an empty option.*`)
		})
	}
}
