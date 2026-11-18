package parser_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/internal/builtintest"
	"ptah.run/internal/parser"
)

// declaredColumn returns the column a single CREATE TABLE declares after its
// id, read as PostgreSQL.
func declaredColumn(c *qt.C, column string) *ast.ColumnNode {
	c.Helper()
	statements := parsePostgres(c, "CREATE TABLE t (id int PRIMARY KEY, "+column+");")
	table, ok := statements.Statements[0].(*ast.CreateTableNode)
	c.Assert(ok, qt.IsTrue, qt.Commentf("got %T", statements.Statements[0]))
	c.Assert(table.Columns, qt.HasLen, 2)
	return table.Columns[1]
}

// A default written as a parenthesized expression is the form pg_get_expr
// prints and pg_dump writes. It is read whole, with the cast or the operator
// after the group, and the expression ends where the column's next clause
// begins.
func TestParse_ParenthesizedColumnDefault_HappyPath(t *testing.T) {
	rows := []struct {
		name           string
		column         string
		wantExpression string
		wantNullable   bool
	}{
		{
			name:           "the form pg_get_expr prints",
			column:         "tag text DEFAULT ('X-'::text || (gen_random_uuid())::text)",
			wantExpression: "('X-'::text || (gen_random_uuid())::text)",
			wantNullable:   true,
		},
		{
			name:           "a cast after the group",
			column:         "code varchar(40) DEFAULT (('X-'::text || (gen_random_uuid())::text))::character varying",
			wantExpression: "(('X-'::text || (gen_random_uuid())::text))::character varying",
			wantNullable:   true,
		},
		{
			name:           "a cast after a call in a group",
			column:         "d date DEFAULT (now())::date",
			wantExpression: "(now())::date",
			wantNullable:   true,
		},
		{
			name:           "a cast after the group, then NOT NULL",
			column:         "s text NOT NULL DEFAULT ('a' || 'b')::text",
			wantExpression: "('a' || 'b')::text",
		},
		{
			name:           "an operator after the group",
			column:         "n int DEFAULT (1) + 2",
			wantExpression: "(1) + 2",
			wantNullable:   true,
		},
		{
			name:           "an operator after the group, then NOT NULL",
			column:         "n int DEFAULT (1) + 2 NOT NULL",
			wantExpression: "(1) + 2",
		},
		{
			name:           "NULL as the operand after an operator",
			column:         "n int DEFAULT (1) + NULL NOT NULL",
			wantExpression: "(1) + NULL",
		},
		{
			name:           "an operator after a call, then NOT NULL",
			column:         "e timestamptz DEFAULT now() + interval '1 day' NOT NULL",
			wantExpression: "now() + interval '1 day'",
		},
		{
			name:           "nested groups",
			column:         "n int DEFAULT ((1))",
			wantExpression: "((1))",
			wantNullable:   true,
		},
		{
			name:           "a group, then NOT NULL",
			column:         "n int DEFAULT (1) NOT NULL",
			wantExpression: "(1)",
		},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)

			column := declaredColumn(c, row.column)

			c.Assert(column.Default, qt.IsNotNil)
			c.Assert(column.Default.Expression, qt.Equals, row.wantExpression)
			c.Assert(column.Default.Value, qt.Equals, "")
			c.Assert(column.Nullable, qt.Equals, row.wantNullable)
		})
	}
}

// An empty group is refused at its opening parenthesis, in a column
// declaration and in ALTER COLUMN SET DEFAULT alike.
func TestParse_ParenthesizedColumnDefault_FailurePath(t *testing.T) {
	rows := []struct {
		name    string
		sql     string
		wantErr string
	}{
		{
			name:    "an empty group",
			sql:     "CREATE TABLE t (id int PRIMARY KEY, n int DEFAULT ());",
			wantErr: `line 1, column 51: expected default value: empty DEFAULT expression`,
		},
		{
			name:    "a group holding only space",
			sql:     "CREATE TABLE t (id int PRIMARY KEY, n int DEFAULT ( ));",
			wantErr: `line 1, column 51: expected default value: empty DEFAULT expression`,
		},
		{
			name:    "an empty group set by ALTER COLUMN",
			sql:     "ALTER TABLE t ALTER COLUMN a SET DEFAULT ();",
			wantErr: `line 1, column 42: expected default value after ALTER COLUMN a SET DEFAULT: empty DEFAULT expression`,
		},
		{
			name:    "no type after the cast",
			sql:     "CREATE TABLE t (id int PRIMARY KEY, n int DEFAULT (1)::);",
			wantErr: `line \d+, column \d+: expected default value: expected type after '::': .*`,
		},
		{
			name:    "an unterminated group",
			sql:     "ALTER TABLE t ALTER COLUMN a SET DEFAULT (1;",
			wantErr: `line \d+, column \d+: expected default value after ALTER COLUMN a SET DEFAULT: unterminated DEFAULT expression`,
		},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)

			statements, err := parser.NewParser(row.sql, parser.WithDialect("postgres"), parser.WithSQL(builtintest.Runtime().SQL())).Parse()

			c.Assert(err, qt.ErrorMatches, row.wantErr)
			c.Assert(statements, qt.IsNil)
		})
	}
}
