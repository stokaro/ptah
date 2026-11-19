package parser_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/internal/builtintest"
	"ptah.run/internal/parser"
)

// parseComputedColumn reads one CREATE TABLE in dialect and returns the
// column named x.
func parseComputedColumn(c *qt.C, dialect, sql string) *ast.ColumnNode {
	c.Helper()
	statements, err := parser.NewParser(sql, parser.WithDialect(dialect), parser.WithSQL(builtintest.Runtime().SQL())).Parse()
	c.Assert(err, qt.IsNil)
	c.Assert(statements.Statements, qt.HasLen, 1)
	table, ok := statements.Statements[0].(*ast.CreateTableNode)
	c.Assert(ok, qt.IsTrue)
	columns := make(map[string]*ast.ColumnNode, len(table.Columns))
	for _, column := range table.Columns {
		columns[column.Name] = column
	}
	c.Assert(columns["x"], qt.IsNotNil)
	return columns["x"]
}

// TestParse_ComputedColumn_HappyPath reads the clauses that give a ClickHouse
// column its value into the column's generated kind and expression, as
// written. A kind the grammar states as optional reads an empty expression
// where the column ends after it.
func TestParse_ComputedColumn_HappyPath(t *testing.T) {
	rows := []struct {
		name        string
		column      string
		wantKind    string
		wantExpr    string
		wantComment string
	}{
		{name: "MATERIALIZED over operators", column: "x Int64 MATERIALIZED a + b * c", wantKind: "MATERIALIZED", wantExpr: "a + b * c"},
		{name: "ALIAS of a call", column: "x String ALIAS concat(s, '-')", wantKind: "ALIAS", wantExpr: "concat(s, '-')"},
		{name: "EPHEMERAL with an expression", column: "x Int64 EPHEMERAL 5", wantKind: "EPHEMERAL", wantExpr: "5"},
		{name: "EPHEMERAL alone before a comma", column: "x Int64 EPHEMERAL, y Int64", wantKind: "EPHEMERAL"},
		{name: "EPHEMERAL alone before a comment", column: "x Int64 EPHEMERAL COMMENT 'c'", wantKind: "EPHEMERAL", wantComment: "c"},
		{name: "EPHEMERAL NULL reads NULL", column: "x Nullable(Int64) EPHEMERAL NULL", wantKind: "EPHEMERAL", wantExpr: "NULL"},
		{
			name: "MATERIALIZED predicate before a comment", column: "x UInt8 MATERIALIZED a = 1 OR b = 3 COMMENT 'c'",
			wantKind: "MATERIALIZED", wantExpr: "a = 1 OR b = 3", wantComment: "c",
		},
		{name: "lower-case keyword", column: "x Int64 materialized a * 2", wantKind: "MATERIALIZED", wantExpr: "a * 2"},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			sql := "CREATE TABLE t (id UInt64, a Int64, b Int64, c Int64, s String, " + row.column + ") ENGINE = MergeTree ORDER BY id"

			column := parseComputedColumn(c, platform.ClickHouse, sql)

			c.Assert(column.GeneratedKind, qt.Equals, row.wantKind)
			c.Assert(column.GeneratedExpression, qt.Equals, row.wantExpr)
			c.Assert(column.Default, qt.IsNil)
			c.Assert(column.Comment, qt.Equals, row.wantComment)
		})
	}
}

// TestParse_ComputedColumn_FailurePath refuses a computed clause the engine
// refuses, a column that carries two values, and the words on an engine whose
// grammar states no computed kinds.
func TestParse_ComputedColumn_FailurePath(t *testing.T) {
	rows := []struct {
		name    string
		dialect string
		column  string
		wantErr string
	}{
		{
			name: "MATERIALIZED without an expression", dialect: platform.ClickHouse, column: "x Int64 MATERIALIZED",
			wantErr: `line 1, column 57: expected MATERIALIZED expression`,
		},
		{
			name: "ALIAS without an expression before a comment", dialect: platform.ClickHouse, column: "x Int64 ALIAS COMMENT 'c'",
			wantErr: `line 1, column 51: expected ALIAS expression`,
		},
		{
			name: "MATERIALIZED and DEFAULT", dialect: platform.ClickHouse, column: "x Int64 MATERIALIZED 1 DEFAULT 2",
			wantErr: `line 1, column 60: column x carries a second value: it takes one of DEFAULT and MATERIALIZED, ALIAS, EPHEMERAL; remove one of them`,
		},
		{
			name: "DEFAULT and ALIAS", dialect: platform.ClickHouse, column: "x Int64 DEFAULT 1 ALIAS 2",
			wantErr: `line 1, column 55: column x carries a second value: it takes one of DEFAULT and MATERIALIZED, ALIAS, EPHEMERAL; remove one of them`,
		},
		{
			name: "EPHEMERAL and ALIAS", dialect: platform.ClickHouse, column: "x Int64 EPHEMERAL ALIAS 2",
			wantErr: `line 1, column 55: column x carries a second value: it takes one of DEFAULT and MATERIALIZED, ALIAS, EPHEMERAL; remove one of them`,
		},
		{
			name: "MATERIALIZED on PostgreSQL", dialect: platform.Postgres, column: "x bigint MATERIALIZED 1",
			wantErr: `line 1, column 46: unsupported column attribute: MATERIALIZED`,
		},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			sql := "CREATE TABLE t (id UInt64, a Int64, " + row.column + ")"

			statements, err := parser.NewParser(sql, parser.WithDialect(row.dialect), parser.WithSQL(builtintest.Runtime().SQL())).Parse()

			c.Assert(err, qt.ErrorMatches, row.wantErr)
			c.Assert(statements, qt.IsNil)
		})
	}
}
