package chcolumn_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/internal/chcolumn"
)

func TestType_HappyPath(t *testing.T) {
	for _, test := range []struct {
		name   string
		column ast.ColumnNode
		want   chcolumn.Mapping
	}{
		{name: "portable type", column: ast.ColumnNode{Name: "n", Type: "INTEGER"}, want: chcolumn.Mapping{Type: "Int32"}},
		{name: "nullable", column: ast.ColumnNode{Name: "n", Type: "INTEGER", Nullable: true}, want: chcolumn.Mapping{Type: "Nullable(Int32)"}},
		{name: "nullable primary key column", column: ast.ColumnNode{Name: "n", Type: "INTEGER", Nullable: true, Primary: true}, want: chcolumn.Mapping{Type: "Int32"}},
		{name: "a type that admits NULL", column: ast.ColumnNode{Name: "s", Type: "LowCardinality(Nullable(String))", Nullable: true}, want: chcolumn.Mapping{Type: "LowCardinality(Nullable(String))"}},
		{name: "declared text", column: ast.ColumnNode{Name: "d", Type: " DateTime ", TypeIsDeclaredText: true}, want: chcolumn.Mapping{Type: "DateTime"}},
		{name: "JSON", column: ast.ColumnNode{Name: "j", Type: "JSONB", Nullable: true}, want: chcolumn.Mapping{
			Type: "Nullable(String)", Notice: "mapped JSON → Nullable(String); override via platform.clickhouse.type=JSON if you want experimental native JSON",
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got, err := chcolumn.Type(&test.column)
			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.DeepEquals, test.want)
		})
	}
}

func TestType_FailurePath(t *testing.T) {
	c := qt.New(t)
	got, err := chcolumn.Type(&ast.ColumnNode{Name: "n", Type: " "})
	c.Assert(err, qt.ErrorMatches, `column "n": clickhouse: column type is empty`)
	c.Assert(got, qt.DeepEquals, chcolumn.Mapping{})
}

// A key column is never wrapped in Nullable, and a JSON key column's notice
// names the type it is written with.
func TestKeyType(t *testing.T) {
	for _, test := range []struct {
		name   string
		column ast.ColumnNode
		want   chcolumn.Mapping
	}{
		{name: "nullable", column: ast.ColumnNode{Name: "n", Type: "INTEGER", Nullable: true}, want: chcolumn.Mapping{Type: "Int32"}},
		{name: "JSON", column: ast.ColumnNode{Name: "j", Type: "JSON", Nullable: true}, want: chcolumn.Mapping{
			Type: "String", Notice: "mapped JSON → String; override via platform.clickhouse.type=JSON if you want experimental native JSON",
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got, err := chcolumn.KeyType(&test.column)
			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.DeepEquals, test.want)
		})
	}
}

func TestValue(t *testing.T) {
	for _, test := range []struct {
		name                string
		column              ast.ColumnNode
		keyword, expression string
	}{
		{name: "none", column: ast.ColumnNode{Name: "n"}},
		{name: "literal", column: ast.ColumnNode{Name: "s", Default: &ast.DefaultValue{Value: "it's", ValueSet: true}}, keyword: "DEFAULT", expression: "'it''s'"},
		{name: "expression", column: ast.ColumnNode{Name: "t", Default: &ast.DefaultValue{Expression: "now()"}}, keyword: "DEFAULT", expression: "now()"},
		{name: "stored", column: ast.ColumnNode{Name: "c", GeneratedExpression: "a + 1", GeneratedKind: "STORED"}, keyword: "MATERIALIZED", expression: "a + 1"},
		{name: "virtual", column: ast.ColumnNode{Name: "c", GeneratedExpression: "a + 1", GeneratedKind: "virtual"}, keyword: "ALIAS", expression: "a + 1"},
		{name: "computed wins over a default", column: ast.ColumnNode{Name: "c", GeneratedExpression: "a + 1", Default: &ast.DefaultValue{Expression: "0"}}, keyword: "MATERIALIZED", expression: "a + 1"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			keyword, expression := chcolumn.Value(&test.column)
			c.Assert(keyword, qt.Equals, test.keyword)
			c.Assert(expression, qt.Equals, test.expression)
		})
	}
}

func TestDefinition(t *testing.T) {
	for _, test := range []struct {
		name                                    string
		column, typ, keyword, expression, notes string
		want                                    string
	}{
		{name: "plain", column: "n", typ: "Int32", want: "n Int32"},
		{name: "default and comment", column: "s", typ: "String", keyword: "DEFAULT", expression: "'x'", notes: "it's s", want: "s String DEFAULT 'x' COMMENT 'it''s s'"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(chcolumn.Definition(test.column, test.typ, test.keyword, test.expression, test.notes), qt.Equals, test.want)
		})
	}
}
