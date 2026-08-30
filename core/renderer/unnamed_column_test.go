package renderer_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/ast"
	"go.5x5.cz/ptah/core/platform/capability"
	"go.5x5.cz/ptah/core/ptaherr"
	"go.5x5.cz/ptah/core/renderer"
	"go.5x5.cz/ptah/core/schemamodel"
)

// unnamedColumnSchema is one table whose second column carries the name the
// caller passes. Passing "" is the reproduction from stokaro/ptah#2608 -- an
// empty YAML mapping key is the spelling a person reaches it by -- and passing
// a name is the acceptance control.
//
// The table declares a primary key because ClickHouse refuses a MergeTree
// table without one, and that refusal would answer both halves of the
// comparison below for a reason that has nothing to do with the column's name.
func unnamedColumnSchema(name string) schemamodel.Database {
	return schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "Product", Name: "products"}},
		Fields: []schemamodel.Field{
			{StructName: "Product", FieldName: "ID", Name: "id", Type: "BIGINT", Primary: true},
			{StructName: "Product", FieldName: "Label", Name: name, Type: "VARCHAR(64)", Nullable: true},
		},
	}
}

// TestUnnamedColumn_EveryTargetRefusesIt is stokaro/ptah#2608.
//
// Eight targets wrote the column out as an empty delimited identifier and
// exited 0, and Oracle did not return at all. Measured 2026-08-30 against the
// DDL that was produced: PostgreSQL 18 answers `zero-length delimited
// identifier`, MySQL 8.4 and MariaDB 11.8.9 answer `Incorrect column name`
// naming the empty one, and SQLite 3.53.1 is the only engine that stores such a
// column -- one per table, since a second answers `duplicate column name`.
//
// The dialect list is read from the capability package rather than written out,
// so a target added there is covered the day it arrives.
func TestUnnamedColumn_EveryTargetRefusesIt(t *testing.T) {
	for _, dialect := range capability.DefaultDialects() {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)

			schema := unnamedColumnSchema("")
			statements, err := renderer.GetOrderedCreateStatements(&schema, dialect)

			c.Assert(err, qt.IsNotNil)
			c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
			c.Assert(err.Error(), qt.Contains, `table "products" declares a column that has no name`)
			c.Assert(statements, qt.IsNil)
		})
	}
}

// TestUnnamedColumn_ANamedColumnStillRenders is the acceptance control for the
// test above: a refusal that fired for every schema would pass it.
func TestUnnamedColumn_ANamedColumnStillRenders(t *testing.T) {
	for _, dialect := range capability.DefaultDialects() {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)

			schema := unnamedColumnSchema("label")
			statements, err := renderer.GetOrderedCreateStatements(&schema, dialect)

			c.Assert(err, qt.IsNil)
			c.Assert(len(statements) > 0, qt.IsTrue)
		})
	}
}

// TestUnnamedColumn_EveryASTEntryPointRefusesIt covers the four ways a column
// reaches a renderer.
//
// The refusal is one check in prepareColumnNode for exactly this reason: a
// check placed in a schema reader would leave `renderer.RenderSQL` -- public
// API, and the way the planner emits an ALTER -- still writing the empty
// identifier.
func TestUnnamedColumn_EveryASTEntryPointRefusesIt(t *testing.T) {
	tests := []struct {
		name string
		node ast.Node
	}{
		{
			name: "a column rendered on its own",
			node: ast.NewColumn("", "INTEGER"),
		},
		{
			name: "a column inside CREATE TABLE",
			node: ast.NewCreateTable("products").AddColumn(ast.NewColumn("", "INTEGER")),
		},
		{
			name: "ALTER TABLE ADD COLUMN",
			node: &ast.AlterTableNode{
				Name:       "products",
				Operations: []ast.AlterOperation{&ast.AddColumnOperation{Column: ast.NewColumn("", "INTEGER")}},
			},
		},
		{
			name: "ALTER TABLE MODIFY COLUMN",
			node: &ast.AlterTableNode{
				Name:       "products",
				Operations: []ast.AlterOperation{&ast.ModifyColumnOperation{Column: ast.NewColumn("", "INTEGER")}},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			sql, err := renderer.RenderSQL("postgres", test.node)

			c.Assert(err, qt.IsNotNil)
			c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
			c.Assert(err.Error(), qt.Contains, "has no name")
			c.Assert(sql, qt.Equals, "")
		})
	}
}

// TestUnnamedColumn_ANamedColumnStillReachesEveryASTEntryPoint is the control
// for the test above: a refusal that fired for every column would pass it.
//
// It asserts the absence of a refusal rather than the presence of the name in
// the output, because the four entry points do not all produce a statement. A
// column node is not a statement -- the renderer is handed the whole table or
// alter node and decides what the column becomes -- so `RenderSQL` over a bare
// named column answers "" on PostgreSQL with no error, and asserting text
// there would be asserting the wrong thing rather than a weaker thing.
func TestUnnamedColumn_ANamedColumnStillReachesEveryASTEntryPoint(t *testing.T) {
	tests := []struct {
		name string
		node ast.Node
	}{
		{
			name: "a column rendered on its own",
			node: ast.NewColumn("label", "INTEGER"),
		},
		{
			name: "a column inside CREATE TABLE",
			node: ast.NewCreateTable("products").AddColumn(ast.NewColumn("label", "INTEGER")),
		},
		{
			name: "ALTER TABLE ADD COLUMN",
			node: &ast.AlterTableNode{
				Name:       "products",
				Operations: []ast.AlterOperation{&ast.AddColumnOperation{Column: ast.NewColumn("label", "INTEGER")}},
			},
		},
		{
			name: "ALTER TABLE MODIFY COLUMN",
			node: &ast.AlterTableNode{
				Name:       "products",
				Operations: []ast.AlterOperation{&ast.ModifyColumnOperation{Column: ast.NewColumn("label", "INTEGER")}},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			_, err := renderer.RenderSQL("postgres", test.node)

			c.Assert(err, qt.IsNil)
		})
	}
}

// TestUnnamedColumn_TheNameReachesTheStatementItBelongsTo pins the other half:
// the entry points that do produce a statement still write the column name.
func TestUnnamedColumn_TheNameReachesTheStatementItBelongsTo(t *testing.T) {
	tests := []struct {
		name string
		node ast.Node
	}{
		{
			name: "a column inside CREATE TABLE",
			node: ast.NewCreateTable("products").AddColumn(ast.NewColumn("label", "INTEGER")),
		},
		{
			name: "ALTER TABLE ADD COLUMN",
			node: &ast.AlterTableNode{
				Name:       "products",
				Operations: []ast.AlterOperation{&ast.AddColumnOperation{Column: ast.NewColumn("label", "INTEGER")}},
			},
		},
		{
			name: "ALTER TABLE MODIFY COLUMN",
			node: &ast.AlterTableNode{
				Name:       "products",
				Operations: []ast.AlterOperation{&ast.ModifyColumnOperation{Column: ast.NewColumn("label", "INTEGER")}},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			sql, err := renderer.RenderSQL("postgres", test.node)

			c.Assert(err, qt.IsNil)
			c.Assert(sql, qt.Contains, `"label"`)
		})
	}
}
