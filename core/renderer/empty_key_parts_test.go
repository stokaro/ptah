package renderer_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/ptaherr"
	"ptah.run/core/renderer"
	"ptah.run/core/schemamodel"
)

// An index and a UNIQUE, PRIMARY KEY or CHECK constraint each have a payload no
// dialect renders as nothing. Emitting an empty one produced `ON "t" ()`,
// `UNIQUE ()`, `PRIMARY KEY ()` and `CHECK ()` -- invalid on every engine, and
// handed back after a successful render. Measured on master before the fix:
// every dialect but ClickHouse accepted all four, and the two that refused any
// of them did so for unrelated reasons. See stokaro/ptah#2790.

func emptyKeyPartsTable() []schemamodel.Table {
	return []schemamodel.Table{{StructName: "T", Name: "t"}}
}

func emptyKeyPartsFields() []schemamodel.Field {
	// The primary key is what lets ClickHouse into these tests at all: its
	// MergeTree engine refuses a table with neither ORDER BY nor a primary key,
	// and for an index that refusal arrives while rendering the table, before
	// the guard under test is reached. A primary-key column is valid on every
	// other dialect, so one fixture serves the whole loop.
	return []schemamodel.Field{{StructName: "T", Name: "a", Type: "VARCHAR(10)", Primary: true}}
}

func TestRenderEmptyKeyParts_ModelFailurePath(t *testing.T) {
	tests := []struct {
		name    string
		index   []schemamodel.Index
		wantErr string
	}{
		{
			name:    "named index, no fields and no parts",
			index:   []schemamodel.Index{{StructName: "T", Name: "k", TableName: "t"}},
			wantErr: `index "k" declares no key parts; it needs at least one column or expression`,
		},
		{
			name:    "unnamed index",
			index:   []schemamodel.Index{{StructName: "T", TableName: "t"}},
			wantErr: `index declares no key parts; it needs at least one column or expression`,
		},
		{
			name: "structured parts carrying neither a name nor an expression",
			index: []schemamodel.Index{{
				StructName: "T", Name: "k", TableName: "t",
				Parts: []schemamodel.IndexPart{{Desc: true}, {Prefix: "7"}},
			}},
			wantErr: `index "k" declares no key parts; it needs at least one column or expression`,
		},
	}

	for _, dialect := range renderer.SupportedDialects() {
		for _, test := range tests {
			t.Run(dialect+"/"+test.name, func(t *testing.T) {
				c := qt.New(t)
				database := &schemamodel.Database{
					Tables:  emptyKeyPartsTable(),
					Fields:  emptyKeyPartsFields(),
					Indexes: test.index,
				}

				statements, err := renderer.GetOrderedCreateStatements(database, dialect)

				c.Assert(statements, qt.IsNil)
				c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
				c.Assert(err.Error(), qt.Contains, test.wantErr)
			})
		}
	}
}

func TestRenderEmptyConstraintKeyParts_ModelFailurePath(t *testing.T) {
	tests := []struct {
		name       string
		constraint schemamodel.Constraint
		wantErr    string
	}{
		{
			name:       "named unique",
			constraint: schemamodel.Constraint{StructName: "T", Name: "uq", Type: "UNIQUE", Table: "t"},
			wantErr:    `UNIQUE constraint "uq" declares no key parts; it needs at least one column`,
		},
		{
			name:       "unnamed unique",
			constraint: schemamodel.Constraint{StructName: "T", Type: "UNIQUE", Table: "t"},
			wantErr:    `UNIQUE constraint declares no key parts; it needs at least one column`,
		},
		{
			name:       "named primary key",
			constraint: schemamodel.Constraint{StructName: "T", Name: "pk", Type: "PRIMARY KEY", Table: "t"},
			wantErr:    `PRIMARY KEY constraint "pk" declares no key parts; it needs at least one column`,
		},
		{
			name:       "named check with no expression",
			constraint: schemamodel.Constraint{StructName: "T", Name: "ck", Type: "CHECK", Table: "t"},
			wantErr:    `CHECK constraint "ck" declares no key parts; it needs an expression`,
		},
		{
			name:       "check whose expression is only whitespace",
			constraint: schemamodel.Constraint{StructName: "T", Name: "ck", Type: "CHECK", Table: "t", CheckExpression: "   "},
			wantErr:    `CHECK constraint "ck" declares no key parts; it needs an expression`,
		},
	}

	for _, dialect := range renderer.SupportedDialects() {
		for _, test := range tests {
			t.Run(dialect+"/"+test.name, func(t *testing.T) {
				c := qt.New(t)
				database := &schemamodel.Database{
					Tables:      emptyKeyPartsTable(),
					Fields:      emptyKeyPartsFields(),
					Constraints: []schemamodel.Constraint{test.constraint},
				}

				statements, err := renderer.GetOrderedCreateStatements(database, dialect)

				c.Assert(statements, qt.IsNil)
				c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
				c.Assert(err.Error(), qt.Contains, test.wantErr)
			})
		}
	}
}

// The AST entry point is the other half. RenderSQL is handed nodes directly and
// never walks the schema model, so a guard placed on the model would leave this
// path emitting the same invalid SQL.
func TestRenderEmptyKeyParts_ASTFailurePath(t *testing.T) {
	tests := []struct {
		name    string
		node    ast.Node
		wantErr string
	}{
		{
			name:    "standalone index",
			node:    ast.NewIndex("k", "t"),
			wantErr: `index "k" declares no key parts`,
		},
		{
			name:    "standalone unique constraint",
			node:    ast.NewUniqueConstraint("uq"),
			wantErr: `UNIQUE constraint "uq" declares no key parts`,
		},
		{
			name: "unique constraint inside a create table",
			node: ast.NewCreateTable("t").
				AddColumn(ast.NewColumn("a", "VARCHAR(10)")).
				AddConstraint(ast.NewUniqueConstraint("uq")),
			wantErr: `UNIQUE constraint "uq" declares no key parts`,
		},
	}

	for _, dialect := range renderer.SupportedDialects() {
		for _, test := range tests {
			t.Run(dialect+"/"+test.name, func(t *testing.T) {
				c := qt.New(t)

				sql, err := renderer.RenderSQL(dialect, test.node)

				c.Assert(sql, qt.Equals, "")
				c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
				c.Assert(err.Error(), qt.Contains, test.wantErr)
			})
		}
	}
}

// The controls. Each is a shape the refusal must not reach, and each is the
// nearest valid neighbor of a row above: a legacy column list, a structured
// part list, an expression-only part that has no column at all, and the two
// constraint kinds whose payload is present.
func TestRenderPopulatedKeyParts_HappyPath(t *testing.T) {
	tests := []struct {
		name     string
		database *schemamodel.Database
	}{
		{
			name: "index with a legacy field list",
			database: &schemamodel.Database{
				Tables:  emptyKeyPartsTable(),
				Fields:  emptyKeyPartsFields(),
				Indexes: []schemamodel.Index{{StructName: "T", Name: "k", TableName: "t", Fields: []string{"a"}}},
			},
		},
		{
			name: "index with a structured part naming a column",
			database: &schemamodel.Database{
				Tables: emptyKeyPartsTable(),
				Fields: emptyKeyPartsFields(),
				Indexes: []schemamodel.Index{{
					StructName: "T", Name: "k", TableName: "t",
					Parts: []schemamodel.IndexPart{{Name: "a"}},
				}},
			},
		},
		{
			name: "unique constraint with a column",
			database: &schemamodel.Database{
				Tables: emptyKeyPartsTable(),
				Fields: emptyKeyPartsFields(),
				Constraints: []schemamodel.Constraint{
					{StructName: "T", Name: "uq", Type: "UNIQUE", Table: "t", Columns: []string{"a"}},
				},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			statements, err := renderer.GetOrderedCreateStatements(test.database, "postgres")

			c.Assert(err, qt.IsNil)
			c.Assert(len(statements) > 0, qt.IsTrue)
		})
	}
}

// An index whose only key part is an expression has no column and is valid on
// every engine that supports functional indexes. It is the row that separates
// "names no column" from "names nothing", which is what the refusal actually
// asks.
func TestRenderExpressionOnlyIndex_HappyPath(t *testing.T) {
	c := qt.New(t)
	database := &schemamodel.Database{
		Tables: emptyKeyPartsTable(),
		Fields: emptyKeyPartsFields(),
		Indexes: []schemamodel.Index{{
			StructName: "T", Name: "k", TableName: "t",
			Parts: []schemamodel.IndexPart{{Expr: "lower(a)"}},
		}},
	}

	statements, err := renderer.GetOrderedCreateStatements(database, "postgres")

	c.Assert(err, qt.IsNil)
	c.Assert(statements[len(statements)-1], qt.Contains, "lower(a)")
}

// The AST path is where an expression-only part decides the answer on its own.
// Walking the schema model fills Columns with the expression text as well as
// building the Part, so via that path a named column is what admits the index
// and the expression branch never runs. A caller handing RenderSQL an
// ast.IndexNode directly can set Parts and leave Columns empty, and that index
// is valid: it has no column, and it indexes something.
func TestRenderExpressionOnlyIndexNode_ASTHappyPath(t *testing.T) {
	tests := []struct {
		name  string
		parts []ast.IndexPart
	}{
		{name: "expression part", parts: []ast.IndexPart{{Expr: "lower(a)"}}},
		{name: "named part", parts: []ast.IndexPart{{Name: "a"}}},
		{name: "expression beside an empty part", parts: []ast.IndexPart{{Desc: true}, {Expr: "lower(a)"}}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			index := ast.NewIndex("k", "t")
			index.Parts = test.parts

			sql, err := renderer.RenderSQL("postgres", index)

			c.Assert(err, qt.IsNil)
			c.Assert(sql, qt.Contains, "CREATE INDEX")
		})
	}
}

// The same node with parts that name nothing is still refused, which is what
// makes the test above an assertion about the expression rather than about
// having any part at all.
func TestRenderIndexNodeWithBlankParts_ASTFailurePath(t *testing.T) {
	c := qt.New(t)
	index := ast.NewIndex("k", "t")
	index.Parts = []ast.IndexPart{{Desc: true}, {Prefix: "7"}}

	sql, err := renderer.RenderSQL("postgres", index)

	c.Assert(sql, qt.Equals, "")
	c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
	c.Assert(err.Error(), qt.Contains, `index "k" declares no key parts`)
}

// FOREIGN KEY and EXCLUDE are refused by guards that predate this one, and this
// asserts they still are -- so that the new guard is never widened to cover
// them. Two guards over one shape leave neither measurable: either could be
// deleted with the tests still green.
func TestRenderEmptyForeignKeyAndExclude_RefusedByTheirOwnGuards(t *testing.T) {
	tests := []struct {
		name       string
		constraint schemamodel.Constraint
		wantErr    string
	}{
		{
			name:       "foreign key with no columns",
			constraint: schemamodel.Constraint{StructName: "T", Name: "fk", Type: "FOREIGN KEY", Table: "t"},
			wantErr:    "0 local columns and 0 referenced columns",
		},
		{
			name:       "exclude with no elements",
			constraint: schemamodel.Constraint{StructName: "T", Name: "ex", Type: "EXCLUDE", Table: "t"},
			wantErr:    "exclude constraint missing using method or elements",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			database := &schemamodel.Database{
				Tables:      emptyKeyPartsTable(),
				Fields:      emptyKeyPartsFields(),
				Constraints: []schemamodel.Constraint{test.constraint},
			}

			statements, err := renderer.GetOrderedCreateStatements(database, "postgres")

			c.Assert(statements, qt.IsNil)
			c.Assert(err, qt.IsNotNil)
			c.Assert(err.Error(), qt.Contains, test.wantErr)
			c.Assert(err.Error(), qt.Not(qt.Contains), "declares no key parts")
		})
	}
}
