package parser_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/ast"
	"go.5x5.cz/ptah/internal/parser"
)

// A column-level foreign key may be named, and the name is what the constraint
// is called on the server -- stokaro/ptah#2161.
//
// Ptah read `CONSTRAINT <name>` on CHECK alone and refused every other form by
// naming the keyword that followed:
//
//	parent_id INTEGER CONSTRAINT fk_child_parent REFERENCES parent(id)
//	-> unsupported column constraint "REFERENCES"
//
// which points at a word that parses perfectly well one token to the left.
// Measured on PostgreSQL 17.11: all six forms are accepted by the server.
func TestParser_ColumnLevelNamedForeignKey(t *testing.T) {
	c := qt.New(t)

	statements, err := parser.NewParser(`CREATE TABLE child (
		id INTEGER PRIMARY KEY,
		parent_id INTEGER CONSTRAINT fk_child_parent REFERENCES parent(id),
		other_id INTEGER constraint fk_lower references other(id) ON DELETE CASCADE
	);`).Parse()

	c.Assert(err, qt.IsNil)
	table := statements.Statements[0].(*ast.CreateTableNode)
	c.Assert(table.Columns, qt.HasLen, 3)

	c.Assert(table.Columns[1].ForeignKey.Name, qt.Equals, "fk_child_parent")
	c.Assert(table.Columns[1].ForeignKey.Table, qt.Equals, "parent")
	c.Assert(table.Columns[1].ForeignKey.Column, qt.Equals, "id")

	// Lower case, and with a referential action after it: the name must not
	// swallow what follows the reference.
	c.Assert(table.Columns[2].ForeignKey.Name, qt.Equals, "fk_lower")
	c.Assert(table.Columns[2].ForeignKey.OnDelete, qt.Equals, "CASCADE")
}

// An unnamed reference keeps working, and keeps carrying no name. Without this
// the test above would pass on a parser that attached a name to everything.
func TestParser_ColumnLevelUnnamedForeignKeyCarriesNoName(t *testing.T) {
	c := qt.New(t)

	statements, err := parser.NewParser(
		`CREATE TABLE child (parent_id INTEGER REFERENCES parent(id));`).Parse()

	c.Assert(err, qt.IsNil)
	table := statements.Statements[0].(*ast.CreateTableNode)
	c.Assert(table.Columns[0].ForeignKey.Name, qt.Equals, "")
	c.Assert(table.Columns[0].ForeignKey.Table, qt.Equals, "parent")
}

// The four kinds with nowhere to keep a name are refused, and the refusal says
// why rather than blaming the keyword after the name.
//
// Reading them would drop the name, the renderer would emit an unnamed
// constraint, the server would generate its own, and the reader would bring
// that back as a difference nobody can resolve. A refusal is worse than reading
// the file and better than permanent drift.
func TestParser_ColumnLevelNamedConstraintWithNowhereToKeepTheName(t *testing.T) {
	tests := []struct {
		name       string
		constraint string
	}{
		{name: "not null", constraint: "NOT NULL"},
		{name: "default", constraint: "DEFAULT 1"},
		{
			// PRIMARY KEY has a place to keep a name -- the table-level form
			// this parser already reads -- and it is still refused, because the
			// renderer collapses a single-column primary key back into the
			// column and drops the name on the way out. Measured on PostgreSQL
			// 17 from the table-level spelling: `CONSTRAINT c_pk PRIMARY KEY
			// (b)` applies as `t_pkey` and --dry-run answers `Schema is synced`
			// (stokaro/ptah#2180).
			name: "primary key", constraint: "PRIMARY KEY",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			_, err := parser.NewParser(
				`CREATE TABLE t (b INTEGER CONSTRAINT c_x ` + tt.constraint + `);`).Parse()

			c.Assert(err, qt.ErrorMatches, `.*named column constraint "c_x".*`)
			c.Assert(err, qt.ErrorMatches, `.*CHECK, REFERENCES and UNIQUE.*`)
		})
	}
}

// A named UNIQUE written on a column is read as the table constraint it is --
// stokaro/ptah#2161.
//
// `b INTEGER CONSTRAINT uq UNIQUE` and `UNIQUE (b) CONSTRAINT uq` describe the
// same catalog row, and only the second is a shape Ptah's model can carry a
// name for: ColumnNode keeps Unique as a boolean with nowhere to put one.
// Reading the first into the second keeps the name instead of dropping it.
//
// Measured on PostgreSQL 17: applied, `pg_constraint.conname` reads `c_uq`, and
// a second plan reports `Schema is synced`.
func TestParser_ColumnLevelNamedUniqueBecomesATableConstraint(t *testing.T) {
	c := qt.New(t)

	statements, err := parser.NewParser(
		`CREATE TABLE t (a INTEGER, b INTEGER CONSTRAINT c_uq UNIQUE);`).Parse()

	c.Assert(err, qt.IsNil)
	table := statements.Statements[0].(*ast.CreateTableNode)
	c.Assert(table.Constraints, qt.HasLen, 1)
	c.Assert(table.Constraints[0].Name, qt.Equals, "c_uq")
	c.Assert(table.Constraints[0].Type, qt.Equals, ast.UniqueConstraint)
	c.Assert(table.Constraints[0].Columns, qt.DeepEquals, []string{"b"})
	// The column keeps no unique flag of its own: one constraint, stated once.
	// Both would make the renderer emit the key twice.
	c.Assert(table.Columns[1].Unique, qt.IsFalse)
}

// An unnamed UNIQUE stays on the column, which is where a nameless one belongs
// and what every existing schema already produces. Without this the test above
// would pass on a parser that moved every inline UNIQUE to table level.
func TestParser_ColumnLevelUnnamedUniqueStaysOnTheColumn(t *testing.T) {
	c := qt.New(t)

	statements, err := parser.NewParser(
		`CREATE TABLE t (a INTEGER, b INTEGER UNIQUE);`).Parse()

	c.Assert(err, qt.IsNil)
	table := statements.Statements[0].(*ast.CreateTableNode)
	c.Assert(table.Constraints, qt.HasLen, 0)
	c.Assert(table.Columns[1].Unique, qt.IsTrue)
}
