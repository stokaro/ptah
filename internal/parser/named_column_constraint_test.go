package parser_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/internal/parser"
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

// A DEFAULT is the one kind left with nowhere to keep a name, and the refusal
// says why rather than blaming the keyword after the name.
//
// Reading it would drop the name, the renderer would emit an unnamed
// constraint, the server would generate its own, and the reader would bring
// that back as a difference nobody can resolve. A refusal is worse than reading
// the file and better than permanent drift.
func TestParser_ColumnLevelNamedConstraintWithNowhereToKeepTheName(t *testing.T) {
	tests := []struct {
		name       string
		constraint string
		// named is how the refusal must spell the constraint. The cursor sits
		// on one token, so a NOT NULL is `NOT` there -- and a message that says
		// `on NOT` names half a keyword.
		named string
	}{
		// A DEFAULT is what remains: it is a value with nowhere to put a
		// constraint name and no table-level form to read one into the way
		// UNIQUE and PRIMARY KEY have. No engine Ptah supports records one --
		// which is what separates it from NOT NULL, whose name PostgreSQL 18
		// keeps in pg_constraint and Ptah now carries.
		{name: "default", constraint: "DEFAULT 1", named: "DEFAULT"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			_, err := parser.NewParser(
				`CREATE TABLE t (b INTEGER CONSTRAINT c_x ` + tt.constraint + `);`).Parse()

			c.Assert(err, qt.ErrorMatches, `.*named column constraint "c_x".*`)
			c.Assert(err, qt.ErrorMatches, `.*name on `+tt.named+`,.*`)
			c.Assert(err, qt.ErrorMatches, `.*NOT NULL, CHECK, REFERENCES, UNIQUE and PRIMARY KEY.*`)
			// The advice has to be one the reader can follow. There is no
			// `ALTER TABLE ... ADD CONSTRAINT c DEFAULT` on any supported
			// engine, so pointing at the table level would point at nothing.
			c.Assert(err, qt.ErrorMatches, `.*write the constraint without a name.*`)
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

// A named PRIMARY KEY written on a column is read as the table constraint it
// is -- stokaro/ptah#2161, unblocked by stokaro/ptah#2180.
//
// It was refused until the renderer stopped collapsing a single-column primary
// key back into the column: reading it then would have handed a name to a path
// that dropped it, applying as `t_pkey` while --dry-run answered `Schema is
// synced`. With the name surviving, reading it is what keeps it.
//
// Measured on PostgreSQL 17: applied, `pg_constraint.conname` reads `c_pk`, and
// a second plan reports `Schema is synced`.
func TestParser_ColumnLevelNamedPrimaryKeyBecomesATableConstraint(t *testing.T) {
	c := qt.New(t)

	statements, err := parser.NewParser(
		`CREATE TABLE t (a INTEGER, b INTEGER CONSTRAINT c_pk PRIMARY KEY);`).Parse()

	c.Assert(err, qt.IsNil)
	table := statements.Statements[0].(*ast.CreateTableNode)
	c.Assert(table.Constraints, qt.HasLen, 1)
	c.Assert(table.Constraints[0].Name, qt.Equals, "c_pk")
	c.Assert(table.Constraints[0].Type, qt.Equals, ast.PrimaryKeyConstraint)
	c.Assert(table.Constraints[0].Columns, qt.DeepEquals, []string{"b"})
	// The column keeps no primary flag of its own: one key, stated once.
	c.Assert(table.Columns[1].Primary, qt.IsFalse)
}

// An unnamed inline PRIMARY KEY stays on the column, which is where a nameless
// one belongs and what every existing schema produces.
func TestParser_ColumnLevelUnnamedPrimaryKeyStaysOnTheColumn(t *testing.T) {
	c := qt.New(t)

	statements, err := parser.NewParser(
		`CREATE TABLE t (a INTEGER, b INTEGER PRIMARY KEY);`).Parse()

	c.Assert(err, qt.IsNil)
	table := statements.Statements[0].(*ast.CreateTableNode)
	c.Assert(table.Constraints, qt.HasLen, 0)
	c.Assert(table.Columns[1].Primary, qt.IsTrue)
}

// PRIMARY without KEY is a syntax error, not a primary key. Without this the
// reader could accept `CONSTRAINT c PRIMARY` and record a key nobody wrote.
func TestParser_ColumnLevelNamedPrimaryRequiresKey(t *testing.T) {
	c := qt.New(t)

	_, err := parser.NewParser(
		`CREATE TABLE t (b INTEGER CONSTRAINT c_pk PRIMARY);`).Parse()

	c.Assert(err, qt.ErrorMatches, `.*expected KEY after PRIMARY.*`)
}

// TestParser_ColumnLevelNamedNotNullIsRead is the other half of the table
// above: the kind that moved out of it.
//
// The name is read on every dialect and refused later, by the layer that knows
// the target. A parser that refused here would reject a file PostgreSQL 18
// accepts and round-trips; one that gated on its own dialect would refuse a
// `.sql` file being read for a target it was not written against
// (stokaro/ptah#2161).
func TestParser_ColumnLevelNamedNotNullIsRead(t *testing.T) {
	c := qt.New(t)

	statements, err := parser.NewParser(
		`CREATE TABLE t (b INTEGER CONSTRAINT c_keep NOT NULL);`).Parse()

	c.Assert(err, qt.IsNil)
	table := statements.Statements[0].(*ast.CreateTableNode)
	c.Assert(table.Columns[0].NotNullConstraintName, qt.Equals, "c_keep")
	// The name describes a constraint the same clause establishes. Reading one
	// without the other would name nothing.
	c.Assert(table.Columns[0].Nullable, qt.IsFalse)
}

// TestParser_UnnamedNotNullCarriesNoName is the control. Without it, a parser
// that stamped every NOT NULL with a name would pass the test above.
func TestParser_UnnamedNotNullCarriesNoName(t *testing.T) {
	c := qt.New(t)

	statements, err := parser.NewParser(`CREATE TABLE t (b INTEGER NOT NULL);`).Parse()

	c.Assert(err, qt.IsNil)
	table := statements.Statements[0].(*ast.CreateTableNode)
	c.Assert(table.Columns[0].NotNullConstraintName, qt.Equals, "")
	c.Assert(table.Columns[0].Nullable, qt.IsFalse)
}
