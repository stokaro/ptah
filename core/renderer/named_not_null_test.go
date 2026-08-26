package renderer_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/ast"
	"go.5x5.cz/ptah/core/platform/capability"
	"go.5x5.cz/ptah/core/renderer"
)

// notNullTable is one table whose single column is NOT NULL, named when the
// caller passes a name and bare when it passes "".
func notNullTable(constraintName string) *ast.CreateTableNode {
	column := ast.NewColumn("a", "INTEGER").SetNotNull()
	column.SetNotNullConstraintName(constraintName)

	return ast.NewCreateTable("widget").AddColumn(column)
}

// nullableTableNamingItsNotNull is the inconsistent declaration: a constraint
// name on a column that has no such constraint.
func nullableTableNamingItsNotNull(constraintName string) *ast.CreateTableNode {
	column := ast.NewColumn("a", "INTEGER")
	column.SetNotNullConstraintName(constraintName)

	return ast.NewCreateTable("widget").AddColumn(column)
}

// postgresCaps is a PostgreSQL 17 preset with the named-NOT-NULL answer under
// the caller's control, which is what separates the two targets: 17 and 18
// accept the identical syntax and only 18 keeps the name.
func postgresCaps(named bool) capability.Capabilities {
	return capability.Postgres17().With(capability.NamedNotNullConstraints, named)
}

// TestNamedNotNull_ATargetThatKeepsTheNameEmitsIt is the measured case.
//
// PostgreSQL 18 records one row per NOT NULL in pg_constraint with contype 'n',
// keyed to the column through conkey, and answers to the name (stokaro/ptah#2161).
func TestNamedNotNull_ATargetThatKeepsTheNameEmitsIt(t *testing.T) {
	c := qt.New(t)
	r, err := renderer.NewRendererWithCapabilities("postgres", postgresCaps(true))
	c.Assert(err, qt.IsNil)

	sql, renderErr := r.Render(notNullTable("c_keep"))

	c.Assert(renderErr, qt.IsNil)
	c.Assert(sql, qt.Contains, `CONSTRAINT "c_keep" NOT NULL`)
}

// TestNamedNotNull_ATargetThatRecordsNothingRefuses is the other half, and the
// reason the key is about persistence rather than syntax.
//
// PostgreSQL 17 accepts `CONSTRAINT c NOT NULL` and stores nothing, so emitting
// the name there produces DDL that applies, reads back bare, and leaves every
// later comparison reporting a difference no apply can settle. It refuses
// rather than dropping the name, because a silent drop is that same drift with
// no diagnostic.
func TestNamedNotNull_ATargetThatRecordsNothingRefuses(t *testing.T) {
	c := qt.New(t)
	r, err := renderer.NewRendererWithCapabilities("postgres", postgresCaps(false))
	c.Assert(err, qt.IsNil)

	_, renderErr := r.Render(notNullTable("c_keep"))

	c.Assert(renderErr, qt.IsNotNil)
	c.Assert(renderErr.Error(), qt.Contains, "does not keep a NOT NULL constraint name")
}

// TestNamedNotNull_AnUnnamedConstraintIsUnaffected is the control for the test
// above: without it, a renderer that refused every NOT NULL on a target without
// the key would pass.
func TestNamedNotNull_AnUnnamedConstraintIsUnaffected(t *testing.T) {
	c := qt.New(t)
	r, err := renderer.NewRendererWithCapabilities("postgres", postgresCaps(false))
	c.Assert(err, qt.IsNil)

	sql, renderErr := r.Render(notNullTable(""))

	c.Assert(renderErr, qt.IsNil)
	c.Assert(sql, qt.Contains, "NOT NULL")
	c.Assert(sql, qt.Not(qt.Contains), "CONSTRAINT")
}

// TestNamedNotNull_ANameOnANullableColumnNamesNothing refuses a declaration
// that contradicts itself, rather than rendering a nullable column and
// reporting success.
func TestNamedNotNull_ANameOnANullableColumnNamesNothing(t *testing.T) {
	c := qt.New(t)
	r, err := renderer.NewRendererWithCapabilities("postgres", postgresCaps(true))
	c.Assert(err, qt.IsNil)

	_, renderErr := r.Render(nullableTableNamingItsNotNull("c_keep"))

	c.Assert(renderErr, qt.IsNotNil)
	c.Assert(renderErr.Error(), qt.Contains, "names no constraint")
}
