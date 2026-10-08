package builtin

// White-box testing required: renderNodes' contract of clearing a reused
// visitor's accumulated output on failure is not observable through the
// exported RenderSQL functions, which construct a fresh renderer per call.

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/ptaherr"
)

func TestVisitorRenderSQL_FailedRenderClearsPreviousOutput(t *testing.T) {
	c := qt.New(t)
	r, err := NewRenderer("postgres")
	c.Assert(err, qt.IsNil)
	valid := ast.NewCreateTable("parents").AddColumn(ast.NewColumn("id", "INTEGER").SetPrimary())

	sql, err := renderNodes(t.Context(), r, valid)
	c.Assert(err, qt.IsNil)
	c.Assert(sql.SQL(), qt.Contains, "CREATE TABLE")

	sql, err = renderNodes(t.Context(), r, nil)
	c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
	c.Assert(sql.SQL(), qt.Equals, "")
	c.Assert(r.Output(), qt.Equals, "")
}

func TestVisitorRenderSQL_RendererErrorClearsPartialOutput(t *testing.T) {
	c := qt.New(t)
	r, err := NewRenderer("postgres")
	c.Assert(err, qt.IsNil)
	valid := ast.NewCreateTable("parents").AddColumn(ast.NewColumn("id", "INTEGER").SetPrimary())
	nullsDistinct := true
	invalid := ast.NewIndex("idx_parents_id", "parents", "id")
	invalid.NullsDistinct = &nullsDistinct

	sql, err := renderNodes(t.Context(), r, valid, invalid)

	c.Assert(err, qt.ErrorMatches, "postgresql NULLS DISTINCT is only valid for unique indexes")
	c.Assert(sql.SQL(), qt.Equals, "")
	c.Assert(r.Output(), qt.Equals, "")
}
