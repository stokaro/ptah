package postgres_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/ast"
	"go.5x5.cz/ptah/core/renderer"
)

// TestRenderer_WritesCommentsAsStatements pins that a comment reaches the
// server rather than the reader of the plan.
//
// PostgreSQL has no inline COMMENT clause, so a comment is its own statement.
// The renderer wrote the table's as a `-- POSTGRES TABLE: … (…)` header, which
// is a decoration, and wrote nothing at all for a column's — so `schema
// inspect` described a commented database, the document carried the comment,
// and applying it produced a database with no comments on it. Silently: the
// comparison reads both sides the same way and answered `Schema is synced`
// (stokaro/ptah#2101).
func TestRenderer_WritesCommentsAsStatements(t *testing.T) {
	c := qt.New(t)
	table := ast.NewCreateTable("customers").
		AddColumn(ast.NewColumn("id", "BIGINT").SetPrimary()).
		AddColumn(ast.NewColumn("email", "VARCHAR(200)").SetNotNull().SetComment("unique login"))
	table.Comment = "people who buy things"

	sql, err := renderer.RenderSQL("postgres", table)

	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Contains, `COMMENT ON TABLE "customers" IS 'people who buy things';`)
	c.Assert(sql, qt.Contains, `COMMENT ON COLUMN "customers"."email" IS 'unique login';`)
}

// TestRenderer_WritesNoCommentStatementsWithoutComments is the control: a table
// nobody commented gains no statements, so the plan for an ordinary schema does
// not grow lines that say nothing.
func TestRenderer_WritesNoCommentStatementsWithoutComments(t *testing.T) {
	c := qt.New(t)
	table := ast.NewCreateTable("customers").
		AddColumn(ast.NewColumn("id", "BIGINT").SetPrimary()).
		AddColumn(ast.NewColumn("email", "VARCHAR(200)").SetNotNull())

	sql, err := renderer.RenderSQL("postgres", table)

	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Not(qt.Contains), "COMMENT ON")
}

// TestRenderer_EscapesACommentThatCarriesAQuote pins the escaping, because a
// comment is free text a person wrote and an apostrophe in it would end the
// statement early.
func TestRenderer_EscapesACommentThatCarriesAQuote(t *testing.T) {
	c := qt.New(t)
	table := ast.NewCreateTable("customers").
		AddColumn(ast.NewColumn("id", "BIGINT").SetPrimary())
	table.Comment = "people who don't pay"

	sql, err := renderer.RenderSQL("postgres", table)

	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Contains, `IS 'people who don''t pay';`)
}
