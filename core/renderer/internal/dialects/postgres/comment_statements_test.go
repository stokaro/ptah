package postgres_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/renderer"
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

// TestRenderer_WritesAConstraintCommentAsAStatement covers stokaro/ptah#2611.
// `//ptah:schema:constraint comment="..."` is a documented attribute that
// reached no statement on any target, so an author described a constraint and
// the database held one nobody described — the same shape #2101 fixed for
// tables and columns, one object kind later.
func TestRenderer_WritesAConstraintCommentAsAStatement(t *testing.T) {
	c := qt.New(t)
	table := ast.NewCreateTable("customers").
		AddColumn(ast.NewColumn("id", "BIGINT").SetPrimary()).
		AddColumn(ast.NewColumn("email", "VARCHAR(200)"))
	table.AddConstraint(&ast.ConstraintNode{
		Type:       ast.CheckConstraint,
		Name:       "ck_customers_email",
		Expression: "email <> ''",
		Comment:    "an address is not the empty string",
	})

	sql, err := renderer.RenderSQL("postgres", table)

	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Contains,
		`COMMENT ON CONSTRAINT "ck_customers_email" ON "customers" IS 'an address is not the empty string';`)
}

// TestRenderer_WritesNoCommentForAnUnnamedConstraint is the control for the one
// case the statement cannot address. COMMENT ON CONSTRAINT names the
// constraint, and the name PostgreSQL generates for an anonymous one is not
// knowable from here — writing the comment onto a guessed name would put it on
// the wrong constraint, which is worse than not writing it.
func TestRenderer_WritesNoCommentForAnUnnamedConstraint(t *testing.T) {
	c := qt.New(t)
	table := ast.NewCreateTable("customers").
		AddColumn(ast.NewColumn("id", "BIGINT").SetPrimary())
	table.AddConstraint(&ast.ConstraintNode{
		Type:       ast.CheckConstraint,
		Expression: "id > 0",
		Comment:    "keys are positive",
	})

	sql, err := renderer.RenderSQL("postgres", table)

	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Not(qt.Contains), "COMMENT ON CONSTRAINT")
}

// TestRenderer_WritesAConstraintCommentOnTheAlterPath is the half a render-only
// fix leaves out, and it is the half that reaches an operator's database.
//
// `ptah schema apply` adds a constraint to an existing table through ALTER
// rather than CREATE. Measured on PostgreSQL 18: with only the CREATE path
// taught, `schema render` printed the comment and every applied database was
// left without it — the defect intact, behind output that said otherwise.
func TestRenderer_WritesAConstraintCommentOnTheAlterPath(t *testing.T) {
	c := qt.New(t)
	alter := &ast.AlterTableNode{
		Name: "customers",
		Operations: []ast.AlterOperation{
			&ast.AddConstraintOperation{Constraint: &ast.ConstraintNode{
				Type:       ast.CheckConstraint,
				Name:       "ck_customers_email",
				Expression: "email <> ''",
				Comment:    "an address is not the empty string",
			}},
		},
	}

	sql, err := renderer.RenderSQL("postgres", alter)

	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Contains, `ALTER TABLE "customers" ADD CONSTRAINT "ck_customers_email"`)
	c.Assert(sql, qt.Contains,
		`COMMENT ON CONSTRAINT "ck_customers_email" ON "customers" IS 'an address is not the empty string';`)
}

// TestRenderer_EscapesAConstraintCommentThatCarriesAQuote pins the escaping for
// the new statement rather than trusting that it shares the table's path.
func TestRenderer_EscapesAConstraintCommentThatCarriesAQuote(t *testing.T) {
	c := qt.New(t)
	table := ast.NewCreateTable("customers").
		AddColumn(ast.NewColumn("id", "BIGINT").SetPrimary())
	table.AddConstraint(&ast.ConstraintNode{
		Type:       ast.CheckConstraint,
		Name:       "ck_customers_id",
		Expression: "id > 0",
		Comment:    "keys that don't go backwards",
	})

	sql, err := renderer.RenderSQL("postgres", table)

	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Contains, `IS 'keys that don''t go backwards';`)
}
