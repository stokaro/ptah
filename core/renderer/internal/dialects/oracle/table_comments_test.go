package oracle_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/ast"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/platform/capability"
)

// TestCreateTable_CommentsAreStatements pins that a comment reaches the server.
//
// Both were written as SQL line comments above the statement -- decoration the
// server never reads -- so neither survived a replay. Measured on Oracle Free
// 23, a schema this repository did not write, replayed into an empty one:
//
//	source:  CUSTOMERS 'people who buy'   EMAIL 'login address'
//	replay:  CUSTOMERS <none>             EMAIL <none>
//
// `schema apply --dry-run` against the source reported `Schema is synced` and
// the replay reported success, so only reading the second schema's catalog
// could tell (stokaro/ptah#2132).
//
// Oracle has no inline form for either: a comment is its own statement, the way
// PostgreSQL writes one, and it follows the CREATE TABLE because the object has
// to exist before a comment can name it.
func TestCreateTable_CommentsAreStatements(t *testing.T) {
	c := qt.New(t)
	caps := capability.ForDialect(platform.Oracle)

	sql := render(c, caps, &ast.CreateTableNode{
		Name:    "customers",
		Comment: "people who buy",
		Columns: []*ast.ColumnNode{
			{Name: "id", Type: "INT", Primary: true},
			{Name: "email", Type: "VARCHAR(255)", Comment: "login address"},
			{Name: "bio", Type: "CLOB"},
		},
	})

	c.Assert(sql, qt.Contains, "COMMENT ON TABLE customers IS 'people who buy';")
	c.Assert(sql, qt.Contains, "COMMENT ON COLUMN customers.email IS 'login address';")
	// The column with no comment earns no statement. A loop that wrote one for
	// every column would put an empty comment on `bio`, and an empty comment is
	// a comment -- it is not the same as having none.
	c.Assert(sql, qt.Not(qt.Contains), "COMMENT ON COLUMN customers.bio")
	// And the comment is no longer decoration. Without this the statements
	// above could be added while the line comment stayed, which is two answers
	// to one question.
	c.Assert(sql, qt.Not(qt.Contains), "-- people who buy")
}

// TestCreateTable_CommentQuotesAreEscaped is the character a comment is most
// likely to hold. An unescaped one ends the literal, and the statement after it
// is whatever the rest of the comment happens to spell.
func TestCreateTable_CommentQuotesAreEscaped(t *testing.T) {
	c := qt.New(t)
	caps := capability.ForDialect(platform.Oracle)

	sql := render(c, caps, &ast.CreateTableNode{
		Name:    "customers",
		Comment: "what the buyer's account owns",
		Columns: []*ast.ColumnNode{{Name: "id", Type: "INT", Primary: true}},
	})

	c.Assert(sql, qt.Contains, "COMMENT ON TABLE customers IS 'what the buyer''s account owns';")
}

// TestCreateTable_WithoutACommentGainsNoStatement is the control for both.
//
// A table that carries no comment must render exactly what it rendered before,
// or every table in every document grows a statement nobody asked for.
func TestCreateTable_WithoutACommentGainsNoStatement(t *testing.T) {
	c := qt.New(t)
	caps := capability.ForDialect(platform.Oracle)

	sql := render(c, caps, &ast.CreateTableNode{
		Name:    "customers",
		Columns: []*ast.ColumnNode{{Name: "id", Type: "INT", Primary: true}},
	})

	c.Assert(sql, qt.Not(qt.Contains), "COMMENT ON")
}
