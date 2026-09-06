package postgres

// White-box testing required: tableCommentExpr builds one expression of a
// shared query string, and the property under test is what that string
// contains. Reaching it from outside the package would mean a live server.

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform/capability"
)

// TestTableCommentProjection_NamesTheCatalog pins the two-argument form.
//
// `obj_description(oid)` with one argument searches every system catalog, and
// PostgreSQL's own documentation deprecates it for that reason: an OID is
// unique within a catalog and not across them.
//
// Measured on CockroachDB 25.4, an uncommented table answered
//
//	Calculates y-intercept of the least-squares-fit linear equation determined
//	by the (X, Y) pairs.
//
// which is a builtin function's documentation, reached through a colliding OID.
// The two-argument form answers NULL for that table and the comment for a
// commented one.
//
// Nothing noticed while the value was read and never compared. The first
// comparison that used it planned `COMMENT ON TABLE ... IS NULL` against every
// CockroachDB table, which is how this was found (stokaro/ptah#2168).
func TestTableCommentProjection_NamesTheCatalog(t *testing.T) {
	c := qt.New(t)
	reader := &Reader{caps: capability.ForDialect("postgres")}

	projection := reader.tableCommentExpr()

	c.Assert(projection, qt.Contains, "obj_description(c.oid, 'pg_class')")
	// The control: the deprecated single-argument spelling must be gone, not
	// merely accompanied. Asserting only on the two-argument form would pass on
	// a projection that still called the one-argument one somewhere else.
	c.Assert(strings.Count(projection, "obj_description("), qt.Equals, 1)
}

// A target whose catalog cannot answer is asked nothing at all, which is the
// gate the column projection has for the same reason.
func TestTableCommentProjection_AsksOnlyWhereTheCatalogCanAnswer(t *testing.T) {
	c := qt.New(t)
	reader := &Reader{caps: capability.Capabilities{}}

	projection := reader.tableCommentExpr()

	c.Assert(projection, qt.Not(qt.Contains), "obj_description")
	c.Assert(projection, qt.Contains, "table_comment")
}
