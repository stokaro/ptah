package postgres_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/engine/builtin/internal/dialects/postgres"
)

// nullsOrderedIndex declares an ascending key with a NULLS ordering, a
// descending one, and a key with neither.
func nullsOrderedIndex() *ast.IndexNode {
	return &ast.IndexNode{Name: "t_i", Table: "t", Parts: []ast.IndexPart{
		{Name: "a", NullsOrder: ast.NullsOrderFirst},
		{Name: "b", Desc: true, NullsOrder: ast.NullsOrderLast},
		{Name: "c"},
	}}
}

// TestRenderer_IndexNullsOrderOnYugabyteDB writes ASC before the NULLS
// ordering of an ascending key. YugabyteDB 2026.1.2 builds a leading key that
// names no direction as a HASH key and ignores its NULLS ordering, so
// `("a" NULLS FIRST)` read back as an index without one.
func TestRenderer_IndexNullsOrderOnYugabyteDB(t *testing.T) {
	c := qt.New(t)
	r := postgres.NewWithCapabilities(capability.YugabyteDB25(), platform.YugabyteDB)

	got, err := r.Render(nullsOrderedIndex())

	c.Assert(err, qt.IsNil)
	c.Assert(got, qt.Equals, "CREATE INDEX \"t_i\" ON \"t\" (\"a\" ASC NULLS FIRST, \"b\" DESC NULLS LAST, \"c\");\n")
}

// TestRenderer_IndexNullsOrderOnPostgres is the control: PostgreSQL has no
// HASH key, and an ascending key with a NULLS ordering keeps the spelling the
// pinned community binary writes.
func TestRenderer_IndexNullsOrderOnPostgres(t *testing.T) {
	c := qt.New(t)
	r := postgres.NewWithCapabilities(capability.Postgres17(), platform.Postgres)

	got, err := r.Render(nullsOrderedIndex())

	c.Assert(err, qt.IsNil)
	c.Assert(got, qt.Equals, "CREATE INDEX \"t_i\" ON \"t\" (\"a\" NULLS FIRST, \"b\" DESC NULLS LAST, \"c\");\n")
}
