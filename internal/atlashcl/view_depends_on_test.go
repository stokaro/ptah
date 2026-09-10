package atlashcl_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/atlashcl"
)

// dependsOnDocument renders two views, giving the first the declared edge.
//
// Neither body mentions the other, so the ordering has nothing to infer from
// and the declared edge is the only thing that can move them.
func dependsOnDocument(edge string) []byte {
	return []byte(`
schema "public" {}

view "aa_first" {
  schema = schema.public
  as     = "SELECT 1 AS v"
  ` + edge + `
}

view "zz_second" {
  schema = schema.public
  as     = "SELECT 2 AS v"
}
`)
}

// TestParseViewDependsOnOrdersTheRender_HappyPath pins that a declared edge
// decides the order.
//
// The ordering reads each body for the names it mentions, which is the only
// evidence it has and not always the whole truth: a view reaching a table
// through a function, a search_path, or a name this dialect's reader does not
// resolve depends on something no scan of the text can find. Refusing the
// attribute left an author no way to say so, and failed the whole document
// (stokaro/ptah#3121).
func TestParseViewDependsOnOrdersTheRender_HappyPath(t *testing.T) {
	rows := []struct {
		name   string
		edge   string
		first  string
		second string
	}{
		{
			name:   "a declared edge",
			edge:   `depends_on = [view.zz_second]`,
			first:  `"public"."zz_second"`,
			second: `"public"."aa_first"`,
		},
		{
			name:   "no edge leaves the order alone",
			edge:   ``,
			first:  `"public"."aa_first"`,
			second: `"public"."zz_second"`,
		},
	}

	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)

			db, err := atlashcl.Parse(dependsOnDocument(row.edge), "schema.hcl")

			c.Assert(err, qt.IsNil)
			c.Assert(db.Views, qt.HasLen, 2)
			sql := strings.Join(renderStatements(c, db, "postgres"), "\n")
			c.Assert(sql, qt.Contains, row.first)
			c.Assert(sql, qt.Contains, row.second)
			c.Assert(strings.Index(sql, row.first) < strings.Index(sql, row.second), qt.IsTrue,
				qt.Commentf("expected %s before %s:\n%s", row.first, row.second, sql))
		})
	}
}

// TestParseViewDependsOnNamesAnObjectTheSetDoesNotHold_HappyPath pins that an
// edge pointing at nothing is not an error.
//
// The set being ordered is what this render carries, and an object scoped to
// another dialect is absent from it by design. Refusing the document would turn
// a correct `dialects` scope into a parse failure on every other target.
func TestParseViewDependsOnNamesAnObjectTheSetDoesNotHold_HappyPath(t *testing.T) {
	c := qt.New(t)

	db, err := atlashcl.Parse(dependsOnDocument(`depends_on = [view.absent_elsewhere]`), "schema.hcl")

	c.Assert(err, qt.IsNil)
	c.Assert(db.Views, qt.HasLen, 2)
	c.Assert(db.Views[0].DependsOn, qt.DeepEquals, []string{"absent_elsewhere"})
	sql := strings.Join(renderStatements(c, db, "postgres"), "\n")
	c.Assert(sql, qt.Contains, `CREATE VIEW "public"."aa_first"`)
}

// TestParseMaterializedDependsOn_HappyPath pins the same attribute on the other
// view-like block, which is ordered by the same sort.
func TestParseMaterializedDependsOn_HappyPath(t *testing.T) {
	c := qt.New(t)

	db, err := atlashcl.Parse([]byte(`
schema "public" {}

materialized "aa_roll" {
  schema     = schema.public
  as         = "SELECT 1 AS v"
  depends_on = [view.zz_base]
}

view "zz_base" {
  schema = schema.public
  as     = "SELECT 2 AS v"
}
`), "schema.hcl")

	c.Assert(err, qt.IsNil)
	c.Assert(db.MaterializedViews, qt.HasLen, 1)
	c.Assert(db.MaterializedViews[0].DependsOn, qt.DeepEquals, []string{"zz_base"})
	sql := strings.Join(renderStatements(c, db, "postgres"), "\n")
	c.Assert(strings.Index(sql, `"public"."zz_base"`) < strings.Index(sql, `"public"."aa_roll"`), qt.IsTrue,
		qt.Commentf("the declared edge did not order the render:\n%s", sql))
}
