package atlashcl_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/atlashcl"
)

// tableDependsOnDocument renders two tables with no reference between them,
// giving the first the declared edge.
//
// No foreign key joins them, so the sort has nothing to infer from and the
// declared edge is the only thing that can move them.
func tableDependsOnDocument(edge string) []byte {
	return []byte(`
schema "public" {}

table "aa_first" {
  schema = schema.public
  ` + edge + `
  column "id" {
    type = int
  }
}

table "zz_second" {
  schema = schema.public
  column "id" {
    type = int
  }
}
`)
}

// TestParseTableDependsOnOrdersTheRender_HappyPath pins that a declared edge
// decides the order of two tables no reference joins.
//
// A foreign key is the only evidence the sort has, and it is not always the
// whole truth: a table whose default calls a function reading another table, or
// whose rows load in an order the schema does not express, depends on something
// no reference states. Refusing the attribute left an author no way to say so,
// and failed the whole document (stokaro/ptah#3113).
func TestParseTableDependsOnOrdersTheRender_HappyPath(t *testing.T) {
	rows := []struct {
		name   string
		edge   string
		first  string
		second string
	}{
		{
			name:   "a declared edge",
			edge:   `depends_on = [table.zz_second]`,
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

			db, err := atlashcl.Parse(tableDependsOnDocument(row.edge), "schema.hcl")

			c.Assert(err, qt.IsNil)
			c.Assert(db.Tables, qt.HasLen, 2)
			sql := strings.Join(renderStatements(c, db, "postgres"), "\n")
			c.Assert(sql, qt.Contains, row.first)
			c.Assert(sql, qt.Contains, row.second)
			c.Assert(strings.Index(sql, row.first) < strings.Index(sql, row.second), qt.IsTrue,
				qt.Commentf("expected %s before %s:\n%s", row.first, row.second, sql))
		})
	}
}

// TestParseTableDependsOnNamesATableTheSchemaDoesNotHold_HappyPath pins that an
// edge pointing at nothing is not an error.
//
// The set being ordered is what this render carries, and a table scoped to
// another dialect is absent from it by design, so refusing would turn a correct
// `dialects` scope into a parse failure on every other target.
func TestParseTableDependsOnNamesATableTheSchemaDoesNotHold_HappyPath(t *testing.T) {
	c := qt.New(t)

	db, err := atlashcl.Parse(tableDependsOnDocument(`depends_on = [table.absent_elsewhere]`), "schema.hcl")

	c.Assert(err, qt.IsNil)
	c.Assert(db.Tables, qt.HasLen, 2)
	sql := strings.Join(renderStatements(c, db, "postgres"), "\n")
	c.Assert(sql, qt.Contains, `CREATE TABLE "public"."aa_first"`)
}
