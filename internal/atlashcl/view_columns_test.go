package atlashcl_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/atlashcl"
)

// viewColumnDocument renders a table and a view-like block carrying the given
// body and nested lines.
func viewColumnDocument(blockType, body, nested string) []byte {
	return []byte(`
schema "public" {}

table "t" {
  schema = schema.public
  column "id" {
    type = int
    null = true
  }
  column "label" {
    type = text
    null = true
  }
}

` + blockType + ` "v" {
  schema = schema.public
  as     = "` + body + `"
  ` + nested + `
}
`)
}

// TestParseViewColumnsNameTheOutput_HappyPath pins that a view's `column`
// blocks name the columns it returns.
//
// PostgreSQL accepts `CREATE VIEW v (a, b) AS SELECT x, y` and stores `SELECT x
// AS a, y AS b`, which is what pg_get_viewdef reports and what a reader hands
// the comparison. Rendering the alias list would never match the view's own
// catalog row, so the declaration is rewritten into the spelling the catalog
// keeps: the two statements create one view (stokaro/ptah#3172).
func TestParseViewColumnsNameTheOutput_HappyPath(t *testing.T) {
	rows := []struct {
		name      string
		blockType string
	}{
		{name: "view", blockType: "view"},
		{name: "materialized", blockType: "materialized"},
	}

	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)

			db, err := atlashcl.Parse(viewColumnDocument(
				row.blockType,
				"SELECT id, label FROM t",
				"column \"ident\" {\n  }\n  column \"name\" {\n  }",
			), "schema.hcl")

			c.Assert(err, qt.IsNil)
			sql := strings.Join(renderStatements(c, db, "postgres"), "\n")
			c.Assert(sql, qt.Contains, "SELECT id AS ident, label AS name FROM t")
			// The alias list is not rendered: the server would fold it into the
			// body anyway, and the comparison reads the body.
			c.Assert(sql, qt.Not(qt.Contains), `"v" (ident, name)`)
		})
	}
}

// TestParseViewWithoutColumnsIsUnchanged_HappyPath is the control.
//
// A view that declares no columns must render the body byte for byte as its
// author wrote it, or reading the blocks would have been bought by rewriting
// every view that does not use them.
func TestParseViewWithoutColumnsIsUnchanged_HappyPath(t *testing.T) {
	c := qt.New(t)

	db, err := atlashcl.Parse(viewColumnDocument("view", "SELECT id, label FROM t", ""), "schema.hcl")

	c.Assert(err, qt.IsNil)
	c.Assert(db.Views, qt.HasLen, 1)
	c.Assert(db.Views[0].Body, qt.Equals, "SELECT id, label FROM t")
}

// TestParseViewColumns_FailurePath pins what is refused rather than rewritten.
//
// A wrong split gives the author's columns the wrong names, and the view the
// server ends up holding is the one nobody declared, which no diagnostic
// downstream could report.
func TestParseViewColumns_FailurePath(t *testing.T) {
	rows := []struct {
		name   string
		body   string
		nested string
		want   string
	}{
		{
			name:   "a type the server derives",
			body:   "SELECT id FROM t",
			nested: "column \"ident\" {\n    type = int\n  }",
			want:   `unsupported view column attribute "type"`,
		},
		{
			name:   "more select items than columns",
			body:   "SELECT id, label FROM t",
			nested: "column \"ident\" {\n  }",
			want:   `2 select items for 1 declared columns`,
		},
		{
			name:   "fewer select items than columns",
			body:   "SELECT id FROM t",
			nested: "column \"a\" {\n  }\n  column \"b\" {\n  }",
			want:   `1 select items for 2 declared columns`,
		},
		{
			name:   "a star nobody can count",
			body:   "SELECT * FROM t",
			nested: "column \"a\" {\n  }\n  column \"b\" {\n  }",
			want:   `1 select items for 2 declared columns`,
		},
		{
			name:   "a column with no name label",
			body:   "SELECT id FROM t",
			nested: "column {\n  }",
			want:   `column requires one name label`,
		},
		{
			name:   "a block nested in a column",
			body:   "SELECT id FROM t",
			nested: "column \"ident\" {\n    zzz_nonsense {\n    }\n  }",
			want:   `unsupported view column block "zzz_nonsense"`,
		},
	}

	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)

			db, err := atlashcl.Parse(viewColumnDocument("view", row.body, row.nested), "schema.hcl")

			c.Assert(err, qt.ErrorMatches, `(?s).*`+row.want+`.*`)
			c.Assert(db, qt.IsNil)
		})
	}
}
