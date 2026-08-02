package atlashcl_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/internal/atlashcl"
)

func TestParseIndexComment(t *testing.T) {
	c := qt.New(t)

	db, err := atlashcl.Parse([]byte(`
table "users" {
  column "email" {
    type = text
  }
  index "idx_users_email" {
    columns = [column.email]
    comment = "lookup by email"
  }
}
`), "schema.hcl")

	c.Assert(err, qt.IsNil)
	c.Assert(db.Indexes, qt.HasLen, 1)
	c.Assert(db.Indexes[0].Name, qt.Equals, "idx_users_email")
	c.Assert(db.Indexes[0].Fields, qt.DeepEquals, []string{"email"})
	c.Assert(db.Indexes[0].Comment, qt.Equals, "lookup by email")
}

func TestParseIndexCommentAbsentIsEmpty(t *testing.T) {
	c := qt.New(t)

	db, err := atlashcl.Parse([]byte(`
table "users" {
  column "email" {
    type = text
  }
  index "idx_users_email" {
    columns = [column.email]
  }
}
`), "schema.hcl")

	c.Assert(err, qt.IsNil)
	c.Assert(db.Indexes, qt.HasLen, 1)
	c.Assert(db.Indexes[0].Comment, qt.Equals, "")
}

// TestIndexCommentGoAnnotationParity asserts that the Go annotation frontend and
// the Atlas HCL frontend produce an equivalent index comment for the same
// schema, closing the #684 parity gap for index comments. Like the Go path
// (parseIndexComment), neither frontend normalizes or validates the value, so
// the comment string is carried through verbatim.
func TestIndexCommentGoAnnotationParity(t *testing.T) {
	c := qt.New(t)

	goDB, err := goschema.ParseSource("users.go", `package models

//ptah:schema:table name="users"
type User struct {
	//ptah:schema:field name="email" type="TEXT"
	Email string

	//ptah:schema:index name="idx_users_email" fields="email" comment="lookup by email"
	_ int
}
`)
	c.Assert(err, qt.IsNil)
	c.Assert(goDB.Indexes, qt.HasLen, 1)

	hclDB, err := atlashcl.Parse([]byte(`
table "users" {
  column "email" {
    type = text
  }
  index "idx_users_email" {
    columns = [column.email]
    comment = "lookup by email"
  }
}
`), "schema.hcl")
	c.Assert(err, qt.IsNil)
	c.Assert(hclDB.Indexes, qt.HasLen, 1)

	goIndex := goDB.Indexes[0]
	hclIndex := hclDB.Indexes[0]
	c.Assert(hclIndex.Name, qt.Equals, goIndex.Name)
	c.Assert(hclIndex.Fields, qt.DeepEquals, goIndex.Fields)
	c.Assert(hclIndex.Comment, qt.Equals, goIndex.Comment)
}
