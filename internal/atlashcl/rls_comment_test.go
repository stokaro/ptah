package atlashcl_test

import (
	"maps"
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/feature/pgpolicy"

	"ptah.run/core/goschema"
	"ptah.run/internal/atlashcl"
)

func TestParseRowSecurityComment(t *testing.T) {
	c := qt.New(t)

	db, err := atlashcl.Parse([]byte(`
table "users" {
  column "id" {
    type = int
  }
  row_security {
    enabled = true
    comment = "tenant isolation"
  }
}
`), "schema.hcl")

	c.Assert(err, qt.IsNil)
	c.Assert(ownerSwitches(c, db)["users"].Comment, qt.Equals, "tenant isolation")
}

func TestParseRowSecurityCommentAbsentIsEmpty(t *testing.T) {
	c := qt.New(t)

	db, err := atlashcl.Parse([]byte(`
table "users" {
  column "id" {
    type = int
  }
  row_security {
    enabled = true
  }
}
`), "schema.hcl")

	c.Assert(err, qt.IsNil)
	c.Assert(ownerSwitches(c, db), qt.DeepEquals, map[string]pgpolicy.DesiredTableState{"users": {Enabled: true, StructName: "users"}})
}

// TestRowSecurityCommentGoAnnotationParity asserts that the Go annotation
// frontend and the Atlas HCL frontend produce an equivalent RLS enablement
// comment for the same schema, closing the #684 parity gap for row_security
// comments. Like the Go path (parseFileScopedRLSEnableComment), neither frontend
// normalizes or validates the value, so the comment string is carried through
// verbatim.
func TestRowSecurityCommentGoAnnotationParity(t *testing.T) {
	c := qt.New(t)

	goDB, err := goschema.ParseSource("users.go", `package models

//ptah:schema:table name="users"
type User struct {
	//ptah:schema:field name="id" type="INTEGER" primary="true"
	ID int
}

//ptah:schema:rls:enable table="users" comment="tenant isolation"
type SecurityMarker struct{}
`)
	c.Assert(err, qt.IsNil)

	hclDB, err := atlashcl.Parse([]byte(`
table "users" {
  column "id" {
    type = int
  }
  row_security {
    enabled = true
    comment = "tenant isolation"
  }
}
`), "schema.hcl")
	c.Assert(err, qt.IsNil)
	goRLS := ownerSwitches(c, &goDB)
	hclRLS := ownerSwitches(c, hclDB)
	c.Assert(slices.Collect(maps.Keys(hclRLS)), qt.DeepEquals, slices.Collect(maps.Keys(goRLS)))
	c.Assert(hclRLS["users"].Comment, qt.Equals, goRLS["users"].Comment)
}
