package atlashcl_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/core/goschema"
	"github.com/stokaro/ptah/internal/atlashcl"
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
	c.Assert(db.RLSEnabledTables, qt.HasLen, 1)
	c.Assert(db.RLSEnabledTables[0].Table, qt.Equals, "users")
	c.Assert(db.RLSEnabledTables[0].Comment, qt.Equals, "tenant isolation")
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
	c.Assert(db.RLSEnabledTables, qt.HasLen, 1)
	c.Assert(db.RLSEnabledTables[0].Comment, qt.Equals, "")
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

//migrator:schema:table name="users"
type User struct {
	//migrator:schema:field name="id" type="INTEGER" primary="true"
	ID int
}

//migrator:schema:rls:enable table="users" comment="tenant isolation"
type SecurityMarker struct{}
`)
	c.Assert(err, qt.IsNil)
	c.Assert(goDB.RLSEnabledTables, qt.HasLen, 1)

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
	c.Assert(hclDB.RLSEnabledTables, qt.HasLen, 1)

	goRLS := goDB.RLSEnabledTables[0]
	hclRLS := hclDB.RLSEnabledTables[0]
	c.Assert(hclRLS.Table, qt.Equals, goRLS.Table)
	c.Assert(hclRLS.Comment, qt.Equals, goRLS.Comment)
}
