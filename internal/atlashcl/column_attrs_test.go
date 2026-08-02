package atlashcl_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/atlashcl"
)

func TestParseColumnCheck(t *testing.T) {
	c := qt.New(t)

	db, err := atlashcl.Parse([]byte(`
table "t" {
  column "age" {
    type  = int
    check = "age > 0"
  }
}
`), "schema.hcl")
	c.Assert(err, qt.IsNil)
	c.Assert(db.Fields, qt.HasLen, 1)
	c.Assert(db.Fields[0].Check, qt.Equals, "age > 0")
	c.Assert(db.Fields[0].CheckName, qt.Equals, "")

	sql := legacyRenderedSQL(strings.Join(renderStatements(c, db, "postgres"), "\n"))
	c.Assert(sql, qt.Contains, `CHECK (age > 0)`)
}

func TestParseColumnCheckWithName(t *testing.T) {
	c := qt.New(t)

	db, err := atlashcl.Parse([]byte(`
table "t" {
  column "age" {
    type       = int
    check      = "age > 0"
    check_name = "age_positive"
  }
}
`), "schema.hcl")
	c.Assert(err, qt.IsNil)
	c.Assert(db.Fields, qt.HasLen, 1)
	c.Assert(db.Fields[0].CheckName, qt.Equals, "age_positive")

	sql := legacyRenderedSQL(strings.Join(renderStatements(c, db, "postgres"), "\n"))
	c.Assert(sql, qt.Contains, `CONSTRAINT age_positive CHECK (age > 0)`)
}

func TestParseColumnUniqueExpr(t *testing.T) {
	c := qt.New(t)

	db, err := atlashcl.Parse([]byte(`
table "t" {
  column "email" {
    type        = text
    unique_expr = "lower(email)"
  }
}
`), "schema.hcl")
	c.Assert(err, qt.IsNil)
	c.Assert(db.Fields, qt.HasLen, 1)
	c.Assert(db.Fields[0].UniqueExpr, qt.Equals, "lower(email)")
}

func TestParseColumnIdentityOptions(t *testing.T) {
	c := qt.New(t)

	db, err := atlashcl.Parse([]byte(`
table "t" {
  column "id" {
    type = bigint
    identity {
      generated = "ALWAYS"
      options   = "START WITH 100 INCREMENT BY 5"
    }
  }
}
`), "schema.hcl")
	c.Assert(err, qt.IsNil)
	c.Assert(db.Fields, qt.HasLen, 1)
	c.Assert(db.Fields[0].IdentityOptions, qt.Equals, "START WITH 100 INCREMENT BY 5")

	sql := legacyRenderedSQL(strings.Join(renderStatements(c, db, "postgres"), "\n"))
	c.Assert(sql, qt.Contains, `AS IDENTITY (START WITH 100 INCREMENT BY 5)`)
}
