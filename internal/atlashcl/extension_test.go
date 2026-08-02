package atlashcl_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/atlashcl"
)

func TestParseExtensionIfNotExists(t *testing.T) {
	c := qt.New(t)

	db, err := atlashcl.Parse([]byte(`
extension "pg_trgm" {
  if_not_exists = true
  version       = "1.6"
}
`), "schema.hcl")
	c.Assert(err, qt.IsNil)
	c.Assert(db.Extensions, qt.HasLen, 1)
	c.Assert(db.Extensions[0].IfNotExists, qt.IsTrue)
	c.Assert(db.Extensions[0].Version, qt.Equals, "1.6")

	sql := legacyRenderedSQL(strings.Join(renderStatements(c, db, "postgres"), "\n"))
	c.Assert(sql, qt.Contains, `CREATE EXTENSION IF NOT EXISTS pg_trgm VERSION '1.6';`)
}

func TestParseExtensionDefaultsWithoutIfNotExists(t *testing.T) {
	c := qt.New(t)

	db, err := atlashcl.Parse([]byte(`
extension "pg_trgm" {}
`), "schema.hcl")
	c.Assert(err, qt.IsNil)
	c.Assert(db.Extensions, qt.HasLen, 1)
	c.Assert(db.Extensions[0].IfNotExists, qt.IsFalse)

	sql := legacyRenderedSQL(strings.Join(renderStatements(c, db, "postgres"), "\n"))
	c.Assert(sql, qt.Contains, `CREATE EXTENSION pg_trgm;`)
}

func TestParseExtensionRejectsNonBoolIfNotExists(t *testing.T) {
	c := qt.New(t)

	_, err := atlashcl.Parse([]byte(`
extension "pg_trgm" {
  if_not_exists = "yes"
}
`), "schema.hcl")
	c.Assert(err, qt.ErrorMatches, `.*extension attribute "if_not_exists" must be a bool.*`)
}
