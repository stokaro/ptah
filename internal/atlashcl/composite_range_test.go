package atlashcl_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/atlashcl"
)

func TestParseCompositeType(t *testing.T) {
	c := qt.New(t)

	db, err := atlashcl.Parse([]byte(`
composite "address" {
  field "street" {
    type = text
  }
  field "zip" {
    type = integer
  }
}
`), "schema.hcl")
	c.Assert(err, qt.IsNil)
	c.Assert(db.CompositeTypes, qt.HasLen, 1)
	c.Assert(db.CompositeTypes[0].Name, qt.Equals, "address")
	c.Assert(db.CompositeTypes[0].Fields, qt.HasLen, 2)
	c.Assert(db.CompositeTypes[0].Fields[0].Name, qt.Equals, "street")
	c.Assert(db.CompositeTypes[0].Fields[0].Type, qt.Equals, "text")
	c.Assert(db.CompositeTypes[0].Fields[1].Name, qt.Equals, "zip")

	sql := legacyRenderedSQL(strings.Join(renderStatements(c, db, "postgres"), "\n"))
	c.Assert(sql, qt.Contains, `CREATE TYPE address AS (street text, zip integer);`)
}

func TestParseCompositeTypeWithSchema(t *testing.T) {
	c := qt.New(t)

	db, err := atlashcl.Parse([]byte(`
composite "app" "point" {
  field "x" {
    type = float8
  }
  field "y" {
    type = float8
  }
}
`), "schema.hcl")
	c.Assert(err, qt.IsNil)
	c.Assert(db.CompositeTypes, qt.HasLen, 1)
	c.Assert(db.CompositeTypes[0].Name, qt.Equals, "point")
	c.Assert(db.CompositeTypes[0].Schema, qt.Equals, "app")
}

func TestParseCompositeQuotedMultiWordFieldType(t *testing.T) {
	c := qt.New(t)

	// A multi-word type has no bare HCL spelling, so it must be quoted; the
	// quotes must be stripped from the stored type or the rendered DDL becomes a
	// quoted identifier for a nonexistent type.
	db, err := atlashcl.Parse([]byte(`
composite "measurement" {
  field "value" {
    type = "double precision"
  }
}
`), "schema.hcl")
	c.Assert(err, qt.IsNil)
	c.Assert(db.CompositeTypes, qt.HasLen, 1)
	c.Assert(db.CompositeTypes[0].Fields[0].Type, qt.Equals, "double precision")

	sql := legacyRenderedSQL(strings.Join(renderStatements(c, db, "postgres"), "\n"))
	c.Assert(sql, qt.Contains, `CREATE TYPE measurement AS (value double precision);`)
}

func TestParseCompositeRequiresField(t *testing.T) {
	c := qt.New(t)

	_, err := atlashcl.Parse([]byte(`
composite "empty" {}
`), "schema.hcl")
	c.Assert(err, qt.ErrorMatches, `.*composite "empty" requires at least one field.*`)
}

func TestParseCompositeFieldRequiresType(t *testing.T) {
	c := qt.New(t)

	_, err := atlashcl.Parse([]byte(`
composite "address" {
  field "street" {}
}
`), "schema.hcl")
	c.Assert(err, qt.ErrorMatches, `.*composite field "street" requires type.*`)
}

func TestParseCompositeRejectsUnknownBlock(t *testing.T) {
	c := qt.New(t)

	_, err := atlashcl.Parse([]byte(`
composite "address" {
  column "street" {
    type = text
  }
}
`), "schema.hcl")
	c.Assert(err, qt.ErrorMatches, `.*unsupported composite block "column".*`)
}

func TestParseRangeType(t *testing.T) {
	c := qt.New(t)

	db, err := atlashcl.Parse([]byte(`
range "floatrange" {
  subtype      = float8
  subtype_diff = float8mi
}
`), "schema.hcl")
	c.Assert(err, qt.IsNil)
	c.Assert(db.Ranges, qt.HasLen, 1)
	c.Assert(db.Ranges[0].Name, qt.Equals, "floatrange")
	c.Assert(db.Ranges[0].Subtype, qt.Equals, "float8")
	c.Assert(db.Ranges[0].SubtypeDiff, qt.Equals, "float8mi")

	sql := legacyRenderedSQL(strings.Join(renderStatements(c, db, "postgres"), "\n"))
	c.Assert(sql, qt.Contains, `CREATE TYPE floatrange AS RANGE (SUBTYPE = float8, SUBTYPE_DIFF = float8mi);`)
}

func TestParseRangeTypeAllOptions(t *testing.T) {
	c := qt.New(t)

	db, err := atlashcl.Parse([]byte(`
range "app" "textrange" {
  subtype         = text
  subtype_opclass = text_ops
  collation       = "en_US"
  canonical       = textrange_canonical
  subtype_diff    = textrange_diff
  comment         = "text range"
}
`), "schema.hcl")
	c.Assert(err, qt.IsNil)
	c.Assert(db.Ranges, qt.HasLen, 1)
	c.Assert(db.Ranges[0].Schema, qt.Equals, "app")
	c.Assert(db.Ranges[0].SubtypeOpClass, qt.Equals, "text_ops")
	c.Assert(db.Ranges[0].Collation, qt.Equals, "en_US")
	c.Assert(db.Ranges[0].Canonical, qt.Equals, "textrange_canonical")
	c.Assert(db.Ranges[0].Comment, qt.Equals, "text range")
}

func TestParseRangeRequiresSubtype(t *testing.T) {
	c := qt.New(t)

	_, err := atlashcl.Parse([]byte(`
range "floatrange" {}
`), "schema.hcl")
	c.Assert(err, qt.ErrorMatches, `.*range "floatrange" requires subtype.*`)
}

func TestParseRangeRejectsUnknownAttribute(t *testing.T) {
	c := qt.New(t)

	_, err := atlashcl.Parse([]byte(`
range "floatrange" {
  subtype  = float8
  nonsense = true
}
`), "schema.hcl")
	c.Assert(err, qt.ErrorMatches, `.*unsupported range attribute "nonsense".*`)
}
