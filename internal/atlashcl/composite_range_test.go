package atlashcl_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/feature/oratype"
	"ptah.run/feature/pgtype"
	"ptah.run/internal/atlashcl"
	"ptah.run/internal/builtintest"
)

func TestParseCompositeType(t *testing.T) {
	c := qt.New(t)

	db, err := parseTypeHCL(t.Context(), []byte(`
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
	objects := typeObjects(db, pgtype.CompositeKind)
	c.Assert(objects, qt.HasLen, 1)
	c.Assert(objects[0].Ref, qt.DeepEquals, pgtype.Ref(pgtype.CompositeKind, "", "address"))
	c.Assert(objects[0].Value, qt.DeepEquals, &pgtype.DesiredComposite{Attributes: []pgtype.Attribute{
		{Name: "street", Type: "text"}, {Name: "zip", Type: "integer"},
	}})

	sql := legacyRenderedSQL(strings.Join(renderStatements(c, db, "postgres"), "\n"))
	c.Assert(sql, qt.Contains, `CREATE TYPE address AS (street text, zip integer);`)
}

func TestParseCompositeTypeWithSchema(t *testing.T) {
	c := qt.New(t)

	db, err := parseTypeHCL(t.Context(), []byte(`
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
	objects := typeObjects(db, pgtype.CompositeKind)
	c.Assert(objects, qt.HasLen, 1)
	c.Assert(objects[0].Ref, qt.DeepEquals, pgtype.Ref(pgtype.CompositeKind, "app", "point"))
}

func TestParseCompositeQuotedMultiWordFieldType(t *testing.T) {
	c := qt.New(t)

	// A multi-word type has no bare HCL spelling, so it must be quoted; the
	// quotes must be stripped from the stored type or the rendered DDL becomes a
	// quoted identifier for a nonexistent type.
	db, err := parseTypeHCL(t.Context(), []byte(`
composite "measurement" {
  field "value" {
    type = "double precision"
  }
}
`), "schema.hcl")
	c.Assert(err, qt.IsNil)
	objects := typeObjects(db, pgtype.CompositeKind)
	c.Assert(objects, qt.HasLen, 1)
	c.Assert(objects[0].Value.(*pgtype.DesiredComposite).Attributes, qt.DeepEquals, []pgtype.Attribute{{Name: "value", Type: "double precision"}})

	sql := legacyRenderedSQL(strings.Join(renderStatements(c, db, "postgres"), "\n"))
	c.Assert(sql, qt.Contains, `CREATE TYPE measurement AS (value double precision);`)
}

// TestParseUserTypes_AnOracleDocumentReachesTheOracleTypeOwner pins the
// routing of the blocks Oracle shares with PostgreSQL: a document read for
// Oracle, or a block whose dialect names Oracle, hands its domains and
// composite types to the Oracle type owner.
func TestParseUserTypes_AnOracleDocumentReachesTheOracleTypeOwner(t *testing.T) {
	for _, test := range []struct{ name, dialect, selector string }{
		{name: "an Oracle document", dialect: "oracle"},
		{name: "a block selecting Oracle", dialect: "postgres", selector: `dialect = "oracle"`},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			document := `
domain "email" {
  ` + test.selector + `
  type = varchar2(320)
  null = false
}
composite "address" {
  ` + test.selector + `
  field "street" {
    type = "VARCHAR2(80)"
  }
}
`
			db, err := atlashcl.ParseWithOptions(t.Context(), []byte(document), "schema.hcl", atlashcl.Options{HCL: builtintest.Runtime().HCL(), Dialect: test.dialect})
			c.Assert(err, qt.IsNil)
			objects := must.Must(db.FeatureObjects.All())
			c.Assert(objects, qt.HasLen, 2)
			c.Assert(objects[0].Ref, qt.DeepEquals, oratype.Ref(oratype.DomainKind, "", "email"))
			c.Assert(objects[0].Value, qt.DeepEquals, &oratype.DesiredDomain{BaseType: "varchar2(320)", NotNull: true})
			c.Assert(objects[1].Ref, qt.DeepEquals, oratype.Ref(oratype.ObjectTypeKind, "", "address"))
			c.Assert(objects[1].Value, qt.DeepEquals, &oratype.DesiredObjectType{Attributes: []oratype.Attribute{{Name: "street", Type: "VARCHAR2(80)"}}})

			c.Assert(renderStatements(c, db, "oracle"), qt.DeepEquals, []string{
				"CREATE DOMAIN IF NOT EXISTS email AS varchar2(320) NOT NULL;\n",
				"CREATE OR REPLACE TYPE address AS OBJECT (street VARCHAR2(80));\n",
			})
		})
	}
}

func TestParseCompositeRequiresField(t *testing.T) {
	c := qt.New(t)

	_, err := parseTypeHCL(t.Context(), []byte(`
composite "empty" {}
`), "schema.hcl")
	c.Assert(err, qt.ErrorMatches, `.*composite "empty" requires at least one field.*`)
}

func TestParseCompositeFieldRequiresType(t *testing.T) {
	c := qt.New(t)

	_, err := parseTypeHCL(t.Context(), []byte(`
composite "address" {
  field "street" {}
}
`), "schema.hcl")
	c.Assert(err, qt.ErrorMatches, `.*composite field "street" requires type.*`)
}

func TestParseCompositeRejectsUnknownBlock(t *testing.T) {
	c := qt.New(t)

	_, err := parseTypeHCL(t.Context(), []byte(`
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

	db, err := parseTypeHCL(t.Context(), []byte(`
range "floatrange" {
  subtype      = float8
  subtype_diff = float8mi
}
`), "schema.hcl")
	c.Assert(err, qt.IsNil)
	c.Assert(rangeObjects(db), qt.HasLen, 1)
	c.Assert(rangeObjects(db)[0].Ref.Name.Source, qt.Equals, "floatrange")
	c.Assert(rangeValues(db)[0].Subtype, qt.Equals, "float8")
	c.Assert(rangeValues(db)[0].SubtypeDiff, qt.Equals, "float8mi")

	sql := legacyRenderedSQL(strings.Join(renderStatements(c, db, "postgres"), "\n"))
	c.Assert(sql, qt.Contains, `CREATE TYPE floatrange AS RANGE (SUBTYPE = float8, SUBTYPE_DIFF = float8mi);`)
}

func TestParseRangeTypeAllOptions(t *testing.T) {
	c := qt.New(t)

	db, err := parseTypeHCL(t.Context(), []byte(`
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
	c.Assert(rangeObjects(db), qt.HasLen, 1)
	c.Assert(rangeObjects(db)[0].Ref.Schema.Authored(), qt.Equals, "app")
	c.Assert(rangeValues(db)[0].SubtypeOpClass, qt.Equals, "text_ops")
	c.Assert(rangeValues(db)[0].Collation, qt.Equals, "en_US")
	c.Assert(rangeValues(db)[0].Canonical, qt.Equals, "textrange_canonical")
	c.Assert(rangeValues(db)[0].Comment, qt.Equals, "text range")
}

func TestParseRangeRequiresSubtype(t *testing.T) {
	c := qt.New(t)

	_, err := parseTypeHCL(t.Context(), []byte(`
range "floatrange" {}
`), "schema.hcl")
	c.Assert(err, qt.ErrorMatches, `.*range "floatrange" requires subtype.*`)
}

func TestParseRangeRejectsUnknownAttribute(t *testing.T) {
	c := qt.New(t)

	_, err := parseTypeHCL(t.Context(), []byte(`
range "floatrange" {
  subtype  = float8
  nonsense = true
}
`), "schema.hcl")
	c.Assert(err, qt.ErrorMatches, `.*unsupported range attribute "nonsense".*`)
}
