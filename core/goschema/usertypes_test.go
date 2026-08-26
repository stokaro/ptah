package goschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/ptaherr"
)

func TestParseDomainAnnotation(t *testing.T) {
	const src = `package fixture

//ptah:schema:domain name="email" schema="app" type="TEXT" not_null="true" check="VALUE ~ '@'" comment="Email"
type EmailDomain struct{}
`
	c := qt.New(t)
	db := mustParseSource(c, "fixture.go", src)
	c.Assert(db.Domains, qt.HasLen, 1)
	d := db.Domains[0]
	c.Assert(d.Name, qt.Equals, "email")
	c.Assert(d.Schema, qt.Equals, "app")
	c.Assert(d.BaseType, qt.Equals, "TEXT")
	c.Assert(d.NotNull, qt.IsTrue)
	c.Assert(d.Check, qt.Equals, "VALUE ~ '@'")
	c.Assert(d.QualifiedName(), qt.Equals, "app.email")
}

func TestParseDomainAnnotation_MissingTypeRejected(t *testing.T) {
	const src = `package fixture

//ptah:schema:domain name="email"
type EmailDomain struct{}
`
	c := qt.New(t)
	_, err := goschema.ParseSource("fixture.go", src)
	var parseErr *ptaherr.ParseError
	c.Assert(err, qt.ErrorAs, &parseErr)
	c.Assert(parseErr.Directive, qt.Equals, "ptah:schema:domain")
}

func TestParseCompositeAnnotation(t *testing.T) {
	const src = `package fixture

//ptah:schema:composite name="address" fields="street:TEXT,city:TEXT,zip:VARCHAR(10)"
type AddressType struct{}
`
	c := qt.New(t)
	db := mustParseSource(c, "fixture.go", src)
	c.Assert(db.CompositeTypes, qt.HasLen, 1)
	comp := db.CompositeTypes[0]
	c.Assert(comp.Name, qt.Equals, "address")
	c.Assert(comp.Fields, qt.HasLen, 3)
	c.Assert(comp.Fields[0].Name, qt.Equals, "street")
	c.Assert(comp.Fields[0].Type, qt.Equals, "TEXT")
	c.Assert(comp.Fields[2].Name, qt.Equals, "zip")
	c.Assert(comp.Fields[2].Type, qt.Equals, "VARCHAR(10)")
}

func TestParseCompositeAnnotation_ParameterizedTypesWithCommas(t *testing.T) {
	const src = `package fixture

//ptah:schema:composite name="money" fields="amount:NUMERIC(10,2),cur:VARCHAR(3)"
type MoneyType struct{}
`
	c := qt.New(t)
	db := mustParseSource(c, "fixture.go", src)
	c.Assert(db.CompositeTypes, qt.HasLen, 1)
	comp := db.CompositeTypes[0]
	c.Assert(comp.Fields, qt.HasLen, 2)
	c.Assert(comp.Fields[0].Name, qt.Equals, "amount")
	c.Assert(comp.Fields[0].Type, qt.Equals, "NUMERIC(10,2)")
	c.Assert(comp.Fields[1].Type, qt.Equals, "VARCHAR(3)")
}

func TestParseCompositeAnnotation_InvalidFieldsRejected(t *testing.T) {
	const src = `package fixture

//ptah:schema:composite name="address" fields="street"
type AddressType struct{}
`
	c := qt.New(t)
	_, err := goschema.ParseSource("fixture.go", src)
	var parseErr *ptaherr.ParseError
	c.Assert(err, qt.ErrorAs, &parseErr)
	c.Assert(parseErr.Attribute, qt.Equals, "fields")
}

func TestParseRangeAnnotation(t *testing.T) {
	const src = `package fixture

//ptah:schema:range name="floatrange" subtype="float8" subtype_diff="float8mi"
type FloatRange struct{}
`
	c := qt.New(t)
	db := mustParseSource(c, "fixture.go", src)
	c.Assert(db.Ranges, qt.HasLen, 1)
	r := db.Ranges[0]
	c.Assert(r.Name, qt.Equals, "floatrange")
	c.Assert(r.Subtype, qt.Equals, "float8")
	c.Assert(r.SubtypeDiff, qt.Equals, "float8mi")
}

// TestParseRangeAnnotation_AnEmptyAttributeIsToldFromAnAbsentOne pins the
// distinction the comparator needs.
//
// An omitted attribute and one written `key=""` reach the same empty string in
// Range, and only the key's presence separates them. Omission means "say
// nothing about this", which is what keeps adoption over an existing database
// from planning away a SUBTYPE_DIFF the author never mentioned; an empty value
// means "this range has none", which is the only spelling that removes one
// (stokaro/ptah#2223).
func TestParseRangeAnnotation_AnEmptyAttributeIsToldFromAnAbsentOne(t *testing.T) {
	const src = `package fixture

//ptah:schema:range name="floatrange" subtype="float8" subtype_diff="" canonical="float8canon"
type FloatRange struct{}
`
	c := qt.New(t)
	db := mustParseSource(c, "fixture.go", src)
	c.Assert(db.Ranges, qt.HasLen, 1)
	r := db.Ranges[0]

	// Written empty: cleared.
	c.Assert(r.SubtypeDiff, qt.Equals, "")
	c.Assert(r.Clears("subtype_diff"), qt.IsTrue)

	// Written with a value: not cleared, and the value survives.
	c.Assert(r.Canonical, qt.Equals, "float8canon")
	c.Assert(r.Clears("canonical"), qt.IsFalse)

	// Never written at all: not cleared either, which is the case that must not
	// be confused with the first.
	c.Assert(r.Collation, qt.Equals, "")
	c.Assert(r.Clears("collation"), qt.IsFalse)
	c.Assert(r.ClearedAttributes, qt.DeepEquals, []string{"subtype_diff"})
}

func TestParseRangeAnnotation_MissingSubtypeRejected(t *testing.T) {
	const src = `package fixture

//ptah:schema:range name="floatrange"
type FloatRange struct{}
`
	c := qt.New(t)
	_, err := goschema.ParseSource("fixture.go", src)
	var parseErr *ptaherr.ParseError
	c.Assert(err, qt.ErrorAs, &parseErr)
	c.Assert(parseErr.Directive, qt.Equals, "ptah:schema:range")
}
