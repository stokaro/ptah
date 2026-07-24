package goschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/core/goschema"
	"github.com/stokaro/ptah/core/ptaherr"
)

func TestParseDomainAnnotation(t *testing.T) {
	const src = `package fixture

//migrator:schema:domain name="email" schema="app" type="TEXT" not_null="true" check="VALUE ~ '@'" comment="Email"
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

//migrator:schema:domain name="email"
type EmailDomain struct{}
`
	c := qt.New(t)
	_, err := goschema.ParseSource("fixture.go", src)
	var parseErr *ptaherr.ParseError
	c.Assert(err, qt.ErrorAs, &parseErr)
	c.Assert(parseErr.Directive, qt.Equals, "migrator:schema:domain")
}

func TestParseCompositeAnnotation(t *testing.T) {
	const src = `package fixture

//migrator:schema:composite name="address" fields="street:TEXT,city:TEXT,zip:VARCHAR(10)"
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

func TestParseCompositeAnnotation_InvalidFieldsRejected(t *testing.T) {
	const src = `package fixture

//migrator:schema:composite name="address" fields="street"
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

//migrator:schema:range name="floatrange" subtype="float8" subtype_diff="float8mi"
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

func TestParseRangeAnnotation_MissingSubtypeRejected(t *testing.T) {
	const src = `package fixture

//migrator:schema:range name="floatrange"
type FloatRange struct{}
`
	c := qt.New(t)
	_, err := goschema.ParseSource("fixture.go", src)
	var parseErr *ptaherr.ParseError
	c.Assert(err, qt.ErrorAs, &parseErr)
	c.Assert(parseErr.Directive, qt.Equals, "migrator:schema:range")
}
