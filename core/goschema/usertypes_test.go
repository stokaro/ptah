package goschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/annotation"
	"ptah.run/core/goschema"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemamodel"
	"ptah.run/feature/pgtype"
	"ptah.run/internal/builtintest"
	"ptah.run/internal/pgtypeprovider"
	"ptah.run/internal/pgtypesource"
)

// TestParseUserTypeAnnotation_NoOwnerReadsIt_FailurePath pins that a domain
// or composite type declaration no selected owner reads is refused: no model
// holds it, so keeping it would drop it in silence. A target with no type
// owner, such as MySQL, is refused the same way as a parse with no owner.
func TestParseUserTypeAnnotation_NoOwnerReadsIt_FailurePath(t *testing.T) {
	for _, test := range []struct {
		name, src, wantErr string
		owners             annotation.Set
	}{
		{
			name:    "a domain without owners",
			src:     "package fixture\n\n//ptah:schema:domain name=\"email\" type=\"TEXT\"\ntype EmailDomain struct{}\n",
			owners:  noOwners,
			wantErr: `(?s).*no selected owner reads a domain scoped to no target.*`,
		},
		{
			name:    "a composite type without owners",
			src:     "package fixture\n\n//ptah:schema:composite name=\"address\" fields=\"street:TEXT\"\ntype AddressType struct{}\n",
			owners:  noOwners,
			wantErr: `(?s).*no selected owner reads a composite type scoped to no target.*`,
		},
		{
			name:    "a domain scoped to MySQL",
			src:     "package fixture\n\n//ptah:schema:domain name=\"email\" type=\"TEXT\" dialects=\"mysql\"\ntype EmailDomain struct{}\n",
			owners:  builtintest.Annotations(),
			wantErr: `(?s).*no selected owner reads a domain scoped to mysql.*`,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			db, err := goschema.ParseSource(test.owners, "fixture.go", test.src)

			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(db.FeatureObjects.Len(), qt.Equals, 0)
		})
	}
}

func TestParseDomainAnnotation_MissingTypeRejected(t *testing.T) {
	const src = `package fixture

//ptah:schema:domain name="email"
type EmailDomain struct{}
`
	c := qt.New(t)
	_, err := goschema.ParseSource(builtintest.Annotations(), "fixture.go", src)
	var parseErr *ptaherr.ParseError
	c.Assert(err, qt.ErrorAs, &parseErr)
	c.Assert(parseErr.Directive, qt.Equals, "ptah:schema:domain")
}

func TestParseCompositeAnnotation_InvalidFieldsRejected(t *testing.T) {
	const src = `package fixture

//ptah:schema:composite name="address" fields="street"
type AddressType struct{}
`
	c := qt.New(t)
	_, err := goschema.ParseSource(builtintest.Annotations(), "fixture.go", src)
	var parseErr *ptaherr.ParseError
	c.Assert(err, qt.ErrorAs, &parseErr)
	c.Assert(parseErr.Directive, qt.Equals, "ptah:schema:composite")
	c.Assert(err, qt.ErrorMatches, `expected ":" at position 6 on //ptah:schema:composite at AddressType`)
}

func TestParseRangeAnnotation(t *testing.T) {
	const src = `package fixture

//ptah:schema:range name="floatrange" subtype="float8" subtype_diff="float8mi"
type FloatRange struct{}
`
	c := qt.New(t)
	db := must.Must(goschema.ParseSource(must.Must(annotation.NewSet(pgtypesource.Annotations(pgtypeprovider.Targets()))), "fixture.go", src))
	r := parsedRange(c, db)
	c.Assert(must.Must(db.FeatureObjects.All())[0].Ref.Name.Source, qt.Equals, "floatrange")
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
	db := must.Must(goschema.ParseSource(must.Must(annotation.NewSet(pgtypesource.Annotations(pgtypeprovider.Targets()))), "fixture.go", src))
	r := parsedRange(c, db)

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
	c.Assert(r.Cleared, qt.DeepEquals, []string{"subtype_diff"})
}

func TestParseRangeAnnotation_MissingSubtypeRejected(t *testing.T) {
	const src = `package fixture

//ptah:schema:range name="floatrange"
type FloatRange struct{}
`
	c := qt.New(t)
	_, err := goschema.ParseSource(must.Must(annotation.NewSet(pgtypesource.Annotations(pgtypeprovider.Targets()))), "fixture.go", src)
	var parseErr *ptaherr.ParseError
	c.Assert(err, qt.ErrorAs, &parseErr)
	c.Assert(parseErr.Directive, qt.Equals, "ptah:schema:range")
}

func parsedRange(c *qt.C, db schemamodel.Database) *pgtype.DesiredRange {
	c.Helper()
	objects := must.Must(db.FeatureObjects.All())
	c.Assert(objects, qt.HasLen, 1)
	value, ok := objects[0].Value.(*pgtype.DesiredRange)
	c.Assert(ok, qt.IsTrue)
	return value
}
