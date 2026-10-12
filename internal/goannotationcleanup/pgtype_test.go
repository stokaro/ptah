package goannotationcleanup_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/feature/pgtype"
	"ptah.run/internal/goannotationcleanup"
)

// typesPlan plans the cleanup of a source declaring the domain app.email
// over text, the composite type addr and the range type span over int4.
func typesPlan(c *qt.C) *goannotationcleanup.Plan {
	c.Helper()
	dir := c.TempDir()
	source := "package models\n\n" +
		"//ptah:schema:domain name=\"email\" schema=\"app\" type=\"text\"\n" +
		"//ptah:schema:composite name=\"addr\" fields=\"street:text\"\n" +
		"//ptah:schema:range name=\"span\" subtype=\"int4\"\n" +
		"type Types struct{}\n"
	c.Assert(os.WriteFile(filepath.Join(dir, "model.go"), []byte(source), 0o600), qt.IsNil)
	return must.Must(planDir(dir))
}

// ownerTypes is the export holding the three types as the PostgreSQL type
// owner's objects; domain and span name what they are over.
func ownerTypes(domainBase, subtype string) *schemamodel.Database {
	return &schemamodel.Database{FeatureObjects: must.Must(schemaext.NewObjects(
		must.Must(pgtype.DesiredObject(pgtype.Ref(pgtype.DomainKind, "app", "email"), &pgtype.DesiredDomain{BaseType: domainBase})),
		must.Must(pgtype.DesiredObject(pgtype.Ref(pgtype.CompositeKind, "", "addr"),
			&pgtype.DesiredComposite{Attributes: []pgtype.Attribute{{Name: "street", Type: "text"}}})),
		must.Must(pgtype.DesiredObject(pgtype.Ref(pgtype.RangeKind, "", "span"), &pgtype.DesiredRange{Subtype: subtype})),
	))}
}

// TestValidateParsed_AcceptsTypesTheOwnerHolds pins the cleanup of type
// annotations: the export may hold the types as the PostgreSQL type owner's
// objects, and they represent the annotations as the common types do.
func TestValidateParsed_AcceptsTypesTheOwnerHolds(t *testing.T) {
	c := qt.New(t)
	plan := typesPlan(c)

	err := plan.ValidateParsed(&schemamodel.Database{}, ownerTypes("text", "int4"))

	c.Assert(err, qt.IsNil)
}

// TestValidateParsed_RefusesATypeTheOwnerHoldsDifferently pins that an
// owner's type over another base type or subtype does not represent the
// annotation, as a common one would not.
func TestValidateParsed_RefusesATypeTheOwnerHoldsDifferently(t *testing.T) {
	tests := []struct {
		name     string
		exported *schemamodel.Database
		want     string
	}{
		{name: "a domain over another type", exported: ownerTypes("integer", "int4"), want: `.*//ptah:schema:domain did not produce a schema object`},
		{name: "a range over another subtype", exported: ownerTypes("text", "int8"), want: `.*//ptah:schema:range did not produce a schema object`},
		{name: "a composite type of the domain's name", want: `.*//ptah:schema:domain did not produce a schema object`,
			exported: &schemamodel.Database{FeatureObjects: must.Must(schemaext.NewObjects(
				must.Must(pgtype.DesiredObject(pgtype.Ref(pgtype.CompositeKind, "app", "email"),
					&pgtype.DesiredComposite{Attributes: []pgtype.Attribute{{Name: "address", Type: "text"}}})),
				must.Must(pgtype.DesiredObject(pgtype.Ref(pgtype.CompositeKind, "", "addr"),
					&pgtype.DesiredComposite{Attributes: []pgtype.Attribute{{Name: "street", Type: "text"}}})),
				must.Must(pgtype.DesiredObject(pgtype.Ref(pgtype.RangeKind, "", "span"), &pgtype.DesiredRange{Subtype: "int4"})),
			))}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			plan := typesPlan(c)

			err := plan.ValidateParsed(&schemamodel.Database{}, test.exported)

			c.Assert(err, qt.ErrorIs, goannotationcleanup.ErrUnexportedAnnotation)
			c.Assert(err, qt.ErrorMatches, test.want)
		})
	}
}
