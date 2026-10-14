package dbschematogo_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/schemaext"
	"ptah.run/engine/builtin"
	"ptah.run/feature/pgtype"
	"ptah.run/internal/convert/dbschematogo"
)

// A domain's, a composite's and a range's comment reach the description a read
// produces, so a schema inspected from a server keeps them and a replay writes
// them back (stokaro/ptah#3627).
func TestConvert_CarriesUserTypeComments(t *testing.T) {
	c := qt.New(t)

	database := must.Must(dbschematogo.ConvertDBSchemaToGoSchema(t.Context(), &catalog.Database{
		FeatureObjects: must.Must(schemaext.NewObjects(
			schemaext.Object{Ref: pgtype.Ref(pgtype.DomainKind, "", "d"), Value: &pgtype.ObservedDomain{BaseType: "integer", Comment: "domain note"}},
			schemaext.Object{Ref: pgtype.Ref(pgtype.CompositeKind, "", "c"),
				Value: &pgtype.ObservedComposite{Attributes: []pgtype.Attribute{{Name: "n", Type: "integer"}}, Comment: "composite note"}},
			schemaext.Object{Ref: pgtype.Ref(pgtype.RangeKind, "", "r"), Value: &pgtype.ObservedRange{Subtype: "integer", Comment: "range note"}})),
	}, "postgres", must.Must(builtin.New())))

	domain, found, err := database.FeatureObjects.Get(pgtype.Ref(pgtype.DomainKind, "", "d"))
	c.Assert(err, qt.IsNil)
	c.Assert(found, qt.IsTrue)
	c.Assert(domain.Value.(*pgtype.DesiredDomain).Comment, qt.Equals, "domain note")
	composite, found, err := database.FeatureObjects.Get(pgtype.Ref(pgtype.CompositeKind, "", "c"))
	c.Assert(err, qt.IsNil)
	c.Assert(found, qt.IsTrue)
	c.Assert(composite.Value.(*pgtype.DesiredComposite).Comment, qt.Equals, "composite note")
	rangeType, found, err := database.FeatureObjects.Get(pgtype.Ref(pgtype.RangeKind, "", "r"))
	c.Assert(err, qt.IsNil)
	c.Assert(found, qt.IsTrue)
	c.Assert(rangeType.Value.(*pgtype.DesiredRange).Comment, qt.Equals, "range note")
}
