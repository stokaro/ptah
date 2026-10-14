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
		Domains:        []catalog.Domain{{Name: "d", BaseType: "integer", Comment: "domain note"}},
		Composites:     []catalog.CompositeType{{Name: "c", Fields: []catalog.CompositeField{{Name: "n", Type: "integer"}}, Comment: "composite note"}},
		FeatureObjects: must.Must(schemaext.NewObjects(schemaext.Object{Ref: pgtype.Ref(pgtype.RangeKind, "", "r"), Value: &pgtype.ObservedRange{Subtype: "integer", Comment: "range note"}})),
	}, "postgres", must.Must(builtin.New())))

	c.Assert(database.Domains, qt.HasLen, 1)
	c.Assert(database.Domains[0].Comment, qt.Equals, "domain note")
	c.Assert(database.CompositeTypes, qt.HasLen, 1)
	c.Assert(database.CompositeTypes[0].Comment, qt.Equals, "composite note")
	c.Assert(onlyRange(c, database).Comment, qt.Equals, "range note")
}
