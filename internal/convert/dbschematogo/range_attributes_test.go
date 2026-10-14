package dbschematogo_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/feature/pgtype"
	"ptah.run/internal/convert/dbschematogo"
)

func onlyRange(c *qt.C, database *schemamodel.Database) *pgtype.DesiredRange {
	c.Helper()
	objects := must.Must(database.FeatureObjects.All())
	c.Assert(objects, qt.HasLen, 1)
	value, ok := objects[0].Value.(*pgtype.DesiredRange)
	c.Assert(ok, qt.IsTrue)
	return value
}

// Every catalog attribute reaches the declaration used to replay a range.
// A subtype-only observation gains no invented optional attributes.
func TestConvert_CarriesEveryRangeAttribute(t *testing.T) {
	for _, test := range []struct {
		name     string
		observed pgtype.ObservedRange
		want     pgtype.DesiredRange
	}{
		{name: "all attributes", observed: pgtype.ObservedRange{Subtype: "double precision", SubtypeOpClass: "float8_ops", Collation: "en_US", Canonical: "fancy_canonical", SubtypeDiff: "f8diff", Comment: "range note"}, want: pgtype.DesiredRange{Subtype: "double precision", SubtypeOpClass: "float8_ops", Collation: "en_US", Canonical: "fancy_canonical", SubtypeDiff: "f8diff", Comment: "range note"}},
		{name: "subtype only", observed: pgtype.ObservedRange{Subtype: "double precision"}, want: pgtype.DesiredRange{Subtype: "double precision"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			ref := pgtype.Ref(pgtype.RangeKind, "public", "fancy")
			source := &catalog.Database{FeatureObjects: must.Must(schemaext.NewObjects(schemaext.Object{Ref: ref, Value: &test.observed}))}
			converted, err := dbschematogo.ConvertDBSchemaToGoSchema(c.Context(), source, "postgres", must.Must(builtin.New()))
			c.Assert(err, qt.IsNil)
			c.Assert(onlyRange(c, converted), qt.DeepEquals, &test.want)
			c.Assert(must.Must(converted.FeatureObjects.All())[0].Ref, qt.DeepEquals, ref)
		})
	}
}
