package dbschematogo_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/postgres/pgtable"
	"ptah.run/engine/builtin"
	"ptah.run/internal/convert/dbschematogo"
)

func TestConvert_ExclusionOwnsItsBackingIndex(t *testing.T) {
	c := qt.New(t)
	ref := pgtable.ExclusionRef("rooms", "guard", identifier.ForDialect("postgres"))
	database := &catalog.Database{
		Tables: []catalog.Table{{Name: "rooms", Type: "BASE TABLE", Columns: []catalog.Column{{Name: "room", DataType: "integer", IsNullable: "NO"}}}},
		Indexes: []catalog.Index{
			{Name: "guard_index", TableName: "rooms", Columns: []string{"room"}, ManagedBy: ref},
			{Name: "lookup", TableName: "rooms", Columns: []string{"room"}},
		},
		FeatureObjects:  must.Must(schemaext.NewObjects(schemaext.Object{Ref: ref, Value: &pgtable.ObservedExclusion{UsingMethod: "btree", Elements: "room WITH =", Comment: "Room schedule"}})),
		FeatureCoverage: must.Must(pgtable.ExclusionCoverage(schemaext.Observed, schemaext.Knowledge{State: schemaext.Complete}, nil)),
	}
	result, err := dbschematogo.ConvertDBSchemaToGoSchema(c.Context(), database, "postgres", must.Must(builtin.New()))
	c.Assert(err, qt.IsNil)
	c.Assert(result.Constraints, qt.HasLen, 0)
	c.Assert(result.Indexes, qt.HasLen, 1)
	c.Assert(result.Indexes[0].Name, qt.Equals, "lookup")
	object, found, err := result.FeatureObjects.Get(ref)
	c.Assert(err, qt.IsNil)
	c.Assert(found, qt.IsTrue)
	c.Assert(object.Value.(*pgtable.DesiredExclusion).Comment, qt.Equals, "Room schedule")
	c.Assert(database.Indexes, qt.HasLen, 2, qt.Commentf("conversion preserves the observed index metadata"))
}
