package schemascope_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/schemamodel"
	"ptah.run/core/selectext"
	"ptah.run/internal/schemascope"
)

// TestFilterScopesMaterializedViewIndexesOnBothSidesTheSameWay pins that an
// index on a materialized view rides its view on both sides of a comparison,
// as an index rides its table. The read side keeps every index of a selected
// schema, so scoped by the kept tables alone, the declared index leaves the
// desired side, and `migrate diff --schema` writes the view without the index
// the author declared on it. The index on a view in a schema the selection
// leaves out leaves with it.
func TestFilterScopesMaterializedViewIndexesOnBothSidesTheSameWay(t *testing.T) {
	c := qt.New(t)
	desired := &schemamodel.Database{
		MaterializedViews: []schemamodel.MaterializedView{{Name: "order_totals"}, {Name: "other.w"}},
		Indexes: []schemamodel.Index{
			{Name: "order_totals_total_idx", TableName: "order_totals", Fields: []string{"total"}},
			{Name: "w_idx", TableName: "other.w", Fields: []string{"total"}},
		},
	}
	database := &catalog.Database{
		MatViews: []catalog.MaterializedView{{Name: "order_totals"}, {Schema: "other", Name: "w"}},
		Indexes: []catalog.Index{
			{Name: "order_totals_total_idx", TableName: "order_totals", Columns: []string{"total"}},
			{Name: "w_idx", Schema: "other", TableName: "w", Columns: []string{"total"}},
		},
	}

	generated, err := schemascope.FilterGeneratedWithDefaultSchema(t.Context(), selectext.None(), desired, []string{"public"}, "public")
	c.Assert(err, qt.IsNil)
	read, err := schemascope.FilterDatabaseWithDefaultSchema(t.Context(), selectext.None(), database, []string{"public"}, "public")
	c.Assert(err, qt.IsNil)

	c.Assert(generated.Indexes, qt.DeepEquals, desired.Indexes[:1])
	c.Assert(read.Indexes, qt.DeepEquals, database.Indexes[:1])
}
