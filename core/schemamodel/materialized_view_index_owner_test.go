package schemamodel_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemamodel"
)

// An index whose table name is exactly the name a materialized view is
// declared under keeps that name when the description is finalized, though
// the only table of that name is in another schema. The control declares no
// view: the bare name then reaches that table, as an index that names its
// table without the table's schema does.
func TestFinalize_KeepsAnIndexOnTheMaterializedViewItNames(t *testing.T) {
	tests := []struct {
		name  string
		views []schemamodel.MaterializedView
		want  string
	}{
		{name: "a materialized view of the name", views: []schemamodel.MaterializedView{{Name: "mv", Body: "SELECT 1"}}, want: "mv"},
		{name: "no materialized view of the name", want: "s2.mv"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			database := schemamodel.Database{
				Tables:            []schemamodel.Table{{StructName: "S2Mv", Name: "mv", Schema: "s2"}},
				MaterializedViews: test.views,
				Indexes:           []schemamodel.Index{{Name: "mv_a", StructName: "mv", TableName: "mv", Fields: []string{"a"}}},
			}

			schemamodel.Finalize(&database)

			c.Assert(database.Indexes, qt.HasLen, 1)
			c.Assert(database.Indexes[0].TableName, qt.Equals, test.want)
		})
	}
}

// An index the struct of a table declares stays on that table beside a
// materialized view of the table's bare name: `//ptah:schema:index
// name="items_a" fields="a" table="items"` on a field of Item names app.items,
// the table Item declares, and not the view items. The control declares no
// view, and the index reaches the same table.
func TestFinalize_KeepsAStructsIndexOnItsTableBesideAMaterializedView(t *testing.T) {
	tests := []struct {
		name  string
		views []schemamodel.MaterializedView
	}{
		{name: "a materialized view of the table's name", views: []schemamodel.MaterializedView{{Name: "items", Body: "SELECT 1"}}},
		{name: "no materialized view"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			database := schemamodel.Database{
				Tables:            []schemamodel.Table{{StructName: "Item", Name: "items", Schema: "app"}},
				MaterializedViews: test.views,
				Indexes:           []schemamodel.Index{{Name: "items_a", StructName: "Item", TableName: "items", Fields: []string{"a"}}},
			}

			schemamodel.Finalize(&database)

			c.Assert(database.Indexes, qt.HasLen, 1)
			c.Assert(database.Indexes[0].TableName, qt.Equals, "app.items")
		})
	}
}
