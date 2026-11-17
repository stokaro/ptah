package schemadiff_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/migration/schemadiff"
)

// viewOwnerSides is a table, a view and a materialized view declared with
// the owners given, and a database that holds them owned by app.
func viewOwnerSides(viewOwner, materializedOwner string) (*schemamodel.Database, *catalog.Database) {
	desired := &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "T", Name: "t"}},
		Fields: []schemamodel.Field{{StructName: "T", Name: "id", Type: "integer", Primary: true}},
		Views:  []schemamodel.View{{Name: "v", Body: "SELECT id FROM t", Owner: viewOwner}},
		MaterializedViews: []schemamodel.MaterializedView{
			{Name: "m", Body: "SELECT id FROM t", Owner: materializedOwner},
		},
	}
	current := &catalog.Database{
		Tables: []catalog.Table{{Name: "t", Schema: "public", Type: "BASE TABLE", Columns: []catalog.Column{
			{Name: "id", DataType: "integer", UDTName: "int4", IsNullable: "NO", IsPrimaryKey: true, OrdinalPosition: 1},
		}}},
		Views:    []catalog.View{{Name: "v", Schema: "public", Body: "SELECT id FROM t"}},
		MatViews: []catalog.MaterializedView{{Name: "m", Schema: "public", Body: "SELECT id FROM t"}},
		ObjectOwners: []catalog.ObjectOwner{
			{Kind: "view", Schema: "public", Name: "v", Owner: "app"},
			{Kind: "materialized view", Schema: "public", Name: "m", Owner: "app"},
		},
	}
	return desired, current
}

// TestCompare_ViewOwner_HappyPath pins what a comparison records for the
// owner of a view and of a materialized view. A declaration that names no
// owner leaves it unmanaged, so another role holding the view is no
// difference at all; one that names the role the database reports is none
// either; one that names another role is a change of the owner alone.
func TestCompare_ViewOwner_HappyPath(t *testing.T) {
	tests := []struct {
		name                       string
		viewOwner, materialized    string
		wantView, wantMaterialized []map[string]string
	}{
		{name: "no declared owner"},
		{name: "the owner the database reports", viewOwner: "app", materialized: "app"},
		{
			name: "another owner", viewOwner: "reporting", materialized: "Report Owner",
			wantView:         []map[string]string{{"owner": "app -> reporting"}},
			wantMaterialized: []map[string]string{{"owner": "app -> Report Owner"}},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			desired, current := viewOwnerSides(test.viewOwner, test.materialized)

			diff, err := schemadiff.CompareWithDialect(t.Context(), desired, current, platform.Postgres, must.Must(builtin.New()))

			c.Assert(err, qt.IsNil)
			var views, materialized []map[string]string
			for _, viewDiff := range diff.ViewsModified {
				views = append(views, viewDiff.Changes)
			}
			for _, viewDiff := range diff.MaterializedViewsModified {
				materialized = append(materialized, viewDiff.Changes)
			}
			c.Assert(views, qt.DeepEquals, test.wantView)
			c.Assert(materialized, qt.DeepEquals, test.wantMaterialized)
			c.Assert(diff.HasChanges(), qt.Equals, test.wantView != nil)
		})
	}
}
