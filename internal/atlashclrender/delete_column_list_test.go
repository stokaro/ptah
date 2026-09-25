package atlashclrender_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemamodel"
	"ptah.run/internal/atlashclrender"
)

// Atlas HCL has no attribute for an ON DELETE column list, so a key that
// limits the action to some of its columns is refused rather than written as
// one that clears every column.
func TestRender_NarrowedDeleteAction(t *testing.T) {
	c := qt.New(t)
	database := &schemamodel.Database{
		Tables: []schemamodel.Table{
			{StructName: "Parent", Name: "parents", PrimaryKey: []string{"tenant", "id"}},
			{StructName: "Child", Name: "children"},
		},
		Fields: []schemamodel.Field{
			{StructName: "Parent", Name: "tenant", Type: "INTEGER"},
			{StructName: "Parent", Name: "id", Type: "INTEGER"},
			{StructName: "Child", Name: "tenant", Type: "INTEGER", Nullable: true},
			{StructName: "Child", Name: "parent_id", Type: "INTEGER", Nullable: true},
		},
		Constraints: []schemamodel.Constraint{{
			StructName: "Child", Table: "children", Name: "fk_children_parent", Type: "FOREIGN KEY",
			Columns: []string{"tenant", "parent_id"}, ForeignTable: "parents",
			ForeignColumns: []string{"tenant", "id"}, OnDelete: "SET NULL", OnDeleteColumns: []string{"parent_id"},
		}},
	}

	result, err := atlashclrender.RenderInspected(database, "postgres", "public")

	c.Assert(err, qt.ErrorMatches,
		`foreign key "fk_children_parent" limits ON DELETE SET NULL to columns parent_id, which Atlas HCL cannot represent`)
	c.Assert(result, qt.DeepEquals, atlashclrender.Result{})
}
