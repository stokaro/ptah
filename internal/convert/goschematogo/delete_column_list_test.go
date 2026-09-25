package goschematogo_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemamodel"
	"ptah.run/internal/convert/goschematogo"
)

func narrowedDeleteDatabase(listed ...string) *schemamodel.Database {
	return &schemamodel.Database{
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
			ForeignColumns: []string{"tenant", "id"}, OnDelete: "SET NULL", OnDeleteColumns: listed,
		}},
	}
}

// A key whose ON DELETE action is limited to some of its columns has no
// annotation spelling, so the export refuses it rather than write a key that
// clears every column.
func TestRender_NarrowedDeleteAction_FailurePath(t *testing.T) {
	c := qt.New(t)

	files, err := goschematogo.Render(narrowedDeleteDatabase("parent_id"), goschematogo.Options{SingleFile: true})

	c.Assert(err, qt.ErrorMatches,
		`foreign key "fk_children_parent" limits ON DELETE SET NULL to columns parent_id, which a Go annotation cannot represent`)
	c.Assert(files, qt.IsNil)
}

// A list naming every key column says what no list says, so it exports.
func TestRender_NarrowedDeleteAction_HappyPath(t *testing.T) {
	c := qt.New(t)

	files, err := goschematogo.Render(narrowedDeleteDatabase("parent_id", "tenant"), goschematogo.Options{SingleFile: true})

	c.Assert(err, qt.IsNil)
	c.Assert(files, qt.HasLen, 1)
}
