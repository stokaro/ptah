package goschematogo_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/goschema"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/postgres/pgtable"
	"ptah.run/internal/builtintest"
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
			ForeignColumns: []string{"tenant", "id"}, OnDelete: "SET NULL", Facets: must.Must(pgtable.WithDeleteColumns(schemaext.Facets{}, listed)),
		}},
	}
}

// A narrowed action exports through the owner and reads back with its list.
func TestRender_NarrowedDeleteAction_RoundTrip(t *testing.T) {
	c := qt.New(t)
	files, err := goschematogo.Render(c.Context(), narrowedDeleteDatabase("parent_id"), goschematogo.Options{Annotations: builtintest.Annotations(), SingleFile: true})
	c.Assert(err, qt.IsNil)
	c.Assert(files, qt.HasLen, 1)
	c.Assert(string(files[0].Data), qt.Contains, `on_delete_columns="parent_id"`)
	restored, err := goschema.ParseSource(builtintest.Annotations(), files[0].Name, files[0].Data)
	c.Assert(err, qt.IsNil)
	c.Assert(restored.Constraints, qt.HasLen, 1)
	columns, _, err := pgtable.DeleteColumnsOf(restored.Constraints[0].Facets)
	c.Assert(err, qt.IsNil)
	c.Assert(columns, qt.DeepEquals, []string{"parent_id"})
}

// A list naming every key column says what no list says, so it exports.
func TestRender_NarrowedDeleteAction_HappyPath(t *testing.T) {
	c := qt.New(t)

	files, err := goschematogo.Render(c.Context(), narrowedDeleteDatabase("parent_id", "tenant"), goschematogo.Options{Annotations: builtintest.Annotations(), SingleFile: true})

	c.Assert(err, qt.IsNil)
	c.Assert(files, qt.HasLen, 1)
}
