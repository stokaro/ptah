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

// taskField is a nullable integer column of the Task struct.
func taskField(name string) schemamodel.Field {
	return schemamodel.Field{StructName: "Task", FieldName: name, Name: name, Type: "INTEGER", Nullable: true}
}

// TestCompareWithDialect_AddsColumnsInDeclaredOrder_HappyPath pins the order of
// the columns a table gains: the order the declaration gives them, which is the
// order a table created from it holds them in, and the order the community
// binary adds them in. Name order would leave a table that gained its columns
// with a different `SELECT *` than one created from scratch. Embedded fields
// follow the struct's own, as they do in a created table.
func TestCompareWithDialect_AddsColumnsInDeclaredOrder_HappyPath(t *testing.T) {
	tests := []struct {
		name     string
		fields   []schemamodel.Field
		embedded []schemamodel.EmbeddedField
		want     []string
	}{
		{
			name:   "the struct's own fields",
			fields: []schemamodel.Field{taskField("a"), taskField("z"), taskField("b"), taskField("m")},
			want:   []string{"z", "b", "m"},
		},
		{
			name: "embedded fields after the struct's own",
			fields: []schemamodel.Field{
				taskField("a"), taskField("z"),
				{StructName: "Stamps", FieldName: "Updated", Name: "updated_at", Type: "TIMESTAMP", Nullable: true},
				{StructName: "Stamps", FieldName: "Created", Name: "created_at", Type: "TIMESTAMP", Nullable: true},
				taskField("b"),
			},
			embedded: []schemamodel.EmbeddedField{
				{StructName: "Task", Mode: "inline", EmbeddedTypeName: "Stamps"},
				{StructName: "Task", Mode: "json", Name: "meta", Type: "JSONB", Nullable: true, EmbeddedTypeName: "Meta"},
			},
			want: []string{"z", "b", "updated_at", "created_at", "meta"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			desired := &schemamodel.Database{
				Tables:         []schemamodel.Table{{StructName: "Task", Name: "tasks"}},
				Fields:         test.fields,
				EmbeddedFields: test.embedded,
			}
			held := &catalog.Database{Tables: []catalog.Table{{
				Name: "tasks", Type: "BASE TABLE",
				Columns: []catalog.Column{{Name: "a", DataType: "integer", IsNullable: "YES"}},
			}}}

			diff, err := schemadiff.CompareWithDialect(c.Context(), desired, held, platform.Postgres, must.Must(builtin.New()))

			c.Assert(err, qt.IsNil)
			c.Assert(diff.TablesModified, qt.HasLen, 1)
			c.Assert(diff.TablesModified[0].ColumnsAdded.Names(), qt.DeepEquals, test.want)
		})
	}
}
