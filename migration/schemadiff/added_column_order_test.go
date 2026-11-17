package schemadiff_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/migration/schemadiff"
)

// taskField is a nullable integer column of the Task struct.
func taskField(name string) schemamodel.Field {
	return schemamodel.Field{StructName: "Task", FieldName: name, Name: name, Type: "INTEGER", Nullable: true}
}

// heldTasks is the database holding tasks with its one column a.
func heldTasks() *catalog.Database {
	return &catalog.Database{Tables: []catalog.Table{{
		Name: "tasks", Type: "BASE TABLE",
		Columns: []catalog.Column{{Name: "a", DataType: "integer", IsNullable: "YES", OrdinalPosition: 1}},
	}}}
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
		dialect  string
		fields   []schemamodel.Field
		embedded []schemamodel.EmbeddedField
		want     []string
	}{
		{
			name:    "the struct's own fields",
			dialect: platform.Postgres,
			fields:  []schemamodel.Field{taskField("a"), taskField("z"), taskField("b"), taskField("m")},
			want:    []string{"z", "b", "m"},
		},
		{
			name:    "embedded fields after the struct's own",
			dialect: platform.Postgres,
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

			diff, err := schemadiff.CompareWithDialect(c.Context(), desired, heldTasks(), test.dialect, must.Must(builtin.New()))

			c.Assert(err, qt.IsNil)
			c.Assert(diff.TablesModified, qt.HasLen, 1)
			c.Assert(diff.TablesModified[0].ColumnsAdded.Names(), qt.DeepEquals, test.want)
		})
	}
}

// TestCompareWithDialect_RefusesAColumnDeclaredTwice_FailurePath pins what the
// order of added columns relies on: by the time the columns are compared, no
// two declared columns of a table share an identity, so each declared field is
// at most one added column. A column declared twice, or twice under names the
// target does not tell apart, is refused before the comparison.
func TestCompareWithDialect_RefusesAColumnDeclaredTwice_FailurePath(t *testing.T) {
	tests := []struct {
		name     string
		dialect  string
		fields   []schemamodel.Field
		embedded []schemamodel.EmbeddedField
		wantErr  string
	}{
		{
			name:    "the same name twice",
			dialect: platform.Postgres,
			fields:  []schemamodel.Field{taskField("a"), taskField("z"), taskField("b"), taskField("z")},
			wantErr: `invalid schema diff: duplicate desired owner identity column public\.tasks\.z`,
		},
		{
			name:    "the same name twice, compared without a target",
			fields:  []schemamodel.Field{taskField("a"), taskField("z"), taskField("b"), taskField("z")},
			wantErr: `invalid schema diff: duplicate facet owner column tasks\.z`,
		},
		{
			name:    "an embedded field the target reads as a column the struct declares",
			dialect: platform.MySQL,
			fields: []schemamodel.Field{
				taskField("a"), taskField("z"),
				{StructName: "Stamps", FieldName: "Z", Name: "Z", Type: "INTEGER", Nullable: true},
			},
			embedded: []schemamodel.EmbeddedField{{StructName: "Task", Mode: "inline", EmbeddedTypeName: "Stamps"}},
			wantErr:  `invalid schema diff: duplicate desired owner identity column tasks\.Z`,
		},
		{
			name:    "a JSON embedded column the target reads as a column the struct declares",
			dialect: platform.MySQL,
			fields:  []schemamodel.Field{taskField("a"), taskField("Meta")},
			embedded: []schemamodel.EmbeddedField{
				{StructName: "Task", Mode: "json", Name: "meta", Type: "JSON", Nullable: true, EmbeddedTypeName: "Meta"},
			},
			wantErr: `invalid schema diff: duplicate desired owner identity column tasks\.meta`,
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

			diff, err := schemadiff.CompareWithDialect(c.Context(), desired, heldTasks(), test.dialect, must.Must(builtin.New()))

			c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(diff, qt.IsNil)
		})
	}
}

// TestCompareWithDialect_RemovesColumnsInTheTablesOrder_HappyPath pins the
// order of the columns a table loses: the order the table holds them in, by the
// position the read reports, whatever order the read lists them in. The
// rollback adds the columns back in this order, so in name order a rolled-back
// table would hold them elsewhere than it did. A read that reports no position
// leaves name order.
func TestCompareWithDialect_RemovesColumnsInTheTablesOrder_HappyPath(t *testing.T) {
	tests := []struct {
		name    string
		columns []catalog.Column
		want    []string
	}{
		{
			name: "positions the read reports",
			columns: []catalog.Column{
				{Name: "a", DataType: "integer", IsNullable: "YES", OrdinalPosition: 1},
				{Name: "m", DataType: "integer", IsNullable: "YES", OrdinalPosition: 4},
				{Name: "z", DataType: "integer", IsNullable: "YES", OrdinalPosition: 2},
				{Name: "b", DataType: "integer", IsNullable: "YES", OrdinalPosition: 3},
			},
			want: []string{"z", "b", "m"},
		},
		{
			name: "no positions",
			columns: []catalog.Column{
				{Name: "a", DataType: "integer", IsNullable: "YES"},
				{Name: "z", DataType: "integer", IsNullable: "YES"},
				{Name: "m", DataType: "integer", IsNullable: "YES"},
				{Name: "b", DataType: "integer", IsNullable: "YES"},
			},
			want: []string{"b", "m", "z"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			desired := &schemamodel.Database{
				Tables: []schemamodel.Table{{StructName: "Task", Name: "tasks"}},
				Fields: []schemamodel.Field{taskField("a")},
			}
			held := &catalog.Database{Tables: []catalog.Table{{Name: "tasks", Type: "BASE TABLE", Columns: test.columns}}}

			diff, err := schemadiff.CompareWithDialect(c.Context(), desired, held, platform.Postgres, must.Must(builtin.New()))

			c.Assert(err, qt.IsNil)
			c.Assert(diff.TablesModified, qt.HasLen, 1)
			c.Assert(diff.TablesModified[0].ColumnsRemoved.Names(), qt.DeepEquals, test.want)
		})
	}
}
