package goschematodb_test

import (
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/internal/convert/goschematodb"
)

// TestToDBSchema_CarriesTemporalKeys describes a declaration the way a catalog
// would report it, so a schema file compared with itself pairs every temporal
// key with itself (stokaro/ptah#6330).
func TestToDBSchema_CarriesTemporalKeys(t *testing.T) {
	c := qt.New(t)
	desired := &schemamodel.Database{
		Tables: []schemamodel.Table{
			{StructName: "P", Name: "p", PrimaryKey: []string{"id", "valid_at"}, PrimaryKeyWithoutOverlaps: true},
			{StructName: "C", Name: "c"},
		},
		Fields: []schemamodel.Field{
			{StructName: "P", Name: "id", Type: "integer"},
			{StructName: "P", Name: "valid_at", Type: "daterange"},
			{StructName: "C", Name: "pid", Type: "integer"},
			{StructName: "C", Name: "valid_at", Type: "daterange"},
		},
		Constraints: []schemamodel.Constraint{
			{
				StructName: "C", Table: "c", Name: "c_uq", Type: "UNIQUE",
				Columns: []string{"pid", "valid_at"}, WithoutOverlaps: true,
			},
			{
				StructName: "C", Table: "c", Name: "c_fk", Type: "FOREIGN KEY", Columns: []string{"pid", "valid_at"},
				ForeignTable: "p", ForeignColumns: []string{"id", "valid_at"}, Period: true,
			},
		},
	}

	got := must.Must(goschematodb.ToDBSchema(t.Context(), desired, "postgres", must.Must(builtin.New())))

	described := make(map[string]catalog.Constraint)
	for _, constraint := range got.Constraints {
		described[constraint.Name] = constraint
	}
	c.Assert(described["c_uq"].WithoutOverlaps, qt.IsTrue)
	c.Assert(described["c_uq"].Period, qt.IsFalse)
	c.Assert(described["c_fk"].Period, qt.IsTrue)
	c.Assert(described["c_fk"].WithoutOverlaps, qt.IsFalse)
	primary := slices.DeleteFunc(slices.Clone(got.Constraints), func(constraint catalog.Constraint) bool {
		return constraint.Type != "PRIMARY KEY"
	})
	c.Assert(primary, qt.HasLen, 1)
	c.Assert(primary[0].WithoutOverlaps, qt.IsTrue)
}
