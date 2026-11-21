package dbschematogo_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/engine/builtin"
	"ptah.run/internal/convert/dbschematogo"
)

// TestConvert_DescribesTemporalKeys keeps what the catalog reports about a
// temporal key in the description: the primary key on its table, the others on
// the constraint (stokaro/ptah#6330). Described without them, a database
// compared with itself plans every such key again.
func TestConvert_DescribesTemporalKeys(t *testing.T) {
	c := qt.New(t)
	parent, column := "p", "id"
	database := &catalog.Database{
		Tables: []catalog.Table{
			{Name: "p", Columns: []catalog.Column{
				{Name: "id", DataType: "integer", IsNullable: "NO", IsPrimaryKey: true},
				{Name: "valid_at", DataType: "daterange", IsNullable: "NO", IsPrimaryKey: true},
			}},
			{Name: "c", Columns: []catalog.Column{
				{Name: "pid", DataType: "integer", IsNullable: "NO"},
				{Name: "valid_at", DataType: "daterange", IsNullable: "NO"},
			}},
		},
		Constraints: []catalog.Constraint{
			{
				Name: "p_pkey", TableName: "p", Type: "PRIMARY KEY", ColumnName: "id",
				ColumnNames: []string{"id", "valid_at"}, WithoutOverlaps: true,
			},
			{
				Name: "c_uq", TableName: "c", Type: "UNIQUE", ColumnName: "pid",
				ColumnNames: []string{"pid", "valid_at"}, WithoutOverlaps: true,
			},
			{
				Name: "c_fk", TableName: "c", Type: "FOREIGN KEY", ColumnName: "pid",
				ColumnNames: []string{"pid", "valid_at"}, ForeignTable: &parent, ForeignColumn: &column,
				ForeignColumns: []string{"id", "valid_at"}, Period: true,
			},
		},
	}

	got := must.Must(dbschematogo.ConvertDBSchemaToGoSchema(t.Context(), database, "postgres", must.Must(builtin.New())))

	tables := make(map[string]bool)
	for _, table := range got.Tables {
		tables[table.Name] = table.PrimaryKeyWithoutOverlaps
	}
	c.Assert(tables, qt.DeepEquals, map[string]bool{"p": true, "c": false})
	constraints := make(map[string][2]bool)
	for _, constraint := range got.Constraints {
		constraints[constraint.Name] = [2]bool{constraint.WithoutOverlaps, constraint.Period}
	}
	c.Assert(constraints, qt.DeepEquals, map[string][2]bool{"c_uq": {true, false}, "c_fk": {false, true}})
}
