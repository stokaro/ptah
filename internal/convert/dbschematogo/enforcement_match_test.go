package dbschematogo_test

import (
	"strconv"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/convert/dbschematogo"
)

// columnForeignKeys lists the columns that carry a foreign key, with its MATCH
// type and enforcement.
func columnForeignKeys(database *schemamodel.Database) []string {
	var keys []string
	for _, field := range database.Fields {
		if field.Foreign != "" {
			keys = append(keys, field.Name+" match="+field.ForeignKeyMatch+
				" not_enforced="+strconv.FormatBool(field.ForeignKeyNotEnforced))
		}
	}
	return keys
}

// TestConvert_DescribesEnforcementAndMatch keeps what the catalog reports
// about enforcement and the MATCH type in the description: on the field of a
// single-column foreign key, and on a table-level CHECK (stokaro/ptah#3853).
// Described without them, a database compared with itself plans every such
// constraint again.
func TestConvert_DescribesEnforcementAndMatch(t *testing.T) {
	c := qt.New(t)
	parent, column, clause := "p", "id", "n > 0"
	database := &catalog.Database{
		Tables: []catalog.Table{
			{Name: "p", Columns: []catalog.Column{{Name: "id", DataType: "integer", IsNullable: "NO", IsPrimaryKey: true}}},
			{Name: "c", Columns: []catalog.Column{
				{Name: "id", DataType: "integer", IsNullable: "NO", IsPrimaryKey: true},
				{Name: "p_id", DataType: "integer", IsNullable: "YES"},
				{Name: "n", DataType: "integer", IsNullable: "YES"},
			}},
		},
		Constraints: []catalog.Constraint{
			{
				Name: "c_p_fkey", TableName: "c", Type: "FOREIGN KEY", ColumnName: "p_id", ColumnNames: []string{"p_id"},
				ForeignTable: &parent, ForeignColumn: &column, ForeignColumns: []string{"id"},
				Match: "FULL", NotEnforced: true,
			},
			{Name: "c_n_check", TableName: "c", Type: "CHECK", CheckClause: &clause, NotEnforced: true},
		},
	}

	got := dbschematogo.ConvertDBSchemaToGoSchema(database, "postgres")

	c.Assert(columnForeignKeys(got), qt.DeepEquals, []string{"p_id match=FULL not_enforced=true"})
	c.Assert(got.Constraints, qt.HasLen, 1)
	c.Assert(got.Constraints[0].Name, qt.Equals, "c_n_check")
	c.Assert(got.Constraints[0].NotEnforced, qt.IsTrue)
}
