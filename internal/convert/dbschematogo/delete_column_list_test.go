package dbschematogo_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/internal/convert/dbschematogo"
	"ptah.run/internal/convert/goschematodb"
)

// deleteListCatalog is a composite key whose ON DELETE SET NULL is limited to
// parent_id, as the PostgreSQL reader reports it.
func deleteListCatalog() *catalog.Database {
	parents := "parents"
	setNull := "SET NULL"
	return &catalog.Database{
		Tables: []catalog.Table{
			{Name: "parents", Type: "BASE TABLE", Columns: []catalog.Column{
				{Name: "tenant", DataType: "integer", IsNullable: "NO", IsPrimaryKey: true},
				{Name: "id", DataType: "integer", IsNullable: "NO", IsPrimaryKey: true},
			}},
			{Name: "children", Type: "BASE TABLE", Columns: []catalog.Column{
				{Name: "tenant", DataType: "integer", IsNullable: "NO"},
				{Name: "parent_id", DataType: "integer", IsNullable: "YES"},
			}},
		},
		Constraints: []catalog.Constraint{{
			TableName: "children", Name: "children_parent_fk", Type: "FOREIGN KEY",
			ColumnNames: []string{"tenant", "parent_id"}, ForeignTable: &parents,
			ForeignColumns: []string{"tenant", "id"},
			DeleteRule:     &setNull, OnDeleteColumns: []string{"parent_id"},
		}},
	}
}

// The list survives the conversion of a live catalog into a declaration and
// back, which is the path `ptah db read` and every export of an inspected
// database take. Lost there, the declaration would clear tenant as well.
func TestConvertDBSchemaToGoSchema_CarriesTheDeleteColumnList(t *testing.T) {
	c := qt.New(t)

	declared := dbschematogo.ConvertDBSchemaToGoSchema(deleteListCatalog(), "postgres")
	var lists [][]string
	for _, constraint := range declared.Constraints {
		lists = append(lists, constraint.OnDeleteColumns)
	}
	roundTrip := goschematodb.ToDBSchema(declared, "postgres")
	var roundTripLists [][]string
	for _, constraint := range roundTrip.Constraints {
		roundTripLists = append(roundTripLists, constraint.OnDeleteColumns)
	}

	c.Assert(lists, qt.DeepEquals, [][]string{{"parent_id"}})
	c.Assert(roundTripLists, qt.DeepEquals, [][]string{{"parent_id"}})
}
