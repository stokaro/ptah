package dbschematogo_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/internal/convert/dbschematogo"
	"ptah.run/internal/convert/goschematodb"
)

// commentedCheckCatalog is a table with a named CHECK constraint carrying a
// comment, as the PostgreSQL reader reports it.
func commentedCheckCatalog() *catalog.Database {
	return &catalog.Database{
		Tables: []catalog.Table{{Name: "orders", Type: "BASE TABLE", Columns: []catalog.Column{
			{Name: "id", DataType: "integer", IsNullable: "NO", IsPrimaryKey: true},
			{Name: "total", DataType: "integer", IsNullable: "YES"},
		}}},
		Constraints: []catalog.Constraint{{
			TableName: "orders", Name: "orders_total_positive", Type: "CHECK",
			CheckClause: new("total > 0"), Comment: "a total is positive",
		}},
	}
}

// A constraint's comment survives the conversion of a live catalog into a
// declaration and back, which is the path `ptah db read` and every export of
// an inspected database take (stokaro/ptah#3678). Lost there, the declaration
// would plan the comment's removal against the database it was read from.
func TestConvertDBSchemaToGoSchema_CarriesTheConstraintComment(t *testing.T) {
	c := qt.New(t)

	declared := dbschematogo.ConvertDBSchemaToGoSchema(commentedCheckCatalog(), "postgres")
	roundTrip := goschematodb.ToDBSchema(declared, "postgres")

	c.Assert(declared.Constraints, qt.HasLen, 1)
	c.Assert(declared.Constraints[0].Comment, qt.Equals, "a total is positive")
	comments := make(map[string]string, len(roundTrip.Constraints))
	for _, constraint := range roundTrip.Constraints {
		comments[constraint.Name] = constraint.Comment
	}
	c.Assert(comments["orders_total_positive"], qt.Equals, "a total is positive")
}
