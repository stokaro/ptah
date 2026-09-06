package dbschematogo_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/convert/dbschematogo"
)

// identitySchema is one table whose id column carries the given identity range.
func identitySchema(start, increment string) *catalog.Database {
	return &catalog.Database{
		Tables: []catalog.Table{{
			Name: "orders", Type: "BASE TABLE",
			Columns: []catalog.Column{
				{
					Name: "id", DataType: "int", IsNullable: "NO",
					IsPrimaryKey: true, IsAutoIncrement: true,
					IdentityStart: start, IdentityIncrement: increment,
				},
				{Name: "note", DataType: "nvarchar", IsNullable: "YES"},
			},
		}},
	}
}

// identityField returns the converted id column.
func identityField(c *qt.C, database *schemamodel.Database) schemamodel.Field {
	c.Helper()
	for _, field := range database.Fields {
		if field.Name == "id" {
			return field
		}
	}
	c.Fatalf("no id field in %+v", database.Fields)
	return schemamodel.Field{}
}

// TestConvert_CarriesTheIdentityRange pins that the range crosses this
// conversion.
//
// The columns are copied field by field into a struct literal, which is how a
// newly read fact comes to be read and then dropped one layer later
// (stokaro/ptah#2196).
func TestConvert_CarriesTheIdentityRange(t *testing.T) {
	c := qt.New(t)

	database := dbschematogo.ConvertDBSchemaToGoSchema(identitySchema("1000", "5"), "")

	field := identityField(c, database)
	c.Assert(field.AutoInc, qt.IsTrue)
	c.Assert(field.IdentityStart, qt.Equals, "1000")
	c.Assert(field.IdentityIncrement, qt.Equals, "5")
}

// TestConvert_LeavesAnIdentityColumnWithoutARangeAlone is the control: a reader
// that reports no range must not produce one.
func TestConvert_LeavesAnIdentityColumnWithoutARangeAlone(t *testing.T) {
	c := qt.New(t)

	database := dbschematogo.ConvertDBSchemaToGoSchema(identitySchema("", ""), "")

	field := identityField(c, database)
	c.Assert(field.AutoInc, qt.IsTrue)
	c.Assert(field.IdentityStart, qt.Equals, "")
	c.Assert(field.IdentityIncrement, qt.Equals, "")
}
