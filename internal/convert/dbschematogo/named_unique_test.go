package dbschematogo_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/catalog"
	"go.5x5.cz/ptah/core/schemamodel"
	"go.5x5.cz/ptah/internal/convert/dbschematogo"
)

// uniqueSchema is one table with one single-column UNIQUE constraint under the
// given name, and the column marked unique the way a reader marks it.
func uniqueSchema(constraintName string) *catalog.Database {
	return &catalog.Database{
		Tables: []catalog.Table{{
			Name: "customers",
			Type: "BASE TABLE",
			Columns: []catalog.Column{
				{Name: "id", DataType: "bigint", IsNullable: "NO", IsPrimaryKey: true},
				{Name: "email", DataType: "character varying", IsNullable: "NO", IsUnique: true},
			},
		}},
		Constraints: []catalog.Constraint{{
			TableName:   "customers",
			Name:        constraintName,
			Type:        "UNIQUE",
			ColumnNames: []string{"email"},
		}},
	}
}

// emailField returns the converted column the constraint is about.
func emailField(c *qt.C, database *schemamodel.Database) schemamodel.Field {
	c.Helper()
	for _, field := range database.Fields {
		if field.Name == "email" {
			return field
		}
	}
	c.Fatalf("no email field in %+v", database.Fields)
	return schemamodel.Field{}
}

// TestConvert_KeepsAUniqueConstraintNameSomebodyChose pins that a name survives
// the description.
//
// A single-column UNIQUE is normally carried by the column's own
// `unique = true`, which has nowhere to put a name. So a constraint somebody
// named was described without it and came back under the server's generated
// one -- `customers_email_key` where the author had written
// `customers_email_uq`. A constraint name is an interface: it appears in every
// violation error the application sees, in monitoring that groups by it, and in
// any migration that later drops it by name (stokaro/ptah#2102).
func TestConvert_KeepsAUniqueConstraintNameSomebodyChose(t *testing.T) {
	c := qt.New(t)

	database := dbschematogo.ConvertCatalogToSchema(uniqueSchema("customers_email_uq"))

	c.Assert(database.Constraints, qt.HasLen, 1)
	c.Assert(database.Constraints[0].Name, qt.Equals, "customers_email_uq")
	c.Assert(database.Constraints[0].Columns, qt.DeepEquals, []string{"email"})
	// Both spellings would mean two constraints in one document, and a
	// duplicate on apply.
	c.Assert(emailField(c, database).Unique, qt.IsFalse)
}

// TestConvert_LeavesAGeneratedUniqueNameToTheColumn is the control.
//
// A name the server made up carries nothing a person chose, and the compact
// column spelling reproduces it exactly -- so writing a named constraint for it
// would put a generated identifier into a document somebody edits by hand.
func TestConvert_LeavesAGeneratedUniqueNameToTheColumn(t *testing.T) {
	tests := []struct {
		name           string
		constraintName string
	}{
		{name: "the PostgreSQL form", constraintName: "customers_email_key"},
		{name: "the MySQL form", constraintName: "email"},
		{name: "no name at all", constraintName: ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			database := dbschematogo.ConvertCatalogToSchema(uniqueSchema(tt.constraintName))

			c.Assert(database.Constraints, qt.HasLen, 0)
			c.Assert(emailField(c, database).Unique, qt.IsTrue)
		})
	}
}
