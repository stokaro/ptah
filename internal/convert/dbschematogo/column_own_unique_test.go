package dbschematogo_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/convert/dbschematogo"
)

// twoUniqueSchema is customers with the column email marked unique the way a
// reader marks it, and a single-column UNIQUE over email under each name, with
// the unique indexes given.
func twoUniqueSchema(indexes []catalog.Index, names ...string) *catalog.Database {
	database := uniqueSchema(names[0])
	for _, name := range names[1:] {
		database.Constraints = append(database.Constraints, catalog.Constraint{
			TableName: "customers", Name: name, Type: "UNIQUE", ColumnNames: []string{"email"},
		})
	}
	database.Indexes = indexes
	return database
}

// constraintNames lists the names of the converted constraints in order.
func constraintNames(constraints []schemamodel.Constraint) []string {
	names := make([]string, 0, len(constraints))
	for _, constraint := range constraints {
		names = append(names, constraint.Name)
	}
	return names
}

// TestConvert_KeepsAColumnsOwnUniqueBesideANamedOne describes both keys of a
// column the server holds two UNIQUEs over, one under the name the server
// gives a column's key. Measured on PostgreSQL 18.6, `CREATE TABLE g2 (a int,
// UNIQUE (a))` and `ALTER TABLE g2 ADD UNIQUE (a)` build g2_a_key and
// g2_a_key1. Described as g2_a_key1 alone, a diff of the database with itself
// plans to drop g2_a_key (stokaro/ptah#3819).
func TestConvert_KeepsAColumnsOwnUniqueBesideANamedOne(t *testing.T) {
	tests := []struct {
		name  string
		names []string
		want  []string
	}{
		{name: "the PostgreSQL form", names: []string{"customers_email_key", "customers_email_key1"}, want: []string{"customers_email_key1"}},
		{name: "the PostgreSQL form written second", names: []string{"customers_email_uq", "customers_email_key"}, want: []string{"customers_email_uq"}},
		{name: "the MySQL form", names: []string{"email", "email_2"}, want: []string{"email_2"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			database := dbschematogo.ConvertDBSchemaToGoSchema(twoUniqueSchema(nil, test.names...), "")

			c.Assert(constraintNames(database.Constraints), qt.DeepEquals, test.want)
			c.Assert(emailField(c, database).Unique, qt.IsTrue)
		})
	}
}

// TestConvert_ClearsAColumnsUniqueAnotherObjectDescribes is the control: the
// flag goes when the description names the key another way, so the document
// holds one object per key.
func TestConvert_ClearsAColumnsUniqueAnotherObjectDescribes(t *testing.T) {
	tests := []struct {
		name    string
		indexes []catalog.Index
		names   []string
		checks  []catalog.Constraint
	}{
		{
			name:  "a named UNIQUE alone",
			names: []string{"customers_email_uq"},
		},
		{
			name:  "two named UNIQUEs",
			names: []string{"customers_email_uq", "customers_email_uq2"},
		},
		{
			name:  "a CHECK under the name of the column's key",
			names: []string{"customers_email_uq"},
			checks: []catalog.Constraint{{
				TableName: "customers", Name: "customers_email_key", Type: "CHECK", ColumnNames: []string{"email"},
			}},
		},
		{
			name: "the server's name on an index the description keeps",
			indexes: []catalog.Index{{
				Name: "customers_email_key", TableName: "customers", Columns: []string{"email"},
				IsUnique: true, IncludeColumns: []string{"id"},
			}},
			names: []string{"customers_email_key", "customers_email_uq"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			schema := twoUniqueSchema(test.indexes, test.names...)
			schema.Constraints = append(schema.Constraints, test.checks...)

			database := dbschematogo.ConvertDBSchemaToGoSchema(schema, "")

			c.Assert(emailField(c, database).Unique, qt.IsFalse)
		})
	}
}
