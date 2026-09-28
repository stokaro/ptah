package foreignkeyscope_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemamodel"
	"ptah.run/internal/foreignkeyscope"
)

// described holds orders, unqualified, and accounts in the schema audit.
func described() schemamodel.Database {
	return schemamodel.Database{Tables: []schemamodel.Table{
		{StructName: "Orders", Name: "orders"},
		{StructName: "Accounts", Name: "accounts", Schema: "audit"},
	}}
}

// TestOutside_HappyPath names the schema of a reference into a schema the
// description holds nothing of, on each dialect that accepts such a key.
func TestOutside_HappyPath(t *testing.T) {
	tests := []struct {
		name      string
		dialect   string
		reference string
	}{
		{name: "mysql", dialect: "mysql", reference: "crm.customers"},
		{name: "mariadb", dialect: "mariadb", reference: "crm.customers"},
		{name: "postgres", dialect: "postgres", reference: "crm.customers"},
		{name: "cockroachdb", dialect: "cockroachdb", reference: "crm.customers"},
		{name: "mysql, public is a database like any other", dialect: "mysql", reference: "public.customers"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			schema, outside := foreignkeyscope.Outside(described(), test.dialect, test.reference)

			c.Assert(outside, qt.IsTrue)
			c.Assert(schema, qt.Equals, test.reference[:len(test.reference)-len(".customers")])
		})
	}
}

// TestOutside_FailurePath keeps a reference inside the description: an
// unqualified one, one into a schema the description holds a table of, one
// into public on PostgreSQL beside an unqualified table, and any reference on
// a dialect that does not accept a key out of its description.
func TestOutside_FailurePath(t *testing.T) {
	tests := []struct {
		name      string
		dialect   string
		reference string
	}{
		{name: "unqualified", dialect: "postgres", reference: "customers"},
		{name: "a described schema", dialect: "mysql", reference: "audit.customers"},
		{name: "public beside an unqualified table", dialect: "postgres", reference: "public.customers"},
		{name: "public beside an unqualified table on cockroachdb", dialect: "cockroachdb", reference: "public.customers"},
		{name: "yugabytedb", dialect: "yugabytedb", reference: "crm.customers"},
		{name: "sqlserver", dialect: "sqlserver", reference: "crm.customers"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			schema, outside := foreignkeyscope.Outside(described(), test.dialect, test.reference)

			c.Assert(outside, qt.IsFalse)
			c.Assert(schema, qt.Equals, "")
		})
	}
}

// TestReferences_HappyPath lists a description's keys, the column-declared ones
// before the table constraints, and leaves out every other constraint.
func TestReferences_HappyPath(t *testing.T) {
	c := qt.New(t)
	database := schemamodel.Database{
		Fields: []schemamodel.Field{
			{StructName: "Orders", Name: "id"},
			{StructName: "Orders", Name: "customer_id", Foreign: "crm.customers(id)"},
		},
		Constraints: []schemamodel.Constraint{
			{Name: "orders_total", Type: "CHECK"},
			{Name: "orders_line", Type: "FOREIGN KEY", ForeignTable: "lines"},
		},
	}

	c.Assert(foreignkeyscope.References(database), qt.DeepEquals, []foreignkeyscope.Reference{
		{Declaration: `field "customer_id"`, Table: "crm.customers"},
		{Declaration: `constraint "orders_line"`, Table: "lines"},
	})
}
