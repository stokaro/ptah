package constraintowner_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemamodel"
	"ptah.run/internal/constraintowner"
)

var ownerTables = []schemamodel.Table{
	{StructName: "Order", Name: "orders", Schema: "shop"},
	{StructName: "Order", Name: "orders_archive", Schema: "shop"},
	{StructName: "Item", Name: "items"},
}

// A constraint belongs to the one table its struct declares, narrowed by the
// table name it carries, bare or qualified.
func TestTable_HappyPath(t *testing.T) {
	tests := []struct {
		name       string
		constraint schemamodel.Constraint
		want       string
	}{
		{name: "the struct declares one table", constraint: schemamodel.Constraint{StructName: "Item"}, want: "items"},
		{name: "a bare table name narrows the struct", constraint: schemamodel.Constraint{StructName: "Order", Table: "orders"}, want: "shop.orders"},
		{name: "a qualified table name narrows the struct", constraint: schemamodel.Constraint{StructName: "Order", Table: "shop.orders_archive"}, want: "shop.orders_archive"},
		{name: "a table name alone", constraint: schemamodel.Constraint{Table: "items"}, want: "items"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			table, ok := constraintowner.Table(test.constraint, ownerTables)

			c.Assert(ok, qt.IsTrue)
			c.Assert(table.QualifiedName(), qt.Equals, test.want)
			c.Assert(constraintowner.TableName(test.constraint, ownerTables), qt.Equals, test.want)
		})
	}
}

// A constraint that matches no table, or more than one, belongs to none for
// certain, and TableName answers the name it carries.
func TestTable_FailurePath(t *testing.T) {
	tests := []struct {
		name       string
		constraint schemamodel.Constraint
		wantName   string
	}{
		{name: "two tables of one struct", constraint: schemamodel.Constraint{StructName: "Order"}, wantName: ""},
		{name: "no table of that name", constraint: schemamodel.Constraint{StructName: "Item", Table: " ghosts "}, wantName: "ghosts"},
		{name: "no struct of that name", constraint: schemamodel.Constraint{StructName: "Ghost"}, wantName: ""},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			table, ok := constraintowner.Table(test.constraint, ownerTables)

			c.Assert(ok, qt.IsFalse)
			c.Assert(table, qt.DeepEquals, schemamodel.Table{})
			c.Assert(constraintowner.TableName(test.constraint, ownerTables), qt.Equals, test.wantName)
		})
	}
}
