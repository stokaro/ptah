package schemaprep_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/schemamodel"
	"go.5x5.cz/ptah/internal/schemaprep"
)

func TestParseForeignKeyReference(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		in   string
		want *schemaprep.ForeignKeyReference
	}{
		{name: "empty", in: "", want: nil},
		{name: "default column", in: " users ", want: &schemaprep.ForeignKeyReference{Table: "users", Column: "id"}},
		{name: "named column", in: " users (account_id) ", want: &schemaprep.ForeignKeyReference{Table: "users", Column: "account_id"}},
		{name: "several opens", in: "users((id)", want: nil},
		{name: "trailing text", in: "users(id) trailing", want: nil},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			t.Parallel()
			c.Assert(schemaprep.ParseForeignKeyReference(test.in), qt.DeepEquals, test.want)
		})
	}
}

func TestEnumsFor(t *testing.T) {
	c := qt.New(t)
	t.Parallel()
	enums := []schemamodel.Enum{
		{Name: "status"},
		{Name: "priority"},
		{Name: "unused"},
	}
	fields := []schemamodel.Field{
		{Type: "priority"},
		{Type: "status"},
		{Type: "priority"},
	}

	c.Assert(schemaprep.EnumsFor(fields, enums), qt.DeepEquals, []schemamodel.Enum{
		{Name: "priority"},
		{Name: "status"},
	})
}

func TestGenerateForeignKeyName(t *testing.T) {
	c := qt.New(t)
	t.Parallel()
	c.Assert(schemaprep.GenerateForeignKeyName("OrderItems", "ProductID"), qt.Equals, "fk_orderitems_productid")
}
