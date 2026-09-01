package schemaprep_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/schemamodel"
	"go.5x5.cz/ptah/internal/schemaprep"
)

func TestTableCheckConstraints(t *testing.T) {
	c := qt.New(t)
	t.Parallel()
	table := schemamodel.Table{StructName: "Product", Schema: "store", Name: "products", Checks: []string{
		"price > 0",
		"stock >= 0",
		"published_at IS NOT NULL",
	}}
	declared := []schemamodel.Constraint{
		{StructName: "Product", Name: "products_check", Type: "UNIQUE"},
		{Table: "store.products", Name: "stock_nonnegative", Type: "CHECK", CheckExpression: "stock >= 0"},
	}

	got := schemaprep.TableCheckConstraints(table, declared)
	c.Assert(got, qt.DeepEquals, []schemamodel.Constraint{
		{StructName: "Product", Name: "products_check1", Type: "CHECK", Table: "store.products", CheckExpression: "price > 0"},
		{StructName: "Product", Name: "products_check2", Type: "CHECK", Table: "store.products", CheckExpression: "published_at IS NOT NULL"},
	})
}
