package schemaprojection_test

import (
	"fmt"

	"ptah.run/catalog"
	"ptah.run/core/schemaprojection"
)

// ExampleTableState_Clone shows how a projector changes its prediction without
// changing the captured input that another direction or service still needs.
func ExampleTableState_Clone() {
	before := schemaprojection.TableState{
		Table: catalog.Table{Name: "items", Columns: []catalog.Column{{Name: "id", IsNullable: "YES"}}},
	}
	after := before.Clone()
	after.Table.Columns[0].IsNullable = "NO"
	fmt.Println(before.Table.Columns[0].IsNullable)
	fmt.Println(after.Table.Columns[0].IsNullable)
	// Output:
	// YES
	// NO
}
