package schemacapture_test

import (
	"fmt"

	"ptah.run/core/schemacapture"
	"ptah.run/core/schemamodel"
)

// ExampleTableDeclaration_Clone shows how a service isolates a planning capture
// before changing its local common definitions. Feature knowledge stays unknown
// unless the source explicitly captured it.
func ExampleTableDeclaration_Clone() {
	captured := schemacapture.TableDeclaration{
		Table:  schemamodel.Table{Name: "orders", PrimaryKey: []string{"id"}},
		Fields: []schemamodel.Field{{Name: "id", Type: "int64"}},
	}
	planned := captured.Clone()
	planned.Table.PrimaryKey[0] = "external_id"
	fmt.Println(captured.HasTable(), captured.Table.PrimaryKey[0])
	fmt.Println(planned.Table.PrimaryKey[0])
	// Output:
	// true id
	// external_id
}
