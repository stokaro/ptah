package manageddata_test

import (
	"fmt"

	"ptah.run/core/manageddata"
	"ptah.run/core/schemamodel"
)

// ExampleResolveRows resolves carried artifact rows without opening a source
// file. A string and an integer with the same source text remain distinct.
func ExampleResolveRows() {
	declaration := schemamodel.ManagedData{
		Table: "countries",
		Rows: []schemamodel.ManagedRow{{
			"code": {Tag: "str", Text: "007"},
			"rank": {Tag: "int", Text: "007"},
		}},
	}
	rows, err := manageddata.ResolveRows(declaration)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Printf("code: %T %v\n", rows[0]["code"], rows[0]["code"])
	fmt.Printf("rank: %T %v\n", rows[0]["rank"], rows[0]["rank"])
	// Output:
	// code: string 007
	// rank: int 7
}
