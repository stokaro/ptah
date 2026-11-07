package manageddata_test

import (
	"errors"
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

// ExampleLoadRows_outsideRoot shows the boundary a loader puts around a
// declaration's path. The path is data, and a schema is not always one the reader
// wrote, so a file outside the root is refused and nothing in it is read.
func ExampleLoadRows_outsideRoot() {
	_, err := manageddata.LoadRows("project", schemamodel.ManagedData{
		Table: "regions",
		File:  "../secret.yaml",
	})

	fmt.Println(errors.Is(err, manageddata.ErrOutsideRoot))
	// Output: true
}
