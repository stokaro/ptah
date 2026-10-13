package mssqlroutine_test

import (
	"fmt"

	"ptah.run/dialect/mssql/mssqlroutine"
)

// ExampleNormalizeParameters compares optional AS spellings without discarding
// the dimensions of a parameter's type.
func ExampleNormalizeParameters() {
	fmt.Println(mssqlroutine.NormalizeParameters("@amount AS DECIMAL(10, 2), @label AS NVARCHAR(50)"))
	// Output: @amount DECIMAL(10,2), @label NVARCHAR(50)
}
