package schemavalidation_test

import (
	"errors"
	"fmt"

	"ptah.run/core/ptaherr"
	"ptah.run/core/schemavalidation"
)

// ExampleResult_Err shows how planning callers preserve a completed schema
// refusal as a typed error. Reporting callers can present the same data directly.
func ExampleResult_Err() {
	result := schemavalidation.Result{Complete: true, Diagnostics: []schemavalidation.Diagnostic{{
		Code: schemavalidation.UnsupportedFeature, Kind: "table", Object: "orders",
		Feature: "partitioning", Message: "partitioning is unsupported by this target",
	}}}
	fmt.Println(result.Diagnostics[0].Object)
	fmt.Println(errors.Is(result.Err("custom"), ptaherr.ErrUnsupportedFeature))
	fmt.Println(errors.Is((schemavalidation.Result{}).Err("custom"), schemavalidation.ErrInvalidResult))
	// Output:
	// orders
	// true
	// true
}
