package featureplan_test

import (
	"errors"
	"fmt"

	"ptah.run/core/featureplan"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemavalidation"
)

// ExampleResult_Err preserves a completed planning refusal as data for transport
// and converts it into a typed error only at the local application boundary.
func ExampleResult_Err() {
	request := featureplan.Request{Target: "custom"}
	result := featureplan.Result{Complete: true, Diagnostics: []featureplan.Diagnostic{{Problem: schemavalidation.Diagnostic{
		Code: schemavalidation.UnsupportedFeature, Kind: "example.org/stream", Object: "events", Message: "the stream is managed by replication",
	}}}}
	err := result.Err(request)
	fmt.Println(errors.Is(err, ptaherr.ErrUnsupportedFeature))
	fmt.Println(result.Diagnostics[0].Problem.Object)
	// Output:
	// true
	// events
}
