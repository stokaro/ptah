package chreport_test

import (
	"context"
	"fmt"

	"ptah.run/core/schemaext"
	"ptah.run/dialect/clickhouse/chreport"
	"ptah.run/dialect/clickhouse/chschema"
	"ptah.run/engine"
)

// ExampleService reports captured values using only an explicitly selected
// provider. No built-in composition or live database is required.
func ExampleService() {
	runtime, err := engine.New(engine.Provider{
		ID: "ptah.run/clickhouse", Targets: []engine.Target{{Name: "clickhouse"}}, Codecs: chschema.Codecs(),
		Reporting: []engine.Reporting{{Representation: schemaext.Observed, Definitions: chreport.Definitions(), Service: chreport.Service{}}},
	})
	if err != nil {
		fmt.Println(err)
		return
	}
	report, err := runtime.ReportFeatures(context.Background(), schemaext.ReportingRequest{
		Target: "clickhouse", Representation: schemaext.Observed, Values: []schemaext.Value{&chschema.ObservedTable{Engine: "Memory"}},
	})
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(report.Definitions[0].DisplayName, report.Values[0].Counts[0].Value)
	// Output: ClickHouse table settings 1
}
