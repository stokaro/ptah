package chconvert_test

import (
	"context"
	"fmt"

	"github.com/go-extras/go-kit/must"

	"ptah.run/core/schemaext"
	"ptah.run/dialect/clickhouse/chconvert"
	"ptah.run/dialect/clickhouse/chschema"
	"ptah.run/engine"
)

func ExampleService_ConvertFeatures() {
	runtime := must.Must(engine.New(engine.Provider{
		ID: "example.org/clickhouse", Targets: []engine.Target{{Name: "clickhouse"}}, Codecs: chschema.Codecs(),
		Conversions: []engine.Conversion{{Target: "clickhouse", Kinds: []schemaext.Kind{chschema.TableKind}, Service: chconvert.Service{}}},
	}))
	values := must.Must(runtime.ConvertFeatures(context.Background(), schemaext.ConversionRequest{
		Target: "clickhouse", From: schemaext.Observed, To: schemaext.Desired,
		Values: []schemaext.Value{&chschema.ObservedTable{Engine: "MergeTree", OrderBy: "tenant, id", PrimaryKey: ""}},
	}))
	desired := values[0].(*chschema.DesiredTable)
	fmt.Printf("ORDER BY: %s %q\n", desired.OrderBy.State, desired.OrderBy.Value)
	fmt.Printf("PRIMARY KEY: %s %q\n", desired.PrimaryKey.State, desired.PrimaryKey.Value)
	// Output:
	// ORDER BY: explicit "tenant, id"
	// PRIMARY KEY: explicit ""
}
