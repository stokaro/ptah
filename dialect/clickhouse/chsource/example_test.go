package chsource_test

import (
	"context"
	"fmt"

	"github.com/go-extras/go-kit/must"

	"ptah.run/core/schemaext"
	"ptah.run/dialect/clickhouse/chschema"
	"ptah.run/dialect/clickhouse/chsource"
	"ptah.run/engine"
)

func ExampleService() {
	runtime := must.Must(engine.New(engine.Provider{
		ID: "example.org/clickhouse", Targets: []engine.Target{{Name: "clickhouse"}}, Codecs: chschema.Codecs(),
		Properties: []engine.PropertySource{{Target: "clickhouse", Format: schemaext.TablePlatformProperties,
			Definitions: chsource.Definitions(), Service: chsource.Service{},
		}},
	}))
	fragments := must.Must(runtime.EncodeProperties(context.Background(), schemaext.PropertyEncodeRequest{
		Target: "clickhouse", Format: schemaext.TablePlatformProperties,
		Values: []schemaext.Value{&chschema.DesiredTable{
			OrderBy: chschema.Setting{State: chschema.Default}, PrimaryKey: chschema.Setting{State: chschema.Explicit},
		}},
	}))
	fmt.Printf("order_by.state=%q primary_key=%q\n", fragments[0].Properties["order_by.state"], fragments[0].Properties["primary_key"])
	values := must.Must(runtime.DecodeProperties(context.Background(), schemaext.PropertyDecodeRequest{
		Target: "clickhouse", Format: schemaext.TablePlatformProperties, Fragments: fragments,
	}))
	table := values[0].(*chschema.DesiredTable)
	fmt.Println(table.OrderBy.State, table.PrimaryKey.State)
	// Output:
	// order_by.state="default" primary_key=""
	// default explicit
}
