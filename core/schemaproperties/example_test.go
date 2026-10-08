package schemaproperties_test

import (
	"context"
	"fmt"

	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/core/schemaproperties"
	"ptah.run/dialect/clickhouse/chschema"
	"ptah.run/dialect/clickhouse/chsource"
	"ptah.run/engine"
)

// ExampleDecodeTables selects only the model and property services needed for
// source conversion. It attaches table properties without any bundled runtime.
func ExampleDecodeTables() {
	runtime, err := engine.New(engine.Provider{
		ID: "ptah.run/clickhouse", Targets: []engine.Target{{Name: "clickhouse"}}, Codecs: chschema.Codecs(),
		Properties: []engine.PropertySource{{
			Target: "clickhouse", Format: schemaext.TablePlatformProperties,
			Definitions: chsource.Definitions(), Service: chsource.Service{},
		}},
	})
	if err != nil {
		fmt.Println(err)
		return
	}
	source := &schemamodel.Database{Tables: []schemamodel.Table{{Name: "events", Overrides: map[string]map[string]string{
		"clickhouse": {"order_by.state": "default", "primary_key": ""},
	}}}}
	decoded, err := schemaproperties.DecodeTables(context.Background(), source, "clickhouse", runtime)
	if err != nil {
		fmt.Println(err)
		return
	}
	exported, err := schemaproperties.EncodeTables(context.Background(), decoded, "clickhouse", runtime)
	if err != nil {
		fmt.Println(err)
		return
	}
	properties := exported.Tables[0].Overrides["clickhouse"]
	fmt.Println(decoded.Tables[0].Facets.TargetScope(chschema.TableKind))
	fmt.Printf("default=%s empty=%q\n", properties["order_by.state"], properties["primary_key"])
	// Output:
	// [clickhouse]
	// default=default empty=""
}
