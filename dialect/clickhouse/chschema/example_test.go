package chschema_test

import (
	"context"
	"fmt"

	"ptah.run/core/schemaext"
	"ptah.run/dialect/clickhouse/chschema"
	"ptah.run/engine"
)

// ExampleObservedTable_Desired shows that an inspected empty primary key stays
// explicit, separate from an omitted setting or a request for the target default.
func ExampleObservedTable_Desired() {
	observed := &chschema.ObservedTable{Engine: "MergeTree", OrderBy: "event_time"}
	desired := observed.Desired()
	fmt.Printf("primary key: %s %q\n", desired.PrimaryKey.State, desired.PrimaryKey.Value)
	projected, err := desired.Observed()
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println("preserved:", projected.Equal(observed))
	// Output:
	// primary key: explicit ""
	// preserved: true
}

// ExampleCodecs registers the model with an application-selected provider and
// encodes explicit empty intent without importing the bundled engine runtime.
func ExampleCodecs() {
	runtime, err := engine.New(engine.Provider{ID: "example.org/clickhouse", Codecs: chschema.Codecs()})
	if err != nil {
		fmt.Println(err)
		return
	}
	value := &chschema.DesiredTable{PrimaryKey: chschema.Setting{State: chschema.Explicit}}
	envelopes, err := runtime.Codecs().Encode(context.Background(), schemaext.Desired, []schemaext.Payload{value})
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(envelopes[0].Owner)
	fmt.Println(string(envelopes[0].Payload))
	// Output:
	// example.org/clickhouse
	// {"primary_key":{"state":"explicit"}}
}
