package chschema_test

import (
	"fmt"

	"github.com/go-extras/go-kit/must"

	"ptah.run/dialect/clickhouse/chschema"
)

func ExampleObservedIndex_Desired() {
	observed := &chschema.ObservedIndex{Expression: "payload", IndexType: "minmax", Granularity: 1}
	desired := observed.Desired()
	fmt.Println(desired.IndexType.State, desired.Granularity.State)
	fmt.Println(must.Must(desired.Observed()).Equal(observed))
	// Output:
	// explicit explicit
	// true
}

func ExampleIndexCodecs() {
	value := &chschema.DesiredIndex{Expression: "payload", IndexType: chschema.Setting{State: chschema.Default},
		Granularity: chschema.GranularitySetting{State: chschema.Default}}
	encoded := must.Must(chschema.IndexCodecs()[0].Canonical(value))
	fmt.Println(string(encoded))
	// Output:
	// {"expression":"payload","index_type":{"state":"default"},"granularity":{"state":"default"}}
}
