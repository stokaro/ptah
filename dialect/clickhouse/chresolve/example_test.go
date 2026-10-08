package chresolve_test

import (
	"fmt"

	"ptah.run/dialect/clickhouse/chresolve"
	"ptah.run/dialect/clickhouse/chschema"
)

// ExampleTable keeps the author's default request while preparing the inherited
// key against an offline catalog fixture. It does not claim a migration ran.
func ExampleTable() {
	result, err := chresolve.Table(chresolve.Request{
		Desired: &chschema.DesiredTable{PrimaryKey: chschema.Setting{State: chschema.Default}},
		Current: &chschema.ObservedTable{Engine: "MergeTree", OrderBy: "tenant, id", PrimaryKey: "tenant"},
	})
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(result.Declared.PrimaryKey.State)
	fmt.Println(result.Prepared.PrimaryKey.Value)
	fmt.Println(result.Origins.OrderBy, result.Origins.PrimaryKey)
	// Output:
	// default
	// tenant, id
	// observation creation-rule
}
