package chcompare_test

import (
	"context"
	"fmt"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/clickhouse/chcompare"
	"ptah.run/dialect/clickhouse/chschema"
)

// ExampleService_CompareFacets compares captured settings without a database.
// The prior observation supplies every property that the declaration retains.
func ExampleService_CompareFacets() {
	before := &chschema.ObservedTable{Engine: "MergeTree", OrderBy: "id", PrimaryKey: "id", TTL: "ts + toIntervalDay(7)"}
	after := before.Desired()
	after.TTL = chschema.Setting{State: chschema.Explicit, Value: "ts + toIntervalDay(30)"}
	current, err := schemaext.NewFacets(before)
	if err != nil {
		fmt.Println(err)
		return
	}
	desired, err := schemaext.NewFacets(after)
	if err != nil {
		fmt.Println(err)
		return
	}
	subject := objectidentity.NewBuilder(identifier.ForDialect("clickhouse")).Table("events")
	result, err := (chcompare.Service{}).CompareFacets(context.Background(), schemaext.FacetComparisonRequest{
		Target: "clickhouse", Kinds: []schemaext.Kind{chschema.TableKind},
		Owners:  []schemaext.ParentState{{Subject: subject, Desired: true, Current: true}},
		Desired: schemaext.FacetState{Records: []schemaext.FacetRecord{{Subject: subject, Values: desired}}},
		Current: schemaext.FacetState{Records: []schemaext.FacetRecord{{Subject: subject, Values: current}}},
	})
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println("changes:", len(result.Changes))
	fmt.Println("undecided:", len(result.Undecided))
	// Output:
	// changes: 1
	// undecided: 0
}
