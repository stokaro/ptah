package chprepare_test

import (
	"context"
	"fmt"

	"ptah.run/core/schemacapture"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/core/schemapreparation"
	"ptah.run/dialect/clickhouse/chprepare"
)

// ExampleService_PrepareTables preserves authored fields while resolving the
// key membership the ClickHouse catalog reports for an ORDER BY expression.
func ExampleService_PrepareTables() {
	request := schemapreparation.Request{Target: "clickhouse", Tables: []schemapreparation.Table{{
		Desired: schemacapture.TableDeclaration{
			Table:  schemamodel.Table{Name: "events", Overrides: map[string]map[string]string{"clickhouse": {"order_by": "id"}}},
			Fields: []schemamodel.Field{{Name: "id", Type: "UInt64"}},
		},
		CurrentKnowledge: schemaext.Knowledge{State: schemaext.Absent},
	}}}
	result, err := (chprepare.Service{}).PrepareTables(context.Background(), request)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println("source column flag:", request.Tables[0].Desired.Fields[0].Primary)
	fmt.Println("prepared column flag:", result.Tables[0].Desired.Fields[0].Primary)
	// Output:
	// source column flag: false
	// prepared column flag: true
}
