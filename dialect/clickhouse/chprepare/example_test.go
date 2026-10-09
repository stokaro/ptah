package chprepare_test

import (
	"context"
	"fmt"

	"ptah.run/core/schemacapture"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/core/schemapreparation"
	"ptah.run/core/schemaprojection"
	"ptah.run/dialect/clickhouse/chprepare"
	"ptah.run/dialect/clickhouse/chschema"
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

// ExampleService_ProjectTableCreations predicts storage defaults for a table
// without platform properties while keeping the declaration unchanged.
func ExampleService_ProjectTableCreations() {
	request := schemaprojection.TableCreationRequest{Target: "clickhouse", Tables: []schemaprojection.TableCreationInput{{
		Declaration: schemacapture.TableDeclaration{
			Table:  schemamodel.Table{Name: "events"},
			Fields: []schemamodel.Field{{Name: "id", Type: "UInt64", Primary: true}},
		},
	}}}
	result, err := (chprepare.Service{}).ProjectTableCreations(context.Background(), request)
	if err != nil {
		fmt.Println(err)
		return
	}
	settings, _, err := schemaext.FacetAs[*chschema.DesiredTable](result.Tables[0].Facets, chschema.TableKind)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println("predicted engine:", settings.Engine.Value)
	fmt.Println("predicted key:", result.Tables[0].ColumnPrimaryKeys)
	fmt.Println("source facets:", request.Tables[0].Declaration.Table.Facets.Len())
	// Output:
	// predicted engine: MergeTree
	// predicted key: [id]
	// source facets: 0
}
