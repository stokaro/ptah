package ydbplan_test

import (
	"context"
	"fmt"

	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/featureplan"
	"ptah.run/core/objectidentity"
	"ptah.run/core/plangraph"
	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemacapture"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/ydb/ydbdiff"
	"ptah.run/dialect/ydb/ydbplan"
	"ptah.run/dialect/ydb/ydbschema"
)

// ExampleService plans an addition against a captured existing table with a
// known empty stream namespace. The graph is scheduled before reading its
// operations. A larger host must include its common contributions too.
func ExampleService() {
	ctx := context.Background()
	semantics := identifier.ForDialect("ydb")
	parent := objectidentity.NewBuilder(semantics).TableParts("", "items")
	spec := ydbschema.ChangefeedSpec{Name: "updates", Mode: "UPDATES", Format: "JSON"}
	request := featureplan.Request{
		Target: "ydb", Identifiers: semantics, Capabilities: capability.YDB262(),
		Tables: []featureplan.Table{{Subject: parent,
			Desired: schemacapture.TableDeclaration{
				Table:           schemamodel.Table{Name: "items"},
				OwnedObjects:    must.Must(schemaext.NewObjects(ydbschema.DesiredObject("", "items", spec))),
				FeatureCoverage: must.Must(ydbschema.ChangefeedCoverage(schemaext.Desired, nil)),
			},
			Current: schemacapture.TableObservation{
				Table:           catalog.Table{Name: "items"},
				FeatureCoverage: must.Must(ydbschema.ChangefeedCoverage(schemaext.Observed, nil)),
			},
		}},
		Changes: []schemaext.ChangeRecord{{Subject: ydbschema.ChangefeedRef("", "items", "updates"),
			Value: &ydbdiff.Changefeed{After: &ydbschema.DesiredChangefeed{Spec: spec}},
		}},
	}
	result, err := (ydbplan.Service{}).PlanFeatures(ctx, request)
	if err != nil {
		fmt.Println(err)
		return
	}
	plan, err := plangraph.Schedule(ctx, result.Contributions...)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(result.Changes[0].Strategy)
	fmt.Println(plan.Steps[0].Payload.Payload.Kind())
	fmt.Println(plan.Steps[0].Transaction)
	// Output:
	// apply changefeed operations
	// ptah.run/ydb/add-changefeed
	// forbidden
}
