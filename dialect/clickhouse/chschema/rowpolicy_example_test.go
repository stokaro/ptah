package chschema_test

import (
	"context"
	"fmt"

	"ptah.run/core/schemaext"
	"ptah.run/dialect/clickhouse/chschema"
	"ptah.run/engine"
)

// ExampleRowPolicyCodecs declares a restrictive row policy for every user but
// one as a feature object, and encodes it through a runtime that registers the
// codecs. The exception list is a set and encodes in byte order; the policy's
// database, table and name travel in its identity, not in the payload.
func ExampleRowPolicyCodecs() {
	filter := "tenant_id = 1"
	object, err := chschema.DesiredRowPolicyObject(chschema.RowPolicyRef("analytics", "orders", "tenant_rows"), chschema.DesiredRowPolicy{
		Filter:      &filter,
		Composition: chschema.Restrictive,
		Roles:       chschema.RoleSelection{All: true, Except: []string{"etl", "admin"}},
	})
	if err != nil {
		fmt.Println(err)
		return
	}
	runtime, err := engine.New(engine.Provider{ID: "ptah.run/clickhouse", Codecs: chschema.RowPolicyCodecs()})
	if err != nil {
		fmt.Println(err)
		return
	}
	encoded, err := runtime.Codecs().Encode(context.Background(), schemaext.Desired, []schemaext.Payload{object.Value})
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(object.Ref.Schema.Source, object.Ref.Parent.Source, object.Ref.Name.Source)
	fmt.Println(string(encoded[0].Payload))
	// Output:
	// analytics orders tenant_rows
	// {"composition":"restrictive","filter":"tenant_id = 1","roles":{"all":true,"except":["admin","etl"]}}
}

// ExampleValidateRowPolicyRef shows a database-wide policy, written ON db.*,
// refused by name: the model holds policies on one table, and a policy that
// applies to every table of a database is a different object.
func ExampleValidateRowPolicyRef() {
	fmt.Println(chschema.ValidateRowPolicyRef(chschema.RowPolicyRef("analytics", "orders", "tenant_rows")))
	fmt.Println(chschema.ValidateRowPolicyRef(chschema.RowPolicyRef("analytics", "", "tenant_rows")))
	// Output:
	// <nil>
	// invalid feature value: ClickHouse row policy "tenant_rows" has no table; a database-wide policy (ON db.*) is not supported, only a policy on one table
}
