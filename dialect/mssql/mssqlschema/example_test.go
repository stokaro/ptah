package mssqlschema_test

import (
	"context"
	"fmt"

	"ptah.run/core/schemaext"
	"ptah.run/dialect/mssql/mssqlschema"
	"ptah.run/engine"
)

// ExampleCodecs declares a disabled policy that binds two tables, one of them
// with both a filter and a BEFORE UPDATE block predicate, as one schema-scoped
// feature object, and encodes it through a runtime that registers the owner's
// codecs. The predicates encode in their canonical order, and the omitted
// schema binding stays omitted: it requests SQL Server's default rather than
// naming it.
func ExampleCodecs() {
	disabled := false
	function := mssqlschema.ObjectName{Schema: "rls", Name: "fn_tenant"}
	orders := mssqlschema.ObjectName{Schema: "app", Name: "orders"}
	invoices := mssqlschema.ObjectName{Schema: "billing", Name: "invoices"}
	object, err := mssqlschema.DesiredSecurityPolicyObject(mssqlschema.SecurityPolicyRef("rls", "tenancy"), mssqlschema.DesiredSecurityPolicy{
		Predicates: []mssqlschema.Predicate{
			{Type: mssqlschema.Filter, Function: function, Arguments: []string{"tenant_id"}, Table: invoices},
			{Type: mssqlschema.Block, Function: function, Arguments: []string{"tenant_id"}, Table: orders, Operation: mssqlschema.BeforeUpdate},
			{Type: mssqlschema.Filter, Function: function, Arguments: []string{"tenant_id"}, Table: orders},
		},
		Enabled: &disabled,
	})
	if err != nil {
		fmt.Println(err)
		return
	}
	runtime, err := engine.New(engine.Provider{ID: mssqlschema.Owner, Codecs: mssqlschema.Codecs()})
	if err != nil {
		fmt.Println(err)
		return
	}
	encoded, err := runtime.Codecs().Encode(context.Background(), schemaext.Desired, []schemaext.Payload{object.Value})
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(object.Ref.Schema.Source, object.Ref.Name.Source, object.Ref.Parent.Empty())
	fmt.Println(string(encoded[0].Payload))
	// Output:
	// rls tenancy true
	// {"enabled":false,"predicates":[{"arguments":["tenant_id"],"function":{"name":"fn_tenant","schema":"rls"},"operation":"BEFORE UPDATE","table":{"name":"orders","schema":"app"},"type":"BLOCK"},{"arguments":["tenant_id"],"function":{"name":"fn_tenant","schema":"rls"},"table":{"name":"orders","schema":"app"},"type":"FILTER"},{"arguments":["tenant_id"],"function":{"name":"fn_tenant","schema":"rls"},"table":{"name":"invoices","schema":"billing"},"type":"FILTER"}]}
}
