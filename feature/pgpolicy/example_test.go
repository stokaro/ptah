package pgpolicy_test

import (
	"context"
	"fmt"

	"ptah.run/core/schemaext"
	"ptah.run/engine"
	"ptah.run/feature/pgpolicy"
)

// ExamplePolicyCodecs declares a policy as a feature object and encodes it
// through a runtime that registers the owner's codecs. The role list encodes in
// its canonical order, keywords first, and the omitted command and composition
// stay omitted: they request PostgreSQL's defaults rather than naming them.
func ExamplePolicyCodecs() {
	using := "tenant_id = current_setting('app.tenant')::int"
	object, err := pgpolicy.DesiredPolicyObject(pgpolicy.PolicyRef("app", "orders", "tenant_isolation"), pgpolicy.DesiredPolicy{
		Roles: []pgpolicy.RoleSelector{{Name: "reader"}, {Keyword: pgpolicy.CurrentUser}},
		Using: &using,
	})
	if err != nil {
		fmt.Println(err)
		return
	}
	runtime, err := engine.New(engine.Provider{ID: pgpolicy.Owner, Codecs: pgpolicy.Codecs()})
	if err != nil {
		fmt.Println(err)
		return
	}
	encoded, err := runtime.Codecs().Encode(context.Background(), schemaext.Desired, []schemaext.Payload{object.Value})
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(object.Ref.Parent.Source, object.Ref.Name.Source)
	fmt.Println(string(encoded[0].Payload))
	// Output:
	// orders tenant_isolation
	// {"roles":[{"keyword":"CURRENT_USER"},{"name":"reader"}],"using":"tenant_id = current_setting('app.tenant')::int"}
}
