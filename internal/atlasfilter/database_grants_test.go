package atlasfilter_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/atlasfilter"
)

// Database grants follow an explicit principal selection, never a table-only
// selection. Schema scopes and exclusions cannot assign them an owning schema.
func TestDatabaseGrantsFollowPrincipalScope(t *testing.T) {
	for _, test := range []struct {
		name  string
		scope atlasfilter.Scope
		count int
	}{
		{"directory", atlasfilter.Scope{Schemas: []string{"shop"}}, 1},
		{"principal", atlasfilter.Scope{Include: []string{"app[type=role]"}}, 1},
		{"table", atlasfilter.Scope{Include: []string{"shop.orders[type=table]"}}, 0},
		{"excluded default schema", atlasfilter.Scope{Exclude: []string{"other[type=schema]"}, DefaultSchema: "other"}, 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			grants := []schemamodel.Grant{{Role: "app", OnDatabase: true, Privileges: []string{"CONNECT"}}}
			desired := &schemamodel.Database{Roles: []schemamodel.Role{{Name: "app"}}, Tables: []schemamodel.Table{{Name: "orders", Schema: "shop"}}, Grants: grants, RevokedGrants: grants}
			live := &catalog.Database{Roles: []catalog.Role{{Name: "app"}}, Tables: []catalog.Table{{Name: "orders", Schema: "shop"}}, Grants: []catalog.Grant{{Role: "app", ObjectType: "DATABASE", Privilege: "CONNECT"}}}
			got, err := atlasfilter.ScopeGenerated(desired, test.scope)
			c.Assert(err, qt.IsNil)
			read, err := atlasfilter.ScopeDatabase(live, test.scope)
			c.Assert(err, qt.IsNil)
			c.Assert(got.Grants, qt.HasLen, test.count)
			c.Assert(got.RevokedGrants, qt.HasLen, test.count)
			c.Assert(read.Grants, qt.HasLen, test.count)
		})
	}
}
