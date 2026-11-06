package atlasfilter_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/internal/atlasfilter"
)

// A selection keeps a role that a kept grant names. What an object's owner holds by
// default names the owner in every read, and ownership is no reason to describe a
// role, so that row does not keep it. A privilege the owner revoked from itself
// does: a description states it as a revoke naming the role.
func TestScopeDatabaseKeepsARoleAKeptGrantNames(t *testing.T) {
	for _, test := range []struct {
		name  string
		grant catalog.Grant
		want  int
	}{
		{name: "a grant", grant: catalog.Grant{Role: "app", ObjectType: "TABLE", ObjectName: "orders", Privilege: "SELECT"}, want: 1},
		{name: "what the owner holds by default", grant: catalog.Grant{Role: "app", ObjectType: "TABLE", ObjectName: "orders", Privilege: "SELECT", Implicit: true}},
		{name: "what the owner revoked from itself", grant: catalog.Grant{Role: "app", ObjectType: "TABLE", ObjectName: "orders", Privilege: "SELECT", Implicit: true, IsPartialRevoke: true}, want: 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			live := &catalog.Database{
				Tables: []catalog.Table{{Name: "orders"}},
				Roles:  []catalog.Role{{Name: "app"}},
				Grants: []catalog.Grant{test.grant},
			}

			read, err := atlasfilter.ScopeDatabase(live, atlasfilter.Scope{Selections: ownerSelections(), Include: []string{"orders"}})

			c.Assert(err, qt.IsNil)
			c.Assert(read.Grants, qt.HasLen, 1)
			c.Assert(read.Roles, qt.HasLen, test.want)
		})
	}
}
