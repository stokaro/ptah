package schema

// White-box testing required: these tests cover the catalog-to-analyzer boundary. Its input is the
// completed live read; a command test would need a database with partial
// ownership metadata to reach the same conversion or refusal.

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/coverage"
	"ptah.run/internal/schemasecurity"
)

func TestSecurityOwnersRetainLoginStates(t *testing.T) {
	c := qt.New(t)
	owners := objectOwners(&catalog.Database{ObjectOwners: []catalog.ObjectOwner{
		{Kind: "table", Name: "items", Owner: "app", OwnerCanLogin: new(true)},
		{Kind: "table", Name: "history", Owner: "store", OwnerCanLogin: new(false)},
		{Kind: "schema", Name: "shop", Owner: "root@builtin"},
	}})
	c.Assert(owners, qt.DeepEquals, []schemasecurity.ObjectOwner{
		{Kind: "table", Name: "items", Owner: "app", OwnerCanLogin: new(true)},
		{Kind: "table", Name: "history", Owner: "store", OwnerCanLogin: new(false)},
		{Kind: "schema", Name: "shop", Owner: "root@builtin"},
	})
}

func TestSecurityRefusesIndependentAccessGaps(t *testing.T) {
	for _, kind := range []coverage.Kind{coverage.Role, coverage.ObjectOwner, coverage.RoleMembership} {
		t.Run(string(kind), func(t *testing.T) {
			c := qt.New(t)
			live := &catalog.Database{NotDescribed: coverage.Set{}.With(coverage.Refused(kind))}
			c.Assert(refuseUnreadPrincipals(live), qt.IsNotNil)
		})
	}
}
