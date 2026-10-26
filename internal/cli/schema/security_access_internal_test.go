package schema

// White-box testing required: these tests cover the catalog-to-analyzer boundary. Its input is the
// completed live read; a command test would need a database with partial
// ownership metadata to reach the same refusal.

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/coverage"
)

func TestSecurityOwnersRetainKnownLoginStates(t *testing.T) {
	c := qt.New(t)
	owners, err := objectOwners(&catalog.Database{ObjectOwners: []catalog.ObjectOwner{
		{Kind: "table", Name: "items", Owner: "app", OwnerCanLogin: new(true)},
		{Kind: "table", Name: "history", Owner: "store", OwnerCanLogin: new(false)},
	}})
	c.Assert(err, qt.IsNil)
	c.Assert(owners, qt.HasLen, 2)
	c.Assert(owners[0].OwnerCanLogin, qt.IsTrue)
	c.Assert(owners[1].OwnerCanLogin, qt.IsFalse)
}

func TestSecurityRefusesUnknownOwnerLogin(t *testing.T) {
	c := qt.New(t)
	owners, err := objectOwners(&catalog.Database{ObjectOwners: []catalog.ObjectOwner{{Kind: "table", Name: "items", Owner: "app"}}})
	c.Assert(err, qt.ErrorMatches, `.*did not establish whether owner "app".*`)
	c.Assert(owners, qt.IsNil)
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
