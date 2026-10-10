package targetscope_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ptaherr"
	"ptah.run/internal/targetscope"
)

func routes(c *qt.C) *targetscope.Routes {
	c.Helper()
	routes := targetscope.New()
	_, taken := routes.Claim(0, targetscope.Scope{Targets: []string{"postgres", "cockroachdb"}, Unscoped: true, Label: "PostgreSQL-family targets"})
	c.Assert(taken, qt.IsFalse)
	_, taken = routes.Claim(1, targetscope.Scope{Targets: []string{"sqlserver"}})
	c.Assert(taken, qt.IsFalse)
	return routes
}

func TestRoutes_Owner_HappyPath(t *testing.T) {
	tests := []struct {
		name      string
		targets   []string
		wantOwner int
		wantFound bool
	}{
		{name: "no target", wantOwner: 0, wantFound: true},
		{name: "an owner's targets", targets: []string{"cockroachdb", "postgres"}, wantOwner: 0, wantFound: true},
		{name: "an alias", targets: []string{"mssql"}, wantOwner: 1, wantFound: true},
		{name: "a target no owner reads", targets: []string{"mysql"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			owner, found, err := routes(c).Owner(test.targets)

			c.Assert(err, qt.IsNil)
			c.Assert(found, qt.Equals, test.wantFound)
			c.Assert(owner, qt.Equals, test.wantOwner)
		})
	}
}

func TestRoutes_Owner_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		targets []string
		wantErr string
	}{
		{name: "an owner's target beside one no owner reads", targets: []string{"postgres", "mysql"},
			wantErr: `.*scoped to postgres,mysql mixes PostgreSQL-family targets with others; .*` +
				`declare one scoped to postgres and another scoped to mysql`},
		{name: "an unlabeled owner's target beside another owner's", targets: []string{"sqlserver", "postgres"},
			wantErr: `.*scoped to sqlserver,postgres mixes one owner's targets with others; .*`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			owner, found, err := routes(c).Owner(test.targets)

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidAttributeValue)
			c.Assert(found, qt.IsFalse)
			c.Assert(owner, qt.Equals, 0)
		})
	}
}

func TestRoutes_Claim_RefusesAClaimAnotherOwnerHolds(t *testing.T) {
	tests := []struct {
		name         string
		scope        targetscope.Scope
		wantConflict targetscope.Conflict
	}{
		{name: "a target", scope: targetscope.Scope{Targets: []string{"postgresql"}}, wantConflict: targetscope.Conflict{Owner: 0, Target: "postgres"}},
		{name: "the unscoped declarations", scope: targetscope.Scope{Unscoped: true}, wantConflict: targetscope.Conflict{Owner: 0}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			claimed := routes(c)

			conflict, taken := claimed.Claim(2, test.scope)
			owner, found, err := claimed.Owner([]string{"postgres"})

			c.Assert(taken, qt.IsTrue)
			c.Assert(conflict, qt.Equals, test.wantConflict)
			c.Assert(err, qt.IsNil)
			c.Assert(found, qt.IsTrue)
			c.Assert(owner, qt.Equals, 0)
		})
	}
}

func TestRoutes_Owner_NilRoutesReadNothing(t *testing.T) {
	c := qt.New(t)
	var none *targetscope.Routes

	owner, found, err := none.Owner(nil)

	c.Assert(err, qt.IsNil)
	c.Assert(found, qt.IsFalse)
	c.Assert(owner, qt.Equals, 0)
}
