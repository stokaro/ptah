package postgres_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/difftypes"
	"ptah.run/core/ptaherr"
	"ptah.run/engine/builtin"
	"ptah.run/internal/planner/dialects/postgres"
)

// A PostgreSQL REVOKE can succeed while leaving another grantor's ACL row
// in place. The plan must name that grantor before it emits any SQL.
func TestPlanner_RefusesForeignGrantorRevoke(t *testing.T) {
	for _, test := range []struct {
		name string
		diff difftypes.SchemaDiff
	}{
		{
			name: "privilege",
			diff: difftypes.SchemaDiff{ConnectedRole: "runner", GrantsRemoved: []difftypes.GrantRef{{
				Role: "reader", Privilege: "SELECT", ObjectType: "TABLE", ObjectName: "public.events", GrantedBy: "owner",
			}}},
		},
		{
			name: "grant option",
			diff: difftypes.SchemaDiff{ConnectedRole: "runner", GrantOptionsRevoked: []difftypes.GrantRef{{
				Role: "reader", Privilege: "SELECT", ObjectType: "TABLE", ObjectName: "public.events", GrantedBy: "owner",
			}}},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			_, err := postgres.New().GenerateMigrationAST(t.Context(), must.Must(builtin.New()), &test.diff)
			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(err, qt.ErrorMatches, `(?s).*grantor "owner" differs from connected role "runner".*`)
		})
	}
}

func TestPlanner_RevokesGrantMadeByConnectedRole(t *testing.T) {
	c := qt.New(t)
	diff := &difftypes.SchemaDiff{ConnectedRole: "runner", GrantsRemoved: []difftypes.GrantRef{{
		Role: "reader", Privilege: "SELECT", ObjectType: "TABLE", ObjectName: "public.events", GrantedBy: "runner",
	}}}

	nodes, err := postgres.New().GenerateMigrationAST(t.Context(), must.Must(builtin.New()), diff)
	c.Assert(err, qt.IsNil)
	c.Assert(nodes, qt.HasLen, 1)
}

func TestPlanner_RevokesOwnerGrantAsSuperuser(t *testing.T) {
	c := qt.New(t)
	diff := &difftypes.SchemaDiff{ConnectedRole: "admin", ConnectedRoleSuperuser: true, GrantsRemoved: []difftypes.GrantRef{{
		Role: "reader", Privilege: "SELECT", ObjectType: "TABLE", ObjectName: "public.events",
		GrantedBy: "owner", ObjectOwner: "owner",
	}}}
	nodes, err := postgres.New().GenerateMigrationAST(t.Context(), must.Must(builtin.New()), diff)
	c.Assert(err, qt.IsNil)
	c.Assert(nodes, qt.HasLen, 1)
}

func TestPlanner_RefusesOtherGrantorEvenAsSuperuser(t *testing.T) {
	c := qt.New(t)
	diff := &difftypes.SchemaDiff{ConnectedRole: "admin", ConnectedRoleSuperuser: true, GrantsRemoved: []difftypes.GrantRef{{
		Role: "reader", Privilege: "SELECT", ObjectType: "TABLE", ObjectName: "public.events",
		GrantedBy: "delegate", ObjectOwner: "owner",
	}}}
	_, err := postgres.New().GenerateMigrationAST(t.Context(), must.Must(builtin.New()), diff)
	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
}
