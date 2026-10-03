package devclean_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/internal/devclean"
)

// keptState is a Supabase-shaped starting point as a read describes it.
func keptState() *catalog.Database {
	return &catalog.Database{
		Schemas:   []catalog.Schema{{Name: "auth"}},
		Tables:    []catalog.Table{{Schema: "auth", Name: "users"}, {Schema: "public", Name: "seed"}},
		Functions: []catalog.Function{{Schema: "auth", Name: "uid"}},
		Grants: []catalog.Grant{
			{Role: "reader", Privilege: "SELECT", ObjectType: "TABLE", Schema: "auth", ObjectName: "users"},
		},
		DefaultPrivileges: []catalog.DefaultPrivilege{
			{Grantor: "postgres", Schema: "auth", ObjectType: "TABLES", Grantee: "reader", Privilege: "SELECT"},
		},
		Roles: []catalog.Role{{Name: "anon"}},
	}
}

// TestWithoutKeptStateLeavesWhatTheRunAdded pins the comparison side of a
// docker block's starting point (stokaro/ptah#4056). The starting point's
// objects leave the read, and what the run added stays wherever it is: a
// trigger on a starting-point table, a grant beside a starting-point grant, a
// table in public. An object the other side declares stays too, and an object
// read without a schema is matched in the default one.
func TestWithoutKeptStateLeavesWhatTheRunAdded(t *testing.T) {
	c := qt.New(t)
	current := keptState()
	current.Tables = []catalog.Table{
		{Schema: "auth", Name: "users"}, {Name: "seed"}, {Name: "profiles"},
	}
	current.Triggers = []catalog.Trigger{{Schema: "auth", Table: "users", Name: "on_signup"}}
	current.Grants = append(current.Grants,
		catalog.Grant{Role: "reader", Privilege: "INSERT", ObjectType: "TABLE", Schema: "auth", ObjectName: "users"})
	current.Roles = append(current.Roles, catalog.Role{Name: "app"})
	declared := &catalog.Database{Functions: []catalog.Function{{Schema: "auth", Name: "uid"}}}

	got := devclean.WithoutKeptState(current, keptState(), declared, "public")

	c.Assert(got.Schemas, qt.HasLen, 0)
	c.Assert(got.Tables, qt.DeepEquals, []catalog.Table{{Name: "profiles"}})
	c.Assert(got.Functions, qt.DeepEquals, []catalog.Function{{Schema: "auth", Name: "uid"}})
	c.Assert(got.Triggers, qt.DeepEquals, []catalog.Trigger{{Schema: "auth", Table: "users", Name: "on_signup"}})
	c.Assert(got.Grants, qt.DeepEquals, []catalog.Grant{
		{Role: "reader", Privilege: "INSERT", ObjectType: "TABLE", Schema: "auth", ObjectName: "users"},
	})
	c.Assert(got.DefaultPrivileges, qt.HasLen, 0)
	c.Assert(got.Roles, qt.DeepEquals, []catalog.Role{{Name: "app"}})
	c.Assert(current.Tables, qt.HasLen, 3)
}

// TestWithoutKeptStateWithoutAStartingPoint pins that a dev database with no
// starting point, which is every one a docker block did not provision, is
// compared as it was read.
func TestWithoutKeptStateWithoutAStartingPoint(t *testing.T) {
	c := qt.New(t)
	current := keptState()

	got := devclean.WithoutKeptState(current, nil, nil, "public")

	c.Assert(got, qt.Equals, current)
}

// TestBaselineWithoutStartingPointOfAClaimedURL pins that a baseline a claim
// did not record a starting point for, the zero one here, leaves the read as
// it is.
func TestBaselineWithoutStartingPointOfAClaimedURL(t *testing.T) {
	c := qt.New(t)
	current := keptState()

	got := devclean.Baseline{}.WithoutStartingPoint(current, nil, "public")

	c.Assert(got, qt.Equals, current)
}
