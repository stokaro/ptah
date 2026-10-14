package privilegecompare_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemaext"
	"ptah.run/feature/pgdefaultprivilege"
	"ptah.run/feature/pgdefaultprivilege/privilegecompare"
)

func sequenceGrants(selectOption, othersOption bool) []pgdefaultprivilege.Grant {
	return []pgdefaultprivilege.Grant{{Privilege: "SELECT", WithOption: selectOption}, {Privilege: "UPDATE", WithOption: othersOption}, {Privilege: "USAGE", WithOption: othersOption}}
}

func TestCompareObjects_AllPreservesSpecificGrantOptions(t *testing.T) {
	tests := []struct {
		name             string
		desired, current []pgdefaultprivilege.Grant
	}{
		{name: "specific option beside plain all", desired: []pgdefaultprivilege.Grant{{Privilege: "ALL"}, {Privilege: "SELECT", WithOption: true}}, current: sequenceGrants(true, false)},
		{name: "all option includes plain specific", desired: []pgdefaultprivilege.Grant{{Privilege: "ALL", WithOption: true}, {Privilege: "SELECT"}}, current: sequenceGrants(true, true)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			request := comparison(c, pgdefaultprivilege.Identity{Grantor: "owner", Schema: "app", ObjectType: "SEQUENCES", Grantee: "reader"},
				[]schemaext.Value{&pgdefaultprivilege.Desired{Privileges: test.desired}}, []schemaext.Value{&pgdefaultprivilege.Observed{Privileges: test.current}}, nil)
			got, err := (privilegecompare.Service{}).CompareObjects(t.Context(), request)
			c.Assert(err, qt.IsNil)
			c.Assert(got.Complete, qt.IsTrue)
			c.Assert(got.Undecided, qt.HasLen, 0)
			c.Assert(got.Changes, qt.HasLen, 0)
		})
	}
}

func TestCompareObjects_AllRemovesOnlyUndeclaredOptions(t *testing.T) {
	c := qt.New(t)
	request := comparison(c, pgdefaultprivilege.Identity{Grantor: "owner", Schema: "app", ObjectType: "SEQUENCES", Grantee: "reader"},
		[]schemaext.Value{&pgdefaultprivilege.Desired{Privileges: []pgdefaultprivilege.Grant{{Privilege: "ALL"}, {Privilege: "SELECT", WithOption: true}}}},
		[]schemaext.Value{&pgdefaultprivilege.Observed{Privileges: sequenceGrants(true, true)}}, nil)
	got, err := (privilegecompare.Service{}).CompareObjects(t.Context(), request)
	c.Assert(err, qt.IsNil)
	c.Assert(got.Changes, qt.HasLen, 1)
	change := got.Changes[0].Value.(*pgdefaultprivilege.Change)
	c.Assert(change.Granted, qt.HasLen, 0)
	c.Assert(change.Revoked, qt.HasLen, 0)
	c.Assert(change.GrantOptionsAdded, qt.HasLen, 0)
	c.Assert(change.GrantOptionsRevoked, qt.DeepEquals, []string{"UPDATE", "USAGE"})
}

// MAINTAIN is outside the portable ALL minimum. Once catalog inspection names
// it, its grant option is part of the desired comparison on that server.
func TestCompareObjects_AllIncludesObservedReleaseSpecificOptions(t *testing.T) {
	tests := []struct {
		name     string
		identity pgdefaultprivilege.Identity
		grants   []pgdefaultprivilege.Grant
	}{
		{name: "scoped", identity: pgdefaultprivilege.Identity{Grantor: "owner", Schema: "app", ObjectType: "TABLES", Grantee: "reader"}, grants: []pgdefaultprivilege.Grant{
			{Privilege: "SELECT"}, {Privilege: "INSERT"}, {Privilege: "UPDATE"}, {Privilege: "DELETE"}, {Privilege: "TRUNCATE"}, {Privilege: "REFERENCES"}, {Privilege: "TRIGGER"}, {Privilege: "MAINTAIN", WithOption: true},
		}},
		{name: "global owner", identity: pgdefaultprivilege.Identity{Grantor: "owner", ObjectType: "TABLES", Grantee: "owner"}, grants: []pgdefaultprivilege.Grant{{Privilege: "MAINTAIN", WithOption: true}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			request := comparison(c, test.identity,
				[]schemaext.Value{&pgdefaultprivilege.Desired{Privileges: []pgdefaultprivilege.Grant{{Privilege: "ALL"}}}},
				[]schemaext.Value{&pgdefaultprivilege.Observed{Privileges: test.grants}}, nil)
			got, err := (privilegecompare.Service{}).CompareObjects(t.Context(), request)
			c.Assert(err, qt.IsNil)
			c.Assert(got.Changes, qt.HasLen, 1)
			change := got.Changes[0].Value.(*pgdefaultprivilege.Change)
			c.Assert(change.Granted, qt.HasLen, 0)
			c.Assert(change.Revoked, qt.HasLen, 0)
			c.Assert(change.GrantOptionsAdded, qt.HasLen, 0)
			c.Assert(change.GrantOptionsRevoked, qt.DeepEquals, []string{"MAINTAIN"})
		})
	}
}

func TestCompareObjects_AllAddsMissingRightsWithoutRemovingDeclaredOption(t *testing.T) {
	c := qt.New(t)
	request := comparison(c, pgdefaultprivilege.Identity{Grantor: "owner", Schema: "app", ObjectType: "SEQUENCES", Grantee: "reader"},
		[]schemaext.Value{&pgdefaultprivilege.Desired{Privileges: []pgdefaultprivilege.Grant{{Privilege: "ALL"}, {Privilege: "SELECT", WithOption: true}}}},
		[]schemaext.Value{&pgdefaultprivilege.Observed{Privileges: []pgdefaultprivilege.Grant{{Privilege: "SELECT", WithOption: true}}}}, nil)
	got, err := (privilegecompare.Service{}).CompareObjects(t.Context(), request)
	c.Assert(err, qt.IsNil)
	c.Assert(got.Changes, qt.HasLen, 1)
	change := got.Changes[0].Value.(*pgdefaultprivilege.Change)
	c.Assert(change.Granted, qt.DeepEquals, []pgdefaultprivilege.Grant{{Privilege: "ALL"}})
	c.Assert(change.Revoked, qt.HasLen, 0)
	c.Assert(change.GrantOptionsAdded, qt.HasLen, 0)
	c.Assert(change.GrantOptionsRevoked, qt.HasLen, 0)
}
