package privilegecompare_test

import (
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/google/go-cmp/cmp/cmpopts"

	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
	"ptah.run/feature/pgdefaultprivilege"
	"ptah.run/feature/pgdefaultprivilege/privilegecompare"
)

type declaredACL struct {
	identity pgdefaultprivilege.Identity
	value    pgdefaultprivilege.Desired
}

type comparedACL struct {
	Identity pgdefaultprivilege.Identity
	Value    *pgdefaultprivilege.Change
}

func declaredObjects(c *qt.C, request schemaext.ObjectComparisonRequest, declarations []declaredACL) schemaext.Objects {
	var objects schemaext.Objects
	for _, declaration := range declarations {
		ref, err := pgdefaultprivilege.Ref(declaration.identity, request.Identifiers)
		c.Assert(err, qt.IsNil)
		object, err := pgdefaultprivilege.DesiredObject(ref, declaration.value)
		c.Assert(err, qt.IsNil)
		objects, err = objects.Declare(object)
		c.Assert(err, qt.IsNil)
	}
	return objects
}

func describedChanges(c *qt.C, changes []schemaext.ChangeRecord) []comparedACL {
	var result []comparedACL
	for _, change := range changes {
		identity, err := pgdefaultprivilege.IdentityOf(change.Subject)
		c.Assert(err, qt.IsNil)
		value, ok := change.Value.(*pgdefaultprivilege.Change)
		c.Assert(ok, qt.IsTrue)
		delta := value.Copy()
		delta.Before = nil
		result = append(result, comparedACL{Identity: identity, Value: delta})
	}
	return result
}

func TestCompareObjects_GrantorIdentityAndDeterministicOrder(t *testing.T) {
	alpha := pgdefaultprivilege.Identity{Grantor: "alpha_owner", Schema: "app", ObjectType: "TABLES", Grantee: "reader"}
	beta := pgdefaultprivilege.Identity{Grantor: "beta_owner", Schema: "app", ObjectType: "TABLES", Grantee: "reader"}
	sequence := pgdefaultprivilege.Identity{Grantor: "alpha_owner", Schema: "app", ObjectType: "SEQUENCES", Grantee: "writer"}
	reporting := pgdefaultprivilege.Identity{Grantor: "alpha_owner", Schema: "reporting", ObjectType: "TABLES", Grantee: "PUBLIC"}
	declarations := []declaredACL{{beta, pgdefaultprivilege.Desired{Privileges: grants("SELECT")}},
		{sequence, pgdefaultprivilege.Desired{Privileges: grants("USAGE", "SELECT")}},
		{alpha, pgdefaultprivilege.Desired{Privileges: grants("SELECT")}},
		{reporting, pgdefaultprivilege.Desired{Privileges: grants("SELECT")}}}
	reversed := slices.Clone(declarations)
	slices.Reverse(reversed)
	tests := []struct {
		name         string
		declarations []declaredACL
	}{
		{name: "declared order", declarations: declarations}, {name: "reverse order", declarations: reversed},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			request := comparison(c, alpha, nil, nil, nil)
			request.Desired.Objects = declaredObjects(c, request, test.declarations)
			got, err := (privilegecompare.Service{}).CompareObjects(t.Context(), request)
			c.Assert(err, qt.IsNil)
			c.Assert(got.Undecided, qt.HasLen, 0)
			c.Assert(describedChanges(c, got.Changes), qt.DeepEquals, []comparedACL{
				{sequence, &pgdefaultprivilege.Change{Granted: grants("SELECT", "USAGE")}},
				{alpha, &pgdefaultprivilege.Change{Granted: grants("SELECT")}},
				{beta, &pgdefaultprivilege.Change{Granted: grants("SELECT")}},
				{reporting, &pgdefaultprivilege.Change{Granted: grants("SELECT")}},
			})
		})
	}
}

func TestCompareObjects_GranteeSpellingIsExact(t *testing.T) {
	c := qt.New(t)
	lower := pgdefaultprivilege.Identity{Grantor: "owner", Schema: "app", ObjectType: "TABLES", Grantee: "reader"}
	upper := pgdefaultprivilege.Identity{Grantor: "owner", Schema: "app", ObjectType: "TABLES", Grantee: "Reader"}
	request := comparison(c, lower, nil, []schemaext.Value{&pgdefaultprivilege.Observed{Privileges: grants("SELECT")}}, []string{"owner"})
	request.Desired.Objects = declaredObjects(c, request, []declaredACL{{upper, pgdefaultprivilege.Desired{Privileges: grants("SELECT")}}})
	got, err := (privilegecompare.Service{}).CompareObjects(t.Context(), request)
	c.Assert(err, qt.IsNil)
	c.Assert(got.Undecided, qt.HasLen, 0)
	c.Assert(describedChanges(c, got.Changes), qt.DeepEquals, []comparedACL{
		{upper, &pgdefaultprivilege.Change{Granted: grants("SELECT")}},
		{lower, &pgdefaultprivilege.Change{Revoked: grants("SELECT")}},
	})
}

func TestCompareObjects_ComposedGrantOptionWins(t *testing.T) {
	tests := []struct {
		name    string
		options []bool
	}{
		{name: "grantable first", options: []bool{true, false}},
		{name: "grantable last", options: []bool{false, true}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			identity := pgdefaultprivilege.Identity{Grantor: "owner", Schema: "app", ObjectType: "TABLES", Grantee: "reader"}
			request := comparison(c, identity, nil, []schemaext.Value{&pgdefaultprivilege.Observed{Privileges: grants("SELECT")}}, nil)
			var declarations []declaredACL
			for _, option := range test.options {
				declarations = append(declarations, declaredACL{identity, pgdefaultprivilege.Desired{Privileges: []pgdefaultprivilege.Grant{{Privilege: "SELECT", WithOption: option}}}})
			}
			request.Desired.Objects = declaredObjects(c, request, declarations)
			got, err := (privilegecompare.Service{}).CompareObjects(t.Context(), request)
			c.Assert(err, qt.IsNil)
			c.Assert(got.Undecided, qt.HasLen, 0)
			c.Assert(describedChanges(c, got.Changes), qt.DeepEquals, []comparedACL{{identity, &pgdefaultprivilege.Change{GrantOptionsAdded: []string{"SELECT"}}}})
		})
	}
}

func TestCompareObjects_PublicGranteeDoesNotNeedManagedRole(t *testing.T) {
	c := qt.New(t)
	identity := pgdefaultprivilege.Identity{Grantor: "owner", Schema: "app", ObjectType: "TABLES", Grantee: "PUBLIC"}
	request := comparison(c, identity, nil, []schemaext.Value{&pgdefaultprivilege.Observed{Privileges: grants("SELECT")}}, []string{"owner"})
	got, err := (privilegecompare.Service{}).CompareObjects(t.Context(), request)
	c.Assert(err, qt.IsNil)
	c.Assert(got.Undecided, qt.HasLen, 0)
	c.Assert(describedChanges(c, got.Changes), qt.DeepEquals, []comparedACL{{identity, &pgdefaultprivilege.Change{Revoked: grants("SELECT")}}})
}

// An unread owner's subset does not hide the inspected ACL for another grantee.
func TestCompareObjects_UnreadOwnerLeavesOtherGranteesComparable(t *testing.T) {
	owner := pgdefaultprivilege.Identity{Grantor: "owner", ObjectType: "TABLES", Grantee: "owner"}
	reader := pgdefaultprivilege.Identity{Grantor: "owner", ObjectType: "TABLES", Grantee: "reader"}
	tests := []struct {
		name          string
		desired       []declaredACL
		roles         []string
		wantOwner     []*pgdefaultprivilege.Change
		wantUndecided []string
	}{
		{name: "partial revoke", desired: []declaredACL{{owner, pgdefaultprivilege.Desired{Revoked: []string{"DELETE"}}}}, wantUndecided: []string{"default privileges are not established: owner subset unavailable"}},
		{name: "partial grant", desired: []declaredACL{{owner, pgdefaultprivilege.Desired{Privileges: grants("SELECT")}}}, wantUndecided: []string{"default privileges are not established: owner subset unavailable"}},
		{name: "all revoke", desired: []declaredACL{{owner, pgdefaultprivilege.Desired{Revoked: []string{"ALL"}}}}, wantOwner: []*pgdefaultprivilege.Change{{UnknownBefore: true, Revoked: grants("ALL")}}},
		{name: "all grant", desired: []declaredACL{{owner, pgdefaultprivilege.Desired{Privileges: grants("ALL")}}}, wantOwner: []*pgdefaultprivilege.Change{{UnknownBefore: true, Granted: grants("ALL")}}},
		{name: "managed grantor", roles: []string{"owner"}, wantOwner: []*pgdefaultprivilege.Change{{UnknownBefore: true, Granted: grants("ALL")}}},
		{name: "unmanaged grantor"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			request := comparison(c, owner, nil, nil, test.roles)
			request.Target = "cockroachdb"
			ref, err := pgdefaultprivilege.Ref(owner, request.Identifiers)
			c.Assert(err, qt.IsNil)
			request.Current.Coverage, err = pgdefaultprivilege.Coverage(schemaext.Observed, schemaext.Knowledge{State: schemaext.Complete}, []schemaext.SubjectCoverage{
				{Kind: pgdefaultprivilege.DefaultPrivilegeKind, Subject: ref, Knowledge: schemaext.Knowledge{State: schemaext.Unrepresentable, Reason: "owner subset unavailable"}},
			})
			c.Assert(err, qt.IsNil)
			request.Desired.Objects = declaredObjects(c, request, append(slices.Clone(test.desired), declaredACL{reader, pgdefaultprivilege.Desired{Privileges: grants("SELECT")}}))
			got, err := (privilegecompare.Service{}).CompareObjects(t.Context(), request)
			c.Assert(err, qt.IsNil)
			var want []comparedACL
			for _, change := range test.wantOwner {
				want = append(want, comparedACL{owner, change})
			}
			want = append(want, comparedACL{reader, &pgdefaultprivilege.Change{Granted: grants("SELECT")}})
			c.Assert(describedChanges(c, got.Changes), qt.DeepEquals, want)
			var reasons []string
			var subjects []objectidentity.ID
			for _, undecided := range got.Undecided {
				reasons = append(reasons, undecided.Reason)
				subjects = append(subjects, undecided.Subject)
			}
			c.Assert(reasons, qt.CmpEquals(cmpopts.EquateEmpty()), test.wantUndecided)
			c.Assert(subjects, qt.All(qt.Equals), ref)
		})
	}
}
