package privilegecompare_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/feature/pgdefaultprivilege"
	"ptah.run/feature/pgdefaultprivilege/privilegecompare"
)

func TestCompareObjects_PrivilegeSemantics(t *testing.T) {
	scoped := pgdefaultprivilege.Identity{Grantor: "owner", Schema: "app", ObjectType: "TABLES", Grantee: "reader"}
	global := pgdefaultprivilege.Identity{Grantor: "owner", ObjectType: "FUNCTIONS", Grantee: "PUBLIC"}
	tests := []struct {
		name             string
		identity         pgdefaultprivilege.Identity
		desired, current []schemaext.Value
		roles            []string
		want             *pgdefaultprivilege.Change
	}{
		{name: "empty identity trims grants", identity: scoped, desired: []schemaext.Value{&pgdefaultprivilege.Desired{}},
			current: []schemaext.Value{&pgdefaultprivilege.Observed{Privileges: []pgdefaultprivilege.Grant{{Privilege: "SELECT", WithOption: true}}}},
			want:    &pgdefaultprivilege.Change{Revoked: []pgdefaultprivilege.Grant{{Privilege: "SELECT", WithOption: true}}}},
		{name: "empty identity restores built-in", identity: global, desired: []schemaext.Value{&pgdefaultprivilege.Desired{}},
			current: []schemaext.Value{&pgdefaultprivilege.Observed{Revoked: []string{"EXECUTE"}}},
			want:    &pgdefaultprivilege.Change{Granted: []pgdefaultprivilege.Grant{{Privilege: "EXECUTE"}}}},
		{name: "new scoped grant", identity: scoped, desired: []schemaext.Value{&pgdefaultprivilege.Desired{Privileges: []pgdefaultprivilege.Grant{{Privilege: "SELECT"}}}},
			want: &pgdefaultprivilege.Change{Granted: []pgdefaultprivilege.Grant{{Privilege: "SELECT"}}}},
		{name: "built-in public revoke", identity: global, desired: []schemaext.Value{&pgdefaultprivilege.Desired{Revoked: []string{"EXECUTE"}}},
			want: &pgdefaultprivilege.Change{Revoked: []pgdefaultprivilege.Grant{{Privilege: "EXECUTE"}}}},
		{name: "restore built-in for managed grantor", identity: global, roles: []string{"owner"}, current: []schemaext.Value{&pgdefaultprivilege.Observed{Revoked: []string{"EXECUTE"}}},
			want: &pgdefaultprivilege.Change{Granted: []pgdefaultprivilege.Grant{{Privilege: "EXECUTE"}}}},
		{name: "trim managed grantor", identity: scoped, roles: []string{"owner"}, current: []schemaext.Value{&pgdefaultprivilege.Observed{Privileges: []pgdefaultprivilege.Grant{{Privilege: "SELECT", WithOption: true}}}},
			want: &pgdefaultprivilege.Change{Revoked: []pgdefaultprivilege.Grant{{Privilege: "SELECT", WithOption: true}}}},
		{name: "add grant option", identity: scoped, desired: []schemaext.Value{&pgdefaultprivilege.Desired{Privileges: []pgdefaultprivilege.Grant{{Privilege: "SELECT", WithOption: true}}}},
			current: []schemaext.Value{&pgdefaultprivilege.Observed{Privileges: []pgdefaultprivilege.Grant{{Privilege: "SELECT"}}}},
			want:    &pgdefaultprivilege.Change{GrantOptionsAdded: []string{"SELECT"}}},
		{name: "remove grant option", identity: scoped, desired: []schemaext.Value{&pgdefaultprivilege.Desired{Privileges: []pgdefaultprivilege.Grant{{Privilege: "SELECT"}}}},
			current: []schemaext.Value{&pgdefaultprivilege.Observed{Privileges: []pgdefaultprivilege.Grant{{Privilege: "SELECT", WithOption: true}}}},
			want:    &pgdefaultprivilege.Change{GrantOptionsRevoked: []string{"SELECT"}}},
		{name: "trim explicit identity", identity: scoped, desired: []schemaext.Value{&pgdefaultprivilege.Desired{Privileges: []pgdefaultprivilege.Grant{{Privilege: "SELECT"}}}},
			current: []schemaext.Value{&pgdefaultprivilege.Observed{Privileges: []pgdefaultprivilege.Grant{{Privilege: "SELECT"}, {Privilege: "UPDATE"}}}},
			want:    &pgdefaultprivilege.Change{Revoked: []pgdefaultprivilege.Grant{{Privilege: "UPDATE"}}}},
		{name: "explicit revoke without managed grantor", identity: scoped, desired: []schemaext.Value{&pgdefaultprivilege.Desired{Revoked: []string{"SELECT"}}},
			current: []schemaext.Value{&pgdefaultprivilege.Observed{Privileges: []pgdefaultprivilege.Grant{{Privilege: "SELECT"}}}},
			want:    &pgdefaultprivilege.Change{Revoked: []pgdefaultprivilege.Grant{{Privilege: "SELECT"}}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			request := comparison(c, test.identity, test.desired, test.current, test.roles)
			got, err := (privilegecompare.Service{}).CompareObjects(context.Background(), request)
			c.Assert(err, qt.IsNil)
			c.Assert(got.Complete, qt.IsTrue)
			c.Assert(got.Undecided, qt.HasLen, 0)
			c.Assert(got.Changes, qt.HasLen, 1)
			actual := got.Changes[0].Value.(*pgdefaultprivilege.Change)
			c.Assert(actual.Before, qt.IsNotNil)
			actual.Before = nil
			c.Assert(actual, qt.DeepEquals, test.want)
			c.Assert(test.want.Validate(), qt.IsNil)
		})
	}
}

func comparison(c *qt.C, identity pgdefaultprivilege.Identity, desired, observed []schemaext.Value, roles []string) schemaext.ObjectComparisonRequest {
	semantics := identifier.ForDialect("postgres")
	ref, err := pgdefaultprivilege.Ref(identity, semantics)
	c.Assert(err, qt.IsNil)
	desiredCoverage, err := pgdefaultprivilege.CompleteCoverage(schemaext.Desired)
	c.Assert(err, qt.IsNil)
	observedCoverage, err := pgdefaultprivilege.CompleteCoverage(schemaext.Observed)
	c.Assert(err, qt.IsNil)
	request := schemaext.ObjectComparisonRequest{Target: "postgres", Identifiers: semantics, Kinds: []schemaext.Kind{pgdefaultprivilege.DefaultPrivilegeKind},
		Desired: schemaext.ObjectState{Objects: objects(c, ref, desired), Coverage: desiredCoverage},
		Current: schemaext.ObjectState{Objects: objects(c, ref, observed), Coverage: observedCoverage}}
	for _, role := range roles {
		request.DeclaredRoles = append(request.DeclaredRoles, objectidentity.NewBuilder(semantics).Role(role))
	}
	return request
}

func objects(c *qt.C, ref objectidentity.ID, values []schemaext.Value) schemaext.Objects {
	var entries []schemaext.Object
	for _, value := range values {
		entries = append(entries, schemaext.Object{Ref: ref, Value: value})
	}
	result, err := schemaext.NewObjects(entries...)
	c.Assert(err, qt.IsNil)
	return result
}
