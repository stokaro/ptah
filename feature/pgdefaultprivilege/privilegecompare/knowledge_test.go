package privilegecompare_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemaext"
	"ptah.run/feature/pgdefaultprivilege"
	"ptah.run/feature/pgdefaultprivilege/privilegecompare"
)

func TestCompareObjects_StableAndUnmanagedState(t *testing.T) {
	scoped := pgdefaultprivilege.Identity{Grantor: "owner", Schema: "app", ObjectType: "TABLES", Grantee: "reader"}
	global := pgdefaultprivilege.Identity{Grantor: "owner", ObjectType: "FUNCTIONS", Grantee: "PUBLIC"}
	tests := []struct {
		name             string
		identity         pgdefaultprivilege.Identity
		desired, current []schemaext.Value
		roles            []string
	}{
		{name: "built-in public grant", identity: global, desired: []schemaext.Value{&pgdefaultprivilege.Desired{Privileges: []pgdefaultprivilege.Grant{{Privilege: "EXECUTE"}}}}},
		{name: "completed public revoke", identity: global, desired: []schemaext.Value{&pgdefaultprivilege.Desired{Revoked: []string{"EXECUTE"}}},
			current: []schemaext.Value{&pgdefaultprivilege.Observed{Revoked: []string{"EXECUTE"}}}},
		{name: "grantee is not ownership", identity: scoped, roles: []string{"reader"},
			current: []schemaext.Value{&pgdefaultprivilege.Observed{Privileges: []pgdefaultprivilege.Grant{{Privilege: "SELECT"}}}}},
		{name: "grantor spelling is exact", identity: scoped, roles: []string{"Owner"},
			current: []schemaext.Value{&pgdefaultprivilege.Observed{Privileges: []pgdefaultprivilege.Grant{{Privilege: "SELECT"}}}}},
		{name: "unmanaged built-in revoke", identity: global,
			current: []schemaext.Value{&pgdefaultprivilege.Observed{Revoked: []string{"EXECUTE"}}}},
		{name: "completed scoped grant", identity: scoped, desired: []schemaext.Value{&pgdefaultprivilege.Desired{Privileges: []pgdefaultprivilege.Grant{{Privilege: "SELECT"}}}},
			current: []schemaext.Value{&pgdefaultprivilege.Observed{Privileges: []pgdefaultprivilege.Grant{{Privilege: "SELECT"}}}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			request := comparison(c, test.identity, test.desired, test.current, test.roles)
			got, err := (privilegecompare.Service{}).CompareObjects(context.Background(), request)
			c.Assert(err, qt.IsNil)
			c.Assert(got.Complete, qt.IsTrue)
			c.Assert(got.Changes, qt.HasLen, 0)
			c.Assert(got.Undecided, qt.HasLen, 0)
		})
	}
}

func TestCompareObjects_UnreadCatalogIsUndecided(t *testing.T) {
	c := qt.New(t)
	identity := pgdefaultprivilege.Identity{Grantor: "owner", ObjectType: "FUNCTIONS", Grantee: "PUBLIC"}
	request := comparison(c, identity, []schemaext.Value{&pgdefaultprivilege.Desired{Revoked: []string{"EXECUTE"}}}, nil, nil)
	unread, err := pgdefaultprivilege.Coverage(schemaext.Observed, schemaext.Knowledge{State: schemaext.Unrepresentable, Reason: "catalog read refused"}, nil)
	c.Assert(err, qt.IsNil)
	request.Current.Coverage = unread
	got, err := (privilegecompare.Service{}).CompareObjects(context.Background(), request)
	c.Assert(err, qt.IsNil)
	c.Assert(got.Changes, qt.HasLen, 0)
	c.Assert(got.Undecided, qt.HasLen, 1)
	c.Assert(got.Undecided[0].Reason, qt.Equals, "default privileges are not established: catalog read refused")
}

func TestCompareObjects_UnreadSourceDoesNotRevoke(t *testing.T) {
	c := qt.New(t)
	identity := pgdefaultprivilege.Identity{Grantor: "owner", Schema: "app", ObjectType: "TABLES", Grantee: "reader"}
	request := comparison(c, identity, nil, []schemaext.Value{&pgdefaultprivilege.Observed{Privileges: []pgdefaultprivilege.Grant{{Privilege: "SELECT"}}}}, []string{"owner"})
	request.Desired.Coverage = schemaext.Coverage{}
	got, err := (privilegecompare.Service{}).CompareObjects(context.Background(), request)
	c.Assert(err, qt.IsNil)
	c.Assert(got.Changes, qt.HasLen, 0)
	c.Assert(got.Undecided, qt.HasLen, 0)
}

func TestCompareObjects_CancellationReturnsNoPartialResult(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	got, err := (privilegecompare.Service{}).CompareObjects(ctx, schemaext.ObjectComparisonRequest{})
	c.Assert(err, qt.ErrorIs, context.Canceled)
	c.Assert(got.Complete, qt.IsFalse)
	c.Assert(got.Changes, qt.HasLen, 0)
}

func TestCompareObjects_IncompleteDeclarationDoesNotRestoreOrTrim(t *testing.T) {
	c := qt.New(t)
	identity := pgdefaultprivilege.Identity{Grantor: "owner", ObjectType: "FUNCTIONS", Grantee: "PUBLIC"}
	request := comparison(c, identity, []schemaext.Value{&pgdefaultprivilege.Desired{}},
		[]schemaext.Value{&pgdefaultprivilege.Observed{Revoked: []string{"EXECUTE"}}}, []string{"owner"})
	request.Desired.Coverage = schemaext.Coverage{}
	got, err := (privilegecompare.Service{}).CompareObjects(context.Background(), request)
	c.Assert(err, qt.IsNil)
	c.Assert(got.Changes, qt.HasLen, 0)
	c.Assert(got.Undecided, qt.HasLen, 1)
	c.Assert(got.Undecided[0].Reason, qt.Equals, "desired default privileges are not established: this source did not describe the feature kind")
}

func TestCompareObjects_ExplicitCompleteSubjectOverridesUnknownNamespace(t *testing.T) {
	c := qt.New(t)
	identity := pgdefaultprivilege.Identity{Grantor: "owner", ObjectType: "FUNCTIONS", Grantee: "PUBLIC"}
	request := comparison(c, identity, []schemaext.Value{&pgdefaultprivilege.Desired{}},
		[]schemaext.Value{&pgdefaultprivilege.Observed{Revoked: []string{"EXECUTE"}}}, nil)
	ref := request.Desired.Objects.Refs()[0]
	coverage, err := pgdefaultprivilege.Coverage(schemaext.Desired, schemaext.Knowledge{State: schemaext.Unrepresentable, Reason: "partial document"},
		[]schemaext.SubjectCoverage{{Kind: pgdefaultprivilege.DefaultPrivilegeKind, Subject: ref, Knowledge: schemaext.Knowledge{State: schemaext.Complete}}})
	c.Assert(err, qt.IsNil)
	request.Desired.Coverage = coverage
	got, err := (privilegecompare.Service{}).CompareObjects(context.Background(), request)
	c.Assert(err, qt.IsNil)
	c.Assert(got.Undecided, qt.HasLen, 0)
	c.Assert(got.Changes, qt.HasLen, 1)
	c.Assert(got.Changes[0].Value, qt.DeepEquals, &pgdefaultprivilege.Change{Before: &pgdefaultprivilege.Observed{Revoked: []string{"EXECUTE"}}, Granted: []pgdefaultprivilege.Grant{{Privilege: "EXECUTE"}}})
}

func TestCompareObjects_PartialDeclarationDoesNotTrimObservedRights(t *testing.T) {
	c := qt.New(t)
	identity := pgdefaultprivilege.Identity{Grantor: "owner", Schema: "app", ObjectType: "TABLES", Grantee: "reader"}
	request := comparison(c, identity, []schemaext.Value{&pgdefaultprivilege.Desired{Privileges: grants("SELECT")}},
		[]schemaext.Value{&pgdefaultprivilege.Observed{Privileges: grants("SELECT", "INSERT")}}, []string{"owner"})
	request.Desired.Coverage = schemaext.Coverage{}
	got, err := (privilegecompare.Service{}).CompareObjects(t.Context(), request)
	c.Assert(err, qt.IsNil)
	c.Assert(got.Changes, qt.HasLen, 0)
	c.Assert(got.Undecided, qt.HasLen, 1)
	c.Assert(got.Undecided[0].Reason, qt.Equals, "desired default privileges are not established: this source did not describe the feature kind")
}
