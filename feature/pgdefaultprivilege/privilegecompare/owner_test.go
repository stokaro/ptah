package privilegecompare_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/core/schemaext"
	"ptah.run/feature/pgdefaultprivilege"
	"ptah.run/feature/pgdefaultprivilege/privilegecompare"
)

func TestCompareObjects_UnreadOwnerConvergesOnlyWithAll(t *testing.T) {
	tests := []struct {
		name    string
		desired []schemaext.Value
		roles   []string
		want    *pgdefaultprivilege.Change
	}{
		{name: "managed grantor restores all", roles: []string{"owner"}, want: &pgdefaultprivilege.Change{UnknownBefore: true, Granted: []pgdefaultprivilege.Grant{{Privilege: "ALL"}}}},
		{name: "explicit empty identity restores all", desired: []schemaext.Value{&pgdefaultprivilege.Desired{}}, want: &pgdefaultprivilege.Change{UnknownBefore: true, Granted: []pgdefaultprivilege.Grant{{Privilege: "ALL"}}}},
		{name: "grant all", desired: []schemaext.Value{&pgdefaultprivilege.Desired{Privileges: []pgdefaultprivilege.Grant{{Privilege: "ALL", WithOption: true}}}}, want: &pgdefaultprivilege.Change{UnknownBefore: true, Granted: []pgdefaultprivilege.Grant{{Privilege: "ALL", WithOption: true}}}},
		{name: "revoke all", desired: []schemaext.Value{&pgdefaultprivilege.Desired{Revoked: []string{"ALL"}}}, want: &pgdefaultprivilege.Change{UnknownBefore: true, Revoked: []pgdefaultprivilege.Grant{{Privilege: "ALL"}}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			identity := pgdefaultprivilege.Identity{Grantor: "owner", ObjectType: "TABLES", Grantee: "owner"}
			request := comparison(c, identity, test.desired, nil, test.roles)
			request.Target = platform.CockroachDB
			ref, err := pgdefaultprivilege.Ref(identity, request.Identifiers)
			c.Assert(err, qt.IsNil)
			request.Current.Coverage, err = pgdefaultprivilege.Coverage(schemaext.Observed, schemaext.Knowledge{State: schemaext.Complete},
				[]schemaext.SubjectCoverage{{Kind: pgdefaultprivilege.DefaultPrivilegeKind, Subject: ref, Knowledge: schemaext.Knowledge{State: schemaext.Unrepresentable, Reason: "owner subset unavailable"}}})
			c.Assert(err, qt.IsNil)
			got, err := (privilegecompare.Service{}).CompareObjects(context.Background(), request)
			c.Assert(err, qt.IsNil)
			c.Assert(got.Undecided, qt.HasLen, 0)
			c.Assert(got.Changes, qt.HasLen, 1)
			c.Assert(got.Changes[0].Subject, qt.Equals, ref)
			c.Assert(got.Changes[0].Value, qt.DeepEquals, test.want)
			c.Assert(test.want.Validate(), qt.IsNil)
		})
	}
}

func TestCompareObjects_UnreadOwnerDoesNotGuessSubset(t *testing.T) {
	c := qt.New(t)
	identity := pgdefaultprivilege.Identity{Grantor: "owner", ObjectType: "TABLES", Grantee: "owner"}
	request := comparison(c, identity, []schemaext.Value{&pgdefaultprivilege.Desired{Revoked: []string{"SELECT"}}}, nil, []string{"owner"})
	request.Target = platform.CockroachDB
	ref := request.Desired.Objects.Refs()[0]
	var err error
	request.Current.Coverage, err = pgdefaultprivilege.Coverage(schemaext.Observed, schemaext.Knowledge{State: schemaext.Complete},
		[]schemaext.SubjectCoverage{{Kind: pgdefaultprivilege.DefaultPrivilegeKind, Subject: ref, Knowledge: schemaext.Knowledge{State: schemaext.Unrepresentable, Reason: "owner subset unavailable"}}})
	c.Assert(err, qt.IsNil)
	got, err := (privilegecompare.Service{}).CompareObjects(context.Background(), request)
	c.Assert(err, qt.IsNil)
	c.Assert(got.Changes, qt.HasLen, 0)
	c.Assert(got.Undecided, qt.HasLen, 1)
}

func TestCompareObjects_UnreadCatalogDoesNotConvergeOwner(t *testing.T) {
	c := qt.New(t)
	identity := pgdefaultprivilege.Identity{Grantor: "owner", ObjectType: "TABLES", Grantee: "owner"}
	request := comparison(c, identity, []schemaext.Value{&pgdefaultprivilege.Desired{Privileges: []pgdefaultprivilege.Grant{{Privilege: "ALL"}}}}, nil, []string{"owner"})
	request.Target = platform.CockroachDB
	var err error
	request.Current.Coverage, err = pgdefaultprivilege.Coverage(schemaext.Observed, schemaext.Knowledge{State: schemaext.Unrepresentable, Reason: "catalog unread"}, nil)
	c.Assert(err, qt.IsNil)
	got, err := (privilegecompare.Service{}).CompareObjects(context.Background(), request)
	c.Assert(err, qt.IsNil)
	c.Assert(got.Changes, qt.HasLen, 0)
	c.Assert(got.Undecided, qt.HasLen, 1)
}
