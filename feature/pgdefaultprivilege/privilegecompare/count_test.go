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

// TestCompareObjects_CountsTheIdentitiesItCompares pins the counts the
// inventory reports for default privileges, one per identity: an identity both
// sides hold is matched whether or not it differs, and so is a declaration the
// built-in privileges already satisfy. A declaration the plan writes an entry
// for is added, an entry it takes back is removed, and an entry it keeps for
// want of authority is counted on neither side.
func TestCompareObjects_CountsTheIdentitiesItCompares(t *testing.T) {
	scoped := pgdefaultprivilege.Identity{Grantor: "owner", Schema: "app", ObjectType: "TABLES", Grantee: "reader"}
	global := pgdefaultprivilege.Identity{Grantor: "owner", ObjectType: "FUNCTIONS", Grantee: "PUBLIC"}
	selecting := []schemaext.Value{&pgdefaultprivilege.Desired{Privileges: []pgdefaultprivilege.Grant{{Privilege: "SELECT"}}}}
	tests := []struct {
		name             string
		identity         pgdefaultprivilege.Identity
		desired, current []schemaext.Value
		roles            []string
		want             schemaext.ObjectCount
	}{
		{name: "an identity that agrees", identity: scoped, desired: selecting,
			current: []schemaext.Value{&pgdefaultprivilege.Observed{Privileges: []pgdefaultprivilege.Grant{{Privilege: "SELECT"}}}},
			want:    schemaext.ObjectCount{Kind: pgdefaultprivilege.ChangeKind, Matched: 1}},
		{name: "an identity that differs", identity: scoped, desired: selecting,
			current: []schemaext.Value{&pgdefaultprivilege.Observed{Privileges: []pgdefaultprivilege.Grant{{Privilege: "SELECT", WithOption: true}}}},
			want:    schemaext.ObjectCount{Kind: pgdefaultprivilege.ChangeKind, Matched: 1}},
		{name: "a declaration the built-in privileges satisfy", identity: global, desired: []schemaext.Value{&pgdefaultprivilege.Desired{}},
			want: schemaext.ObjectCount{Kind: pgdefaultprivilege.ChangeKind, Matched: 1}},
		{name: "a declaration the plan writes", identity: scoped, desired: selecting,
			want: schemaext.ObjectCount{Kind: pgdefaultprivilege.ChangeKind, Added: 1}},
		{name: "an entry the plan takes back", identity: scoped, roles: []string{"owner"},
			current: []schemaext.Value{&pgdefaultprivilege.Observed{Privileges: []pgdefaultprivilege.Grant{{Privilege: "SELECT"}}}},
			want:    schemaext.ObjectCount{Kind: pgdefaultprivilege.ChangeKind, Removed: 1}},
		{name: "an entry kept for want of authority", identity: scoped,
			current: []schemaext.Value{&pgdefaultprivilege.Observed{Privileges: []pgdefaultprivilege.Grant{{Privilege: "SELECT"}}}},
			want:    schemaext.ObjectCount{Kind: pgdefaultprivilege.ChangeKind}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got, err := (privilegecompare.Service{}).CompareObjects(context.Background(), comparison(c, test.identity, test.desired, test.current, test.roles))

			c.Assert(err, qt.IsNil)
			c.Assert(got.Counts, qt.DeepEquals, []schemaext.ObjectCount{test.want})
		})
	}
}

// TestCompareObjects_CountsNothingWhereItChangesAnUnreadIdentity pins that a
// comparison which changes an identity whose catalog entry it could not read
// counts no identity at all: it cannot say which side holds the one it
// changes, so any count would describe part of the kind as all of it.
func TestCompareObjects_CountsNothingWhereItChangesAnUnreadIdentity(t *testing.T) {
	c := qt.New(t)
	identity := pgdefaultprivilege.Identity{Grantor: "owner", ObjectType: "TABLES", Grantee: "owner"}
	request := comparison(c, identity, nil, nil, []string{"owner"})
	request.Target = platform.CockroachDB
	ref, err := pgdefaultprivilege.Ref(identity, request.Identifiers)
	c.Assert(err, qt.IsNil)
	request.Current.Coverage, err = pgdefaultprivilege.Coverage(schemaext.Observed, schemaext.Knowledge{State: schemaext.Complete},
		[]schemaext.SubjectCoverage{{Kind: pgdefaultprivilege.DefaultPrivilegeKind, Subject: ref, Knowledge: schemaext.Knowledge{State: schemaext.Unrepresentable, Reason: "owner subset unavailable"}}})
	c.Assert(err, qt.IsNil)

	got, err := (privilegecompare.Service{}).CompareObjects(context.Background(), request)

	c.Assert(err, qt.IsNil)
	c.Assert(got.Changes, qt.HasLen, 1)
	c.Assert(got.Counts, qt.IsNil)
}
