package privilegecompare_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemaext"
	"ptah.run/feature/pgdefaultprivilege"
	"ptah.run/feature/pgdefaultprivilege/privilegecompare"
)

func TestCompareObjects_ClassLimitsKeepNamedACLsIndependent(t *testing.T) {
	identity := pgdefaultprivilege.Identity{Grantor: "owner", Schema: "app", ObjectType: "TABLES", Grantee: "reader"}
	tests := []struct {
		name               string
		class              pgdefaultprivilege.Class
		changes, undecided int
	}{
		{"FOR ALL ROLES", pgdefaultprivilege.Class{Schema: "app", ObjectType: "TABLES"}, 1, 0},
		{"unknown class", pgdefaultprivilege.Class{Grantor: "owner", Schema: "app", ObjectType: "x"}, 1, 0},
		{"different grantor", pgdefaultprivilege.Class{Grantor: "other", Schema: "app", ObjectType: "TABLES"}, 1, 0},
		{"global is separate", pgdefaultprivilege.Class{Grantor: "owner", ObjectType: "TABLES"}, 1, 0},
		{"named unread class", pgdefaultprivilege.Class{Grantor: "owner", Schema: "app", ObjectType: "TABLES"}, 0, 1},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			request := comparison(c, identity, []schemaext.Value{&pgdefaultprivilege.Desired{Privileges: []pgdefaultprivilege.Grant{{Privilege: "SELECT"}}}}, nil, nil)
			ref, err := pgdefaultprivilege.ClassRef(test.class, request.Identifiers)
			c.Assert(err, qt.IsNil)
			request.Current.Coverage, err = pgdefaultprivilege.Coverage(schemaext.Observed, schemaext.Knowledge{State: schemaext.Complete}, []schemaext.SubjectCoverage{
				{Kind: pgdefaultprivilege.DefaultPrivilegeKind, Subject: ref, Knowledge: schemaext.Knowledge{State: schemaext.Unrepresentable, Reason: "ACL contents cannot be read"}},
			})
			c.Assert(err, qt.IsNil)
			result, err := (privilegecompare.Service{}).CompareObjects(t.Context(), request)
			c.Assert(err, qt.IsNil)
			c.Assert(result.Changes, qt.HasLen, test.changes)
			c.Assert(result.Undecided, qt.HasLen, test.undecided)
			c.Assert(result.Desired.Objects.Refs(), qt.DeepEquals, request.Desired.Objects.Refs())
		})
	}
}

func TestCompareObjects_WhitespaceDoesNotGrantRemovalAuthority(t *testing.T) {
	c := qt.New(t)
	identity := pgdefaultprivilege.Identity{Grantor: " owner ", Schema: " app ", ObjectType: "TABLES", Grantee: " reader "}
	request := comparison(c, identity, nil, []schemaext.Value{&pgdefaultprivilege.Observed{Privileges: []pgdefaultprivilege.Grant{{Privilege: "SELECT"}}}}, []string{"owner"})
	result, err := (privilegecompare.Service{}).CompareObjects(t.Context(), request)
	c.Assert(err, qt.IsNil)
	c.Assert(result.Changes, qt.HasLen, 0)
	c.Assert(result.Undecided, qt.HasLen, 0)
}
