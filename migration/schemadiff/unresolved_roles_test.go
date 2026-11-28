package schemadiff_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/coverage"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/migration/schemadiff"
)

// unresolvedRead is a MySQL read that could not tell the accounts behind the
// name nm@localhost apart, and holds the role nmr.
func unresolvedRead() *catalog.Database {
	return &catalog.Database{
		Roles: []catalog.Role{{Name: "nmr"}},
		NotDescribed: coverage.Set{}.With(coverage.Object{
			Kind: coverage.Role, Name: "nm@localhost", Reason: coverage.Unresolved, Provenance: coverage.Observed,
		}),
	}
}

// TestCompareWithDatabaseInfoRefusesARoleTheReadCouldNotResolve refuses each
// declaration that names the unresolved principal.
func TestCompareWithDatabaseInfoRefusesARoleTheReadCouldNotResolve(t *testing.T) {
	tests := []struct {
		name    string
		desired *schemamodel.Database
	}{
		{name: "a role", desired: &schemamodel.Database{Roles: []schemamodel.Role{{Name: "nm@localhost", Inherit: true}}}},
		{name: "a membership", desired: &schemamodel.Database{Roles: []schemamodel.Role{{Name: "nmr", Inherit: true, MemberOf: []string{" nm@localhost"}}}}},
		{name: "a grant", desired: &schemamodel.Database{Grants: []schemamodel.Grant{{Role: "nm@localhost", Privileges: []string{"SELECT"}, OnSchema: "nmdb"}}}},
		{name: "a revoke", desired: &schemamodel.Database{RevokedGrants: []schemamodel.Grant{{Role: "nm@localhost", Privileges: []string{"SELECT"}, OnSchema: "nmdb"}}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			diff, err := schemadiff.CompareWithDatabaseInfo(
				t.Context(), test.desired, unresolvedRead(), catalog.ServerInfo{Dialect: "mysql"}, nil, must.Must(builtin.New()),
			)

			c.Assert(err, qt.ErrorMatches, `role "nm@localhost" names a principal the read could not resolve, such as two accounts that read as one name, `+
				`so a plan cannot say which one it changes: rename one of them`)
			c.Assert(diff, qt.IsNil)
		})
	}
}

// TestCompareWithDatabaseInfoComparesBesideAnUnresolvedRole compares a
// declaration that names other roles, and one that names a role the read
// left out for another reason.
func TestCompareWithDatabaseInfoComparesBesideAnUnresolvedRole(t *testing.T) {
	tests := []struct {
		name     string
		database *catalog.Database
	}{
		{name: "an unresolved role", database: unresolvedRead()},
		{name: "a role left out by the selection", database: &catalog.Database{
			NotDescribed: coverage.Set{}.With(coverage.Object{Kind: coverage.Role, Name: "nm@localhost", Reason: coverage.OutsideScope}),
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			desired := &schemamodel.Database{
				Roles:  []schemamodel.Role{{Name: "nmr", Inherit: true}},
				Grants: []schemamodel.Grant{{Role: "nmr", Privileges: []string{"SELECT"}, OnSchema: "nmdb"}},
			}

			diff, err := schemadiff.CompareWithDatabaseInfo(
				t.Context(), desired, test.database, catalog.ServerInfo{Dialect: "mysql"}, nil, must.Must(builtin.New()),
			)

			c.Assert(err, qt.IsNil)
			c.Assert(diff.GrantsAdded, qt.HasLen, 1)
		})
	}
}

// TestCompareWithDatabaseInfoComparesARoleLeftOutForAnotherReason compares a
// declaration that names a role the read left out for a reason other than an
// unresolved name: the selection, or a kind the read does not inspect. The
// refusal is for a name the read could not tell apart, and only that one.
func TestCompareWithDatabaseInfoComparesARoleLeftOutForAnotherReason(t *testing.T) {
	for _, reason := range []coverage.Reason{coverage.OutsideScope, coverage.NotInspected} {
		t.Run(string(reason), func(t *testing.T) {
			c := qt.New(t)
			database := &catalog.Database{
				NotDescribed: coverage.Set{}.With(coverage.Object{Kind: coverage.Role, Name: "nm@localhost", Reason: reason, Provenance: coverage.Observed}),
			}
			desired := &schemamodel.Database{
				Grants: []schemamodel.Grant{{Role: "nm@localhost", Privileges: []string{"SELECT"}, OnSchema: "nmdb"}},
			}

			diff, err := schemadiff.CompareWithDatabaseInfo(
				t.Context(), desired, database, catalog.ServerInfo{Dialect: "mysql"}, nil, must.Must(builtin.New()),
			)

			c.Assert(err, qt.IsNil)
			c.Assert(diff, qt.IsNotNil)
		})
	}
}
