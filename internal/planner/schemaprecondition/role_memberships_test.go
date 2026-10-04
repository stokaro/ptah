package schemaprecondition_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ptaherr"
	"ptah.run/internal/planner/schemaprecondition"
	"ptah.run/migration/schemadiff/difftypes"
)

// TestRefuseRoleMemberships_FailurePath refuses a membership added or removed
// for a planner that plans none, naming both roles and the planner: planning
// nothing would leave the member holding what it held and report the database
// synced.
func TestRefuseRoleMemberships_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		diff    *difftypes.SchemaDiff
		wantErr string
	}{
		{name: "an addition",
			diff:    &difftypes.SchemaDiff{RoleMembershipsAdded: []difftypes.RoleMembershipRef{{Role: "readers", Member: "app"}}},
			wantErr: `unsupported feature: the diff makes "app" a member of "readers", and the mysql planner plans no role membership`},
		{name: "a removal",
			diff:    &difftypes.SchemaDiff{RoleMembershipsRemoved: []difftypes.RoleMembershipRef{{Role: "readers", Member: "app"}}},
			wantErr: `unsupported feature: the diff takes "app" out of "readers", and the mysql planner plans no role membership`},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			err := schemaprecondition.RefuseRoleMemberships("mysql", test.diff)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
		})
	}
}

// TestRefuseRoleMemberships_HappyPath passes a diff carrying no membership,
// a role change included, and no diff at all.
func TestRefuseRoleMemberships_HappyPath(t *testing.T) {
	tests := []struct {
		name string
		diff *difftypes.SchemaDiff
	}{
		{name: "no diff"},
		{name: "a role added", diff: &difftypes.SchemaDiff{RolesAdded: difftypes.RoleChanges{{Name: "app"}}}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(schemaprecondition.RefuseRoleMemberships("mysql", test.diff), qt.IsNil)
		})
	}
}
