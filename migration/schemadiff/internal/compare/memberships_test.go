package compare_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/coverage"
	"ptah.run/core/difftypes"
	"ptah.run/core/platform/capability"
	"ptah.run/core/schemamodel"
	"ptah.run/migration/schemadiff/internal/compare"
)

func TestMembershipRemovalRequiresIndependentCoverage(t *testing.T) {
	for _, test := range []struct {
		name              string
		desired, observed coverage.Set
		removed           int
	}{
		{name: "complete", removed: 1},
		{name: "desired unknown", desired: coverage.Set{}.With(coverage.Refused(coverage.RoleMembership))},
		{name: "observed unknown", observed: coverage.Set{}.With(coverage.Refused(coverage.RoleMembership))},
		{name: "another member unknown", observed: coverage.Set{}.WithObject(coverage.RoleMembership, "other"), removed: 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			desired := &schemamodel.Database{Roles: []schemamodel.Role{{Name: "app"}, {Name: "team", Group: true}}, NotDescribed: test.desired}
			current := &catalog.Database{RoleMemberships: []catalog.RoleMembership{{Role: "team", Member: "app"}}, NotDescribed: test.observed}
			var diff difftypes.SchemaDiff
			compare.RoleMemberships(desired, current, &diff, capability.Of(map[capability.Capability]bool{capability.RoleMembership: true}))
			c.Assert(diff.RoleMembershipsRemoved, qt.HasLen, test.removed)
			c.Assert(diff.RoleMembershipsAdded, qt.HasLen, 0)
		})
	}
}

func TestMembershipAdditionRetainsExplicitIntentWithUnreadCurrentState(t *testing.T) {
	c := qt.New(t)
	desired := &schemamodel.Database{Roles: []schemamodel.Role{{Name: "app", MemberOf: []string{"team"}}}}
	current := &catalog.Database{NotDescribed: coverage.Set{}.With(coverage.Refused(coverage.RoleMembership))}
	var diff difftypes.SchemaDiff
	compare.RoleMemberships(desired, current, &diff, capability.Of(map[capability.Capability]bool{capability.RoleMembership: true}))
	c.Assert(diff.RoleMembershipsAdded, qt.DeepEquals, []difftypes.RoleMembershipRef{{Role: "team", Member: "app"}})
	desired.NotDescribed = coverage.Set{}.With(coverage.Refused(coverage.RoleMembership))
	diff = difftypes.SchemaDiff{}
	compare.RoleMemberships(desired, current, &diff, capability.Of(map[capability.Capability]bool{capability.RoleMembership: true}))
	c.Assert(diff.RoleMembershipsAdded, qt.HasLen, 0)
}
