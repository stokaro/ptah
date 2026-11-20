package generator

// White-box testing required: reverseSchemaDiffWithPrior is unexported, and
// the rollback of a membership change is observable only in the reverse diff
// it builds, before any statement is rendered from it.

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/difftypes"
	"ptah.run/core/schemamodel"
)

// TestReverseSchemaDiff_RollsBackMembershipOptions swaps the two states of a
// membership whose options changed, and grants a removed membership again with
// the options the database held, so a rollback restores them.
func TestReverseSchemaDiff_RollsBackMembershipOptions(t *testing.T) {
	c := qt.New(t)
	schema, dbSchema := reverseCoverageContext()
	held := schemamodel.RoleMembershipOptions{Admin: new(true), Inherit: new(false), Set: new(true)}
	diff := &difftypes.SchemaDiff{
		RoleMembershipsModified: []difftypes.RoleMembershipChange{{
			Role: "readers", Member: "app",
			From: schemamodel.RoleMembershipOptions{Admin: new(true), Inherit: new(true), Set: new(true)},
			To:   schemamodel.RoleMembershipOptions{Admin: new(false), Inherit: new(true), Set: new(true)},
		}},
		RoleMembershipsRemoved: []difftypes.RoleMembershipRef{{Role: "writers", Member: "app", Options: held}},
	}

	reversed := reverseCoveragePlan(c, diff, schema, dbSchema)

	c.Assert(reversed.RoleMembershipsModified, qt.DeepEquals, []difftypes.RoleMembershipChange{{
		Role: "readers", Member: "app",
		From: schemamodel.RoleMembershipOptions{Admin: new(false), Inherit: new(true), Set: new(true)},
		To:   schemamodel.RoleMembershipOptions{Admin: new(true), Inherit: new(true), Set: new(true)},
	}})
	c.Assert(reversed.RoleMembershipsAdded, qt.DeepEquals, []difftypes.RoleMembershipRef{
		{Role: "writers", Member: "app", Options: held},
	})
}
