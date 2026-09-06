package constraintscope_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform/identifier"
	"ptah.run/internal/constraintscope"
	"ptah.run/migration/schemadiff/difftypes"
)

// TestNormalize_FillsAnIdentityNoProducerResolved is what Normalize is for.
//
// A diff the comparator built arrives with identities resolved. One an embedder
// built by hand does not, and a planner keying on the zero identity reads every
// such constraint as one key: it would pair a drop on one table with an add on
// another and emit neither correctly.
//
// It asked a second question until stokaro/ptah#2315: the diff carried a bare
// name list beside the records, and Normalize synthesized a record for a name
// that had none. There are no bare names now -- a constraint change IS its
// record -- so that half is gone and this is what is left.
func TestNormalize_FillsAnIdentityNoProducerResolved(t *testing.T) {
	c := qt.New(t)
	diff := &difftypes.SchemaDiff{
		ConstraintsAdded: difftypes.ConstraintAdditions{
			{Name: "chk_total", TableName: "app.orders", Type: "CHECK"},
		},
		ConstraintsRemoved: difftypes.ConstraintRemovals{
			{Name: "chk_legacy", TableName: "app.orders", Type: "CHECK"},
		},
	}

	constraintscope.Normalize(diff, identifier.ForDialect("postgres"))

	c.Assert(diff.ConstraintsAdded[0].Identity, qt.Not(qt.Equals), difftypes.ConstraintIdentity{})
	c.Assert(diff.ConstraintsAdded[0].Identity.Table, qt.Equals, "orders")
	c.Assert(diff.ConstraintsAdded[0].Identity.Name, qt.Equals, "chk_total")
	c.Assert(diff.ConstraintsRemoved[0].Identity, qt.Not(qt.Equals), difftypes.ConstraintIdentity{})
	c.Assert(diff.ConstraintsRemoved[0].Identity.Name, qt.Equals, "chk_legacy")
}

// TestNormalize_LeavesAResolvedIdentityAlone is the control: the fill is for
// records that carry none, and a producer's answer is never replaced.
func TestNormalize_LeavesAResolvedIdentityAlone(t *testing.T) {
	c := qt.New(t)
	resolved := difftypes.ConstraintIdentity{Schema: "elsewhere", Table: "other", Name: "resolved"}
	diff := &difftypes.SchemaDiff{
		ConstraintsAdded: difftypes.ConstraintAdditions{
			{Name: "chk_total", TableName: "app.orders", Type: "CHECK", Identity: resolved},
		},
	}

	constraintscope.Normalize(diff, identifier.ForDialect("postgres"))

	c.Assert(diff.ConstraintsAdded[0].Identity, qt.Equals, resolved)
}

// TestNormalize_RunningTwiceChangesNothing pins that the fill is idempotent.
//
// Normalize runs at the door of two planners, and a diff can reach both. A pass
// that rewrote what the previous one filled would make a planner emit the
// constraint twice, and the assertions above would not see it.
func TestNormalize_RunningTwiceChangesNothing(t *testing.T) {
	c := qt.New(t)
	diff := &difftypes.SchemaDiff{
		ConstraintsAdded: difftypes.ConstraintAdditions{
			{Name: "c", TableName: "orders", Type: "CHECK"},
		},
		ConstraintsRemoved: difftypes.ConstraintRemovals{
			{Name: "e", TableName: "orders", Type: "CHECK"},
		},
	}

	constraintscope.Normalize(diff, identifier.ForDialect("postgres"))
	onceAdded := append(difftypes.ConstraintAdditions(nil), diff.ConstraintsAdded...)
	onceRemoved := append(difftypes.ConstraintRemovals(nil), diff.ConstraintsRemoved...)
	constraintscope.Normalize(diff, identifier.ForDialect("postgres"))

	c.Assert(diff.ConstraintsAdded, qt.DeepEquals, onceAdded)
	c.Assert(diff.ConstraintsRemoved, qt.DeepEquals, onceRemoved)
}
