package generator_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/migration/generator"
	"ptah.run/migration/schemadiff/difftypes"
)

// A table made unlogged is made logged again by the rollback, and the forward
// plan writes the change it was asked for.
func TestPlanBidirectionalSchemaDiff_UnloggedTableRollsBack(t *testing.T) {
	c := qt.New(t)
	diff := &difftypes.SchemaDiff{TablesModified: []difftypes.TableDiff{{
		TableName: "app.cache", UnloggedChange: &difftypes.UnloggedChange{Current: false, Desired: true},
	}}}

	plan, err := generator.PlanBidirectionalSchemaDiff(c.Context(),
		generator.BidirectionalSchemaPlanOptions{Runtime: must.Must(builtin.New()), Diff: diff,
			DesiredSchema: &schemamodel.Database{},
			CurrentSchema: &catalog.Database{},
			Dialect:       platform.Postgres,
			Capabilities:  capability.Postgres18(),
		})
	c.Assert(err, qt.IsNil)
	forward, err := builtin.RenderSQL(platform.Postgres, plan.Forward.Nodes...)
	c.Assert(err, qt.IsNil)
	reverse, err := builtin.RenderSQL(platform.Postgres, plan.Reverse.Nodes...)
	c.Assert(err, qt.IsNil)

	c.Assert(forward, qt.Contains, `ALTER TABLE "app"."cache" SET UNLOGGED;`)
	c.Assert(plan.Reverse.Diff.TablesModified[0].UnloggedChange, qt.DeepEquals,
		&difftypes.UnloggedChange{Current: true, Desired: false})
	c.Assert(reverse, qt.Contains, `ALTER TABLE "app"."cache" SET LOGGED;`)
	c.Assert(diff.TablesModified[0].UnloggedChange.Desired, qt.IsTrue,
		qt.Commentf("the reversal must not write through to the forward diff"))
}
