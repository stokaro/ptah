package generator_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/migration/generator"
	"ptah.run/migration/schemadiff/difftypes"
)

// A rollback shows again an index the forward change hid from the optimizer,
// in place, as MySQL 8.4.11 spells it (stokaro/ptah#3853).
func TestPlanBidirectionalSchemaDiff_IndexVisibilityRollsBack(t *testing.T) {
	c := qt.New(t)
	diff := &difftypes.SchemaDiff{IndexVisibilityChanged: []difftypes.IndexVisibilityChange{
		{TableName: "orders", Name: "k_total", Invisible: true},
	}}

	plan, err := generator.PlanBidirectionalSchemaDiff(generator.BidirectionalSchemaPlanOptions{
		Diff:          diff,
		DesiredSchema: &schemamodel.Database{},
		CurrentSchema: &catalog.Database{},
		Dialect:       platform.MySQL,
		Capabilities:  capability.MySQL84(),
	})
	c.Assert(err, qt.IsNil)
	sql, err := builtin.RenderSQLWithCapabilities(platform.MySQL, capability.MySQL84(), plan.Reverse.Nodes...)

	c.Assert(err, qt.IsNil)
	c.Assert(plan.Reverse.Diff.IndexVisibilityChanged, qt.DeepEquals, []difftypes.IndexVisibilityChange{
		{TableName: "orders", Name: "k_total", Invisible: false},
	})
	c.Assert(sql, qt.Contains, "ALTER TABLE `orders` ALTER INDEX `k_total` VISIBLE;")
	c.Assert(diff.IndexVisibilityChanged[0].Invisible, qt.IsTrue,
		qt.Commentf("the reversal must not write through to the forward diff"))
}
