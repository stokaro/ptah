package generator_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/migration/generator"
	"ptah.run/migration/schemadiff/difftypes"
)

// A rollback of a changefeed change takes the table back to the changefeeds
// it held: the one the forward change added is dropped, and the one it
// dropped is added again with its consumer. The forward diff is left as it
// was.
func TestPlanBidirectionalSchemaDiff_ChangefeedsRollBack(t *testing.T) {
	c := qt.New(t)
	added := ast.ChangefeedSpec{Name: "fresh", Mode: "KEYS_ONLY", Format: "JSON"}
	dropped := ast.ChangefeedSpec{Name: "gone", Mode: "UPDATES", Format: "JSON",
		Consumers: []ast.TopicConsumerSpec{{Name: "reader"}}}
	diff := &difftypes.SchemaDiff{TablesModified: []difftypes.TableDiff{{
		TableName: "items",
		ChangefeedsChange: &difftypes.ChangefeedsChange{
			Desired: []ast.ChangefeedSpec{added},
			Current: []ast.ChangefeedSpec{dropped},
		},
	}}}

	plan, err := generator.PlanBidirectionalSchemaDiff(generator.BidirectionalSchemaPlanOptions{
		Diff:          diff,
		DesiredSchema: &schemamodel.Database{},
		CurrentSchema: &catalog.Database{},
		Dialect:       platform.YDB,
		Capabilities:  capability.YDB262(),
	})
	c.Assert(err, qt.IsNil)
	sql, err := builtin.RenderSQLWithCapabilities(platform.YDB, capability.YDB262(), plan.Reverse.Nodes...)

	c.Assert(err, qt.IsNil)
	c.Assert(plan.Reverse.Diff.TablesModified, qt.HasLen, 1)
	c.Assert(plan.Reverse.Diff.TablesModified[0].ChangefeedsChange, qt.DeepEquals, &difftypes.ChangefeedsChange{
		Desired: []ast.ChangefeedSpec{dropped},
		Current: []ast.ChangefeedSpec{added},
	})
	c.Assert(sql, qt.Equals, "-- Changefeed fresh of table items is dropped with its topic: the records nobody read are lost.\n"+
		"ALTER TABLE `items` DROP CHANGEFEED `fresh`;\n"+
		"ALTER TABLE `items` ADD CHANGEFEED `gone` WITH (MODE = 'UPDATES', FORMAT = 'JSON');\n"+
		"ALTER TOPIC `items/gone` ADD CONSUMER `reader`;\n")
	c.Assert(diff.TablesModified[0].ChangefeedsChange.Desired, qt.DeepEquals, []ast.ChangefeedSpec{added},
		qt.Commentf("the reversal must not write through to the forward diff"))
}
