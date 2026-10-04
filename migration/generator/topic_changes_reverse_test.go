package generator_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/renderer"
	"ptah.run/core/schemamodel"
	"ptah.run/migration/generator"
	"ptah.run/migration/schemadiff/difftypes"
)

// A rollback drops the topic the forward plan created, creates again the one
// it dropped from the settings and consumers the removal carried, and moves a
// changed topic back to the nearest state YDB reaches in place: the
// partitions the forward change added stay, and the consumer it dropped comes
// back. The forward diff is left as it was.
func TestPlanBidirectionalSchemaDiff_TopicsRollBack(t *testing.T) {
	c := qt.New(t)
	diff := &difftypes.SchemaDiff{
		TopicsAdded:   difftypes.TopicChanges{{Name: "fresh"}},
		TopicsRemoved: difftypes.TopicChanges{{Name: "gone", Spec: ast.TopicSpec{RetentionPeriod: "PT2H"}}},
		TopicsModified: []difftypes.TopicDiff{{Name: "events",
			Desired: ast.TopicSpec{MinActivePartitions: 3},
			Current: ast.TopicSpec{MinActivePartitions: 1, Consumers: []ast.TopicConsumerSpec{{Name: "billing"}}},
		}},
	}

	plan, err := generator.PlanBidirectionalSchemaDiff(generator.BidirectionalSchemaPlanOptions{
		Diff:          diff,
		DesiredSchema: &schemamodel.Database{},
		CurrentSchema: &catalog.Database{},
		Dialect:       platform.YDB,
		Capabilities:  capability.YDB262(),
	})
	c.Assert(err, qt.IsNil)
	sql, err := renderer.RenderSQLWithCapabilities(platform.YDB, capability.YDB262(), plan.Reverse.Nodes...)

	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Equals, "DROP TOPIC `fresh`;\n"+
		"CREATE TOPIC `gone` WITH (retention_period = Interval('PT2H'));\n"+
		"ALTER TOPIC `events` ADD CONSUMER `billing`;\n")
	c.Assert(diff.TopicsModified[0].Current.Consumers, qt.HasLen, 1,
		qt.Commentf("the reversal must not write through to the forward diff"))
	c.Assert(diff.TopicsRemoved.Names(), qt.DeepEquals, []string{"gone"})
}
