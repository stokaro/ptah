package generator_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/ast"
	"ptah.run/core/featureplan"
	"ptah.run/core/platform/capability"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/ydb/ydbdiff"
	"ptah.run/dialect/ydb/ydbschema"
	"ptah.run/engine"
	"ptah.run/engine/builtin"
	"ptah.run/internal/convert/goschematodb"
	"ptah.run/migration/generator"
	"ptah.run/migration/schemadiff"
)

type selectedFeaturePlanning struct {
	*engine.Runtime
	plan func(context.Context, featureplan.Request) (featureplan.Result, error)
}

func (s selectedFeaturePlanning) PlanFeatures(ctx context.Context, request featureplan.Request) (featureplan.Result, error) {
	return s.plan(ctx, request)
}

func changefeedDeclaration(c *qt.C, stream ydbschema.ChangefeedSpec) *schemamodel.Database {
	c.Helper()
	objects, err := schemaext.NewObjects(ydbschema.DesiredObject("", "items", stream))
	c.Assert(err, qt.IsNil)
	coverage, err := ydbschema.ChangefeedCoverage(schemaext.Desired, nil)
	c.Assert(err, qt.IsNil)
	return &schemamodel.Database{
		Tables:         []schemamodel.Table{{Name: "items", StructName: "Item"}},
		Fields:         []schemamodel.Field{{Name: "id", StructName: "Item", Type: "uint64", Primary: true}},
		FeatureObjects: objects, FeatureCoverage: coverage,
	}
}

// A rollback restores declarations and reports the state that recreation loses.
func TestPlanBidirectionalSchemaDiff_ChangefeedsRollBack(t *testing.T) {
	c := qt.New(t)
	runtime, err := builtin.New()
	c.Assert(err, qt.IsNil)
	added := ydbschema.ChangefeedSpec{Name: "fresh", Mode: "KEYS_ONLY", Format: "JSON"}
	dropped := ydbschema.ChangefeedSpec{Name: "gone", Mode: "UPDATES", Format: "JSON", Consumers: []ast.TopicConsumerSpec{{Name: "reader"}}}
	before, after := changefeedDeclaration(c, dropped), changefeedDeclaration(c, added)
	current, err := goschematodb.ToDBSchema(t.Context(), before, "ydb", runtime)
	c.Assert(err, qt.IsNil)
	caps := capability.YDB262()
	diff, err := schemadiff.CompareWithDatabaseInfo(t.Context(), after, current, catalog.ServerInfo{Dialect: "ydb", Capabilities: caps}, nil, runtime)
	c.Assert(err, qt.IsNil)
	var requests []featureplan.Request
	selected := selectedFeaturePlanning{Runtime: runtime, plan: func(ctx context.Context, request featureplan.Request) (featureplan.Result, error) {
		c.Assert(ctx, qt.Equals, t.Context())
		requests = append(requests, request)
		return runtime.PlanFeatures(ctx, request)
	}}
	plan, err := generator.PlanBidirectionalSchemaDiff(t.Context(), generator.BidirectionalSchemaPlanOptions{
		Runtime: selected, Diff: diff, DesiredSchema: after, CurrentSchema: current, Dialect: "ydb", Capabilities: caps,
	})
	c.Assert(err, qt.IsNil)
	c.Assert(requests, qt.HasLen, 2)
	c.Assert(requests[0].Changes[0].Value.(*ydbdiff.Changefeed).After.Spec, qt.DeepEquals, added)
	c.Assert(requests[1].Changes[0].Value.(*ydbdiff.Changefeed).Before.Spec, qt.DeepEquals, added)
	c.Assert(plan.Reverse.Diff.TablesModified, qt.HasLen, 1)
	changes := plan.Reverse.Diff.TablesModified[0].FeatureChanges
	c.Assert(changes, qt.HasLen, 2)
	c.Assert(changes[0].Subject, qt.Equals, ydbschema.ChangefeedRef("", "items", "fresh"))
	c.Assert(changes[0].Value.(*ydbdiff.Changefeed).After, qt.IsNil)
	c.Assert(changes[1].Value.(*ydbdiff.Changefeed).After.Spec, qt.DeepEquals, dropped)
	sql, err := builtin.RenderSQLWithCapabilities("ydb", caps, plan.Reverse.Nodes...)
	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Contains, "Recovery limit:")
	c.Assert(sql, qt.Contains, "ALTER TABLE `items` DROP CHANGEFEED `fresh`;")
	c.Assert(sql, qt.Contains, "ALTER TABLE `items` ADD CHANGEFEED `gone` WITH (MODE = 'UPDATES', FORMAT = 'JSON');")
	c.Assert(sql, qt.Contains, "ALTER TOPIC `items/gone` ADD CONSUMER `reader`;")
	c.Assert(diff.TablesModified[0].FeatureChanges[0].Value.(*ydbdiff.Changefeed).After.Spec, qt.DeepEquals, added)
}
