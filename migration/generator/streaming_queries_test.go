package generator_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/platform/capability"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/ydb/ydbstreaming"
	"ptah.run/engine/builtin"
	"ptah.run/migration/generator"
	"ptah.run/migration/schemadiff"
)

func TestStreamingQueries_ReverseBodyChangeRequiresTheSamePermission(t *testing.T) {
	c := qt.New(t)
	caps := capability.YDB262().With(capability.StreamingQueries, true)
	current := &catalog.Database{FeatureCoverage: must.Must(ydbstreaming.Coverage(schemaext.Observed, schemaext.Knowledge{State: schemaext.Complete}, nil)), FeatureObjects: must.Must(schemaext.NewObjects(ydbstreaming.ObservedObject("", "q", ydbstreaming.Spec{Text: "INSERT INTO dst SELECT * FROM src;", Run: new(false)})))}
	after := ydbstreaming.DesiredObject("", "q", "", ydbstreaming.Spec{Text: "INSERT INTO dst SELECT * FROM src WHERE TRUE;", Run: new(false)}, false)
	desired := &schemamodel.Database{FeatureCoverage: must.Must(ydbstreaming.Coverage(schemaext.Desired, schemaext.Knowledge{State: schemaext.Complete}, nil)), FeatureObjects: must.Must(schemaext.NewObjects(after))}
	diff, err := schemadiff.CompareWithDatabaseInfo(t.Context(),
		desired, current, catalog.ServerInfo{Dialect: "ydb", Capabilities: caps}, nil, must.Must(builtin.New()),
	)
	c.Assert(err, qt.IsNil)
	_, err = generator.PlanBidirectionalSchemaDiff(t.Context(),
		generator.BidirectionalSchemaPlanOptions{Runtime: must.Must(builtin.New()), Diff: diff, DesiredSchema: desired, CurrentSchema: current, Dialect: "ydb", Capabilities: caps})
	c.Assert(err, qt.ErrorMatches, `(?s).*allow_state_reset=true.*`)
	after.Value.(*ydbstreaming.Desired).AllowStateReset = true
	desired.FeatureObjects = must.Must(desired.FeatureObjects.Replace(after))
	diff, err = schemadiff.CompareWithDatabaseInfo(t.Context(),
		desired, current, catalog.ServerInfo{Dialect: "ydb", Capabilities: caps}, nil, must.Must(builtin.New()),
	)
	c.Assert(err, qt.IsNil)
	plan, err := generator.PlanBidirectionalSchemaDiff(t.Context(),
		generator.BidirectionalSchemaPlanOptions{Runtime: must.Must(builtin.New()), Diff: diff, DesiredSchema: desired, CurrentSchema: current, Dialect: "ydb", Capabilities: caps})
	c.Assert(err, qt.IsNil)
	forward, err := builtin.RenderSQLWithCapabilities("ydb", caps, plan.Forward.Nodes...)
	c.Assert(err, qt.IsNil)
	reverse, err := builtin.RenderSQLWithCapabilities("ydb", caps, plan.Reverse.Nodes...)
	c.Assert(err, qt.IsNil)
	c.Assert(forward, qt.Contains, "FORCE = TRUE")
	c.Assert(forward, qt.Contains, "WHERE TRUE")
	c.Assert(reverse, qt.Contains, "FORCE = TRUE")
	c.Assert(reverse, qt.Not(qt.Contains), "WHERE TRUE")
	c.Assert(forward, qt.Not(qt.Contains), "DROP STREAMING")
	c.Assert(reverse, qt.Not(qt.Contains), "DROP STREAMING")
}
