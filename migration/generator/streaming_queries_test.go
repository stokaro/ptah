package generator_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/ast"
	"ptah.run/core/platform/capability"
	"ptah.run/core/renderer"
	"ptah.run/core/schemamodel"
	"ptah.run/migration/generator"
	"ptah.run/migration/schemadiff"
)

func TestStreamingQueries_ReverseBodyChangeRequiresTheSamePermission(t *testing.T) {
	c := qt.New(t)
	caps := capability.YDB262().With(capability.StreamingQueries, true)
	current := &catalog.Database{StreamingQueries: []catalog.StreamingQuery{{Name: "q", Spec: ast.StreamingQuerySpec{Text: "INSERT INTO dst SELECT * FROM src;", Run: new(false)}}}}
	desired := &schemamodel.Database{StreamingQueries: []schemamodel.StreamingQuery{{Name: "q", Spec: ast.StreamingQuerySpec{Text: "INSERT INTO dst SELECT * FROM src; /* changed */", Run: new(false)}}}}
	diff, err := schemadiff.CompareWithDatabaseInfo(desired, current, catalog.ServerInfo{Dialect: "ydb", Capabilities: caps}, nil)
	c.Assert(err, qt.IsNil)
	_, err = generator.PlanBidirectionalSchemaDiff(generator.BidirectionalSchemaPlanOptions{Diff: diff, DesiredSchema: desired, CurrentSchema: current, Dialect: "ydb", Capabilities: caps})
	c.Assert(err, qt.ErrorMatches, `(?s).*allow_state_reset=true.*`)
	desired.StreamingQueries[0].AllowStateReset = true
	diff, err = schemadiff.CompareWithDatabaseInfo(desired, current, catalog.ServerInfo{Dialect: "ydb", Capabilities: caps}, nil)
	c.Assert(err, qt.IsNil)
	plan, err := generator.PlanBidirectionalSchemaDiff(generator.BidirectionalSchemaPlanOptions{Diff: diff, DesiredSchema: desired, CurrentSchema: current, Dialect: "ydb", Capabilities: caps})
	c.Assert(err, qt.IsNil)
	forward, err := renderer.RenderSQLWithCapabilities("ydb", caps, plan.Forward.Nodes...)
	c.Assert(err, qt.IsNil)
	reverse, err := renderer.RenderSQLWithCapabilities("ydb", caps, plan.Reverse.Nodes...)
	c.Assert(err, qt.IsNil)
	c.Assert(forward, qt.Contains, "FORCE = TRUE")
	c.Assert(forward, qt.Contains, "/* changed */")
	c.Assert(reverse, qt.Contains, "FORCE = TRUE")
	c.Assert(reverse, qt.Not(qt.Contains), "/* changed */")
	c.Assert(forward, qt.Not(qt.Contains), "DROP STREAMING")
	c.Assert(reverse, qt.Not(qt.Contains), "DROP STREAMING")
}
