package atlashclrender_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/platform"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/ydb/ydbcoordination"
	"ptah.run/internal/atlashcl"
	"ptah.run/internal/atlashclrender"
)

// TestRenderedCoordinationNodeRoundTrips is `schema inspect > out.hcl` then
// `schema apply --to file://out.hcl` for a YDB coordination node: the block
// carries the directory, the name and the settings the node was given, and
// leaves out the ones it was not, so the parsed node is the node rendered.
func TestRenderedCoordinationNodeRoundTrips(t *testing.T) {
	c := qt.New(t)
	db := inspectedTable("")
	db.Schemas = []schemamodel.Schema{{Name: "app"}}
	nodes := []schemaext.Object{
		ydbcoordination.DesiredObject("app", "limits", "", ydbcoordination.Spec{
			SelfCheckPeriodMillis: 1500, SessionGracePeriodMillis: 20000,
			ReadConsistencyMode: "strict", AttachConsistencyMode: "relaxed", RateLimiterCountersMode: "detailed",
		}),
		ydbcoordination.DesiredObject("", "locks", "", ydbcoordination.Spec{}),
	}
	db.FeatureObjects = must.Must(schemaext.NewObjects(nodes...))

	result, err := atlashclrender.RenderInspected(db, platform.YDB, "")
	c.Assert(err, qt.IsNil)
	parsed, err := atlashcl.Parse(result.Data, "rendered.hcl")

	c.Assert(err, qt.IsNil)
	c.Assert(string(result.Data), qt.Contains, `coordination_node "limits" {`)
	c.Assert(string(result.Data), qt.Contains, `  self_check_period = "PT1.5S"`)
	c.Assert(parsed.FeatureObjects.Equal(db.FeatureObjects), qt.IsTrue)
	c.Assert(result.Diagnostics, qt.HasLen, 0)
}
