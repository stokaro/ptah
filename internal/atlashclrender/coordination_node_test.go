package atlashclrender_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/platform"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/ydb/ydbcoordination"
	"ptah.run/dialect/ydb/ydbworkload"
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

func TestHCLCoordinationSourceLimitsSurviveRepeatedExport(t *testing.T) {
	for _, source := range []string{
		`// ptah:not-described coordination_node`,
		`// ptah:not-described coordination_node "app/locks"`,
		`// ptah:not-described coordination_node "/locks.v1"`,
		`// ptah:not-described coordination_node "app.v1/locks"`,
	} {
		t.Run(source, func(t *testing.T) {
			c := qt.New(t)
			original, err := atlashcl.Parse([]byte(source), "source.hcl")
			c.Assert(err, qt.IsNil)
			first, err := atlashclrender.RenderForDialect(original, platform.YDB)
			c.Assert(err, qt.IsNil)
			parsed, err := atlashcl.Parse(first.Data, "export.hcl")
			c.Assert(err, qt.IsNil)
			c.Assert(parsed.FeatureCoverage.Equal(original.FeatureCoverage), qt.IsTrue)
			second, err := atlashclrender.RenderForDialect(parsed, platform.YDB)
			c.Assert(err, qt.IsNil)
			c.Assert(second.Data, qt.DeepEquals, first.Data)
		})
	}
}

// Exporting an unenrolled namespace cannot authorize removal when its HCL is
// read back. The complete case keeps omission meaningful for inspected nodes.
func TestRenderedHCLPreservesCoordinationEnrollment(t *testing.T) {
	for _, dialect := range []string{platform.YDB, platform.Postgres} {
		for _, test := range []struct {
			name  string
			known schemaext.Coverage
			want  schemaext.KnowledgeState
		}{
			{name: "unenrolled", want: schemaext.Uninspected},
			{name: "complete", known: must.Must(ydbcoordination.Coverage(schemaext.Desired,
				schemaext.Knowledge{State: schemaext.Complete}, nil)), want: schemaext.Complete},
		} {
			t.Run(dialect+"/"+test.name, func(t *testing.T) {
				c := qt.New(t)
				result, err := atlashclrender.RenderInspected(&schemamodel.Database{FeatureCoverage: test.known}, dialect, "")
				c.Assert(err, qt.IsNil)
				parsed, err := atlashcl.Parse(result.Data, "export.hcl")
				c.Assert(err, qt.IsNil)
				c.Assert(parsed.NotDescribed, qt.DeepEquals, result.NotDescribed)
				c.Assert(parsed.FeatureCoverage.Lookup(ydbcoordination.Kind, ydbcoordination.Ref("app", "locks")).State, qt.Equals, test.want)
				c.Assert(parsed.FeatureCoverage.Lookup(ydbworkload.PoolKind, ydbworkload.PoolRef("batch")).State, qt.Equals, schemaext.Uninspected)
			})
		}
	}
}
