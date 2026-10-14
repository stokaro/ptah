package atlashclrender_test

import (
	"strings"
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
	"ptah.run/internal/builtintest"
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

	result, err := atlashclrender.RenderInspected(db, "", atlashclrender.Options{HCL: builtintest.Runtime().HCL(), Dialect: platform.YDB})
	c.Assert(err, qt.IsNil)
	parsed, err := atlashcl.ParseWithOptions(result.Data, "rendered.hcl", atlashcl.Options{HCL: builtintest.Runtime().HCL()})

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
			original, err := atlashcl.ParseWithOptions([]byte(source), "source.hcl", atlashcl.Options{HCL: builtintest.Runtime().HCL()})
			c.Assert(err, qt.IsNil)
			first, err := atlashclrender.Render(original, atlashclrender.Options{HCL: builtintest.Runtime().HCL(), Dialect: platform.YDB})
			c.Assert(err, qt.IsNil)
			parsed, err := atlashcl.ParseWithOptions(first.Data, "export.hcl", atlashcl.Options{HCL: builtintest.Runtime().HCL()})
			c.Assert(err, qt.IsNil)
			c.Assert(parsed.FeatureCoverage.Equal(original.FeatureCoverage), qt.IsTrue)
			second, err := atlashclrender.Render(parsed, atlashclrender.Options{HCL: builtintest.Runtime().HCL(), Dialect: platform.YDB})
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
				result, err := atlashclrender.RenderInspected(&schemamodel.Database{FeatureCoverage: test.known}, "", atlashclrender.Options{HCL: builtintest.Runtime().HCL(), Dialect: dialect})
				c.Assert(err, qt.IsNil)
				parsed, err := atlashcl.ParseWithOptions(result.Data, "export.hcl", atlashcl.Options{HCL: builtintest.Runtime().HCL()})
				c.Assert(err, qt.IsNil)
				c.Assert(parsed.NotDescribed, qt.DeepEquals, result.NotDescribed)
				c.Assert(parsed.FeatureCoverage.Lookup(ydbcoordination.Kind, ydbcoordination.Ref("app", "locks")).State, qt.Equals, test.want)
				c.Assert(parsed.FeatureCoverage.Lookup(ydbworkload.PoolKind, ydbworkload.PoolRef("batch")).State, qt.Equals, schemaext.Uninspected)
			})
		}
	}
}

func TestHCLRefusesIncompatibleAndDuplicateCapturedModels(t *testing.T) {
	known := must.Must(ydbcoordination.Coverage(schemaext.Desired, schemaext.Knowledge{State: schemaext.Complete}, nil))
	source := must.Must(atlashclrender.Render(&schemamodel.Database{FeatureCoverage: known}, atlashclrender.Options{HCL: builtintest.Runtime().HCL(), Dialect: "ydb"}))
	for _, test := range []struct {
		name string
		text string
		want error
	}{
		{"version", strings.Replace(string(source.Data), `"version":1`, `"version":2`, 1), schemaext.ErrIncompatibleCodec},
		{"second account", "// ptah:not-described coordination_node\n" + string(source.Data), schemaext.ErrDuplicate},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			parsed, err := atlashcl.ParseWithOptions([]byte(test.text), "source.hcl", atlashcl.Options{HCL: builtintest.Runtime().HCL()})
			c.Assert(err, qt.ErrorIs, test.want)
			c.Assert(parsed, qt.IsNil)
		})
	}
}

// TestRenderRefusesACoordinationNodeYDBWouldNotKeep refuses a node whose
// settings YDB would move into its range, rather than write a block the
// reader refuses.
func TestRenderRefusesACoordinationNodeYDBWouldNotKeep(t *testing.T) {
	c := qt.New(t)
	db := &schemamodel.Database{FeatureObjects: must.Must(schemaext.NewObjects(
		ydbcoordination.DesiredObject("app", "limits", "", ydbcoordination.Spec{SelfCheckPeriodMillis: 100}),
	))}

	result, err := atlashclrender.Render(db, atlashclrender.Options{HCL: builtintest.Runtime().HCL(), Dialect: platform.YDB})

	c.Assert(err, qt.ErrorMatches, `.*self_check_period PT0\.1S: YDB runs a node's self-check every PT0\.5S to PT10S.*`)
	c.Assert(result.Data, qt.IsNil)
}

// TestRenderedObjectGrantsRoundTrip writes a grant on a coordination node as
// a `permission` whose `for` names the node's block, which reads back as the
// same grant. A grant on a topic, which no HCL block declares, is left out
// with a warning rather than written as a grant on something else.
func TestRenderedObjectGrantsRoundTrip(t *testing.T) {
	c := qt.New(t)
	db := inspectedTable("")
	db.Schemas = []schemamodel.Schema{{Name: "app"}}
	db.FeatureObjects = must.Must(schemaext.NewObjects(ydbcoordination.DesiredObject("app", "locks", "", ydbcoordination.Spec{})))
	db.Roles = []schemamodel.Role{{Name: "reader", Login: true, Inherit: true}}
	db.Grants = []schemamodel.Grant{
		{Role: "reader", Privileges: []string{"YDB.GENERIC.USE"}, OnObject: "app.locks", ObjectKind: "COORDINATION NODE"},
		{Role: "reader", Privileges: []string{"YDB.GENERIC.READ"}, OnObject: "app.events", ObjectKind: "TOPIC"},
	}

	result, err := atlashclrender.Render(db, atlashclrender.Options{HCL: builtintest.Runtime().HCL(), Dialect: platform.YDB})
	c.Assert(err, qt.IsNil)
	parsed, err := atlashcl.ParseWithOptions(result.Data, "rendered.hcl", atlashcl.Options{HCL: builtintest.Runtime().HCL()})

	c.Assert(err, qt.IsNil)
	c.Assert(string(result.Data), qt.Contains, "  for = coordination_node.app.locks\n")
	c.Assert(parsed.Grants, qt.DeepEquals, db.Grants[:1])
	c.Assert(strings.Join(diagnosticMessages(result.Diagnostics), "\n"), qt.Contains,
		"grant requires role, table, schema, sequence or routine target, or an object an owner's block declares")
}
