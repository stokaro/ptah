package atlasfilter_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/ydb/ydbcoordination"
	"ptah.run/internal/atlasfilter"
)

var coordinationScopeCases = []struct {
	name  string
	scope atlasfilter.Scope
}{
	{name: "include", scope: atlasfilter.Scope{Include: []string{"app.*[type=coordination_node]"}}},
	{name: "exclude", scope: atlasfilter.Scope{Exclude: []string{"other.*[type=coordination_node]"}}},
	{name: "schema", scope: atlasfilter.Scope{Schemas: []string{"app"}}},
}

func TestScopeDatabasePreservesStandaloneCoordinationKnowledge(t *testing.T) {
	for _, test := range coordinationScopeCases {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			source := &catalog.Database{
				FeatureObjects: must.Must(schemaext.NewObjects(
					ydbcoordination.ObservedObject("app", "kept", ydbcoordination.Spec{}),
					ydbcoordination.ObservedObject("other", "dropped", ydbcoordination.Spec{}),
				)),
				FeatureCoverage: coordinationScopeCoverage(c, schemaext.Observed),
			}
			got, err := atlasfilter.ScopeDatabase(source, test.scope)
			c.Assert(err, qt.IsNil)
			assertSelectedCoordination(c, got.FeatureObjects, got.FeatureCoverage, source.FeatureObjects, source.FeatureCoverage)
		})
	}
}

func TestScopeGeneratedPreservesStandaloneCoordinationKnowledge(t *testing.T) {
	for _, test := range coordinationScopeCases {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			source := &schemamodel.Database{
				FeatureObjects: must.Must(schemaext.NewObjects(
					ydbcoordination.DesiredObject("app", "kept", "Kept", ydbcoordination.Spec{}),
					ydbcoordination.DesiredObject("other", "dropped", "Dropped", ydbcoordination.Spec{}),
				)),
				FeatureCoverage: coordinationScopeCoverage(c, schemaext.Desired),
			}
			got, err := atlasfilter.ScopeGenerated(source, test.scope)
			c.Assert(err, qt.IsNil)
			assertSelectedCoordination(c, got.FeatureObjects, got.FeatureCoverage, source.FeatureObjects, source.FeatureCoverage)
		})
	}
}

func coordinationScopeCoverage(c *qt.C, representation schemaext.Representation) schemaext.Coverage {
	c.Helper()
	result, err := ydbcoordination.Coverage(representation, schemaext.Knowledge{State: schemaext.Complete}, []schemaext.SubjectCoverage{
		{Kind: ydbcoordination.Kind, Subject: ydbcoordination.Ref("app", "limited"), Knowledge: schemaext.Knowledge{State: schemaext.Unrepresentable, Reason: "unknown setting"}},
		{Kind: ydbcoordination.Kind, Subject: ydbcoordination.Ref("other", "limited"), Knowledge: schemaext.Knowledge{State: schemaext.Uninspected, Reason: "not inspected"}},
	})
	c.Assert(err, qt.IsNil)
	return result
}

func assertSelectedCoordination(c *qt.C, objects schemaext.Objects, coverage schemaext.Coverage, original schemaext.Objects, originalCoverage schemaext.Coverage) {
	c.Helper()
	c.Assert(objects.Len(), qt.Equals, 1)
	want, ok, err := original.Get(ydbcoordination.Ref("app", "kept"))
	c.Assert(err, qt.IsNil)
	c.Assert(ok, qt.IsTrue)
	c.Assert(must.Must(objects.All()), qt.DeepEquals, []schemaext.Object{want})
	c.Assert(coverage.Representation(), qt.Equals, originalCoverage.Representation())
	c.Assert(coverage.KindRecords(), qt.DeepEquals, originalCoverage.KindRecords())
	c.Assert(coverage.SubjectRecords(), qt.DeepEquals, []schemaext.SubjectCoverage{
		{Kind: ydbcoordination.Kind, Subject: ydbcoordination.Ref("app", "limited"), Knowledge: schemaext.Knowledge{State: schemaext.Unrepresentable, Reason: "unknown setting"}},
	})
	c.Assert(original.Len(), qt.Equals, 2)
	c.Assert(originalCoverage.SubjectRecords(), qt.HasLen, 2)
}
