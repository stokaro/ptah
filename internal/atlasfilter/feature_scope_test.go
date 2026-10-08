package atlasfilter_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/ydb/ydbschema"
	"ptah.run/internal/atlasfilter"
)

var featureScopeCases = []struct {
	name  string
	scope atlasfilter.Scope
}{
	{name: "include", scope: atlasfilter.Scope{Include: []string{"app.kept"}}},
	{name: "exclude", scope: atlasfilter.Scope{Exclude: []string{"other.dropped"}}},
	{name: "schema", scope: atlasfilter.Scope{Schemas: []string{"app"}}},
}

func TestScopeDatabasePreservesSelectedFeatureChildren(t *testing.T) {
	for _, test := range featureScopeCases {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			source := &catalog.Database{
				Tables: []catalog.Table{{Schema: "app", Name: "kept"}, {Schema: "other", Name: "dropped"}},
				FeatureObjects: must.Must(schemaext.NewObjects(
					ydbschema.ObservedObject("app", "kept", ydbschema.ChangefeedSpec{Name: "updates", Mode: "NEW_IMAGE", Format: "JSON"}),
					ydbschema.ObservedObject("other", "dropped", ydbschema.ChangefeedSpec{Name: "updates", Mode: "KEYS_ONLY", Format: "JSON"}),
				)),
				FeatureCoverage: featureScopeCoverage(c, schemaext.Observed),
			}
			got, err := atlasfilter.ScopeDatabase(source, test.scope)
			c.Assert(err, qt.IsNil)
			assertSelectedFeatures(c, got.FeatureObjects, got.FeatureCoverage, source.FeatureObjects, source.FeatureCoverage)
		})
	}
}

func TestScopeGeneratedPreservesSelectedFeatureChildren(t *testing.T) {
	for _, test := range featureScopeCases {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			source := &schemamodel.Database{
				Tables: []schemamodel.Table{{Schema: "app", Name: "kept", StructName: "Kept"}, {Schema: "other", Name: "dropped", StructName: "Dropped"}},
				FeatureObjects: must.Must(schemaext.NewObjects(
					ydbschema.DesiredObject("app", "kept", ydbschema.ChangefeedSpec{Name: "updates", Mode: "NEW_IMAGE", Format: "JSON"}),
					ydbschema.DesiredObject("other", "dropped", ydbschema.ChangefeedSpec{Name: "updates", Mode: "KEYS_ONLY", Format: "JSON"}),
				)),
				FeatureCoverage: featureScopeCoverage(c, schemaext.Desired),
			}
			got, err := atlasfilter.ScopeGenerated(source, test.scope)
			c.Assert(err, qt.IsNil)
			assertSelectedFeatures(c, got.FeatureObjects, got.FeatureCoverage, source.FeatureObjects, source.FeatureCoverage)
		})
	}
}

func TestScopeDatabasePreservesUninspectedFeatureNamespace(t *testing.T) {
	c := qt.New(t)
	definition := must.Must(ydbschema.ChangefeedCoverage(schemaext.Observed, nil)).KindRecords()
	definition[0].Knowledge = schemaext.Knowledge{State: schemaext.Uninspected, Reason: "source did not inspect streams"}
	source := &catalog.Database{
		Tables:          []catalog.Table{{Schema: "app", Name: "kept"}},
		FeatureCoverage: must.Must(schemaext.NewCoverage(schemaext.Observed, definition, nil)),
	}
	got, err := atlasfilter.ScopeDatabase(source, atlasfilter.Scope{Schemas: []string{"app"}})
	c.Assert(err, qt.IsNil)
	c.Assert(got.FeatureCoverage.Representation(), qt.Equals, schemaext.Observed)
	c.Assert(got.FeatureCoverage.KindRecords(), qt.DeepEquals, definition)
	c.Assert(got.FeatureObjects.Len(), qt.Equals, 0)
}

func featureScopeCoverage(c *qt.C, representation schemaext.Representation) schemaext.Coverage {
	c.Helper()
	builder := objectidentity.NewBuilder(identifier.ForDialect("ydb"))
	coverage, err := ydbschema.ChangefeedCoverage(representation, []schemaext.SubjectCoverage{
		{Kind: ydbschema.ChangefeedKind, Subject: builder.TableParts("app", "kept"), Knowledge: schemaext.Knowledge{State: schemaext.Complete}},
		{Kind: ydbschema.ChangefeedKind, Subject: ydbschema.ChangefeedRef("app", "kept", "limited"), Knowledge: schemaext.Knowledge{State: schemaext.Unrepresentable, Reason: "unknown retained setting"}},
		{Kind: ydbschema.ChangefeedKind, Subject: ydbschema.ChangefeedRef("other", "dropped", "limited"), Knowledge: schemaext.Knowledge{State: schemaext.Uninspected, Reason: "excluded owner"}},
	})
	c.Assert(err, qt.IsNil)
	return coverage
}

func assertSelectedFeatures(c *qt.C, objects schemaext.Objects, coverage schemaext.Coverage, sourceObjects schemaext.Objects, sourceCoverage schemaext.Coverage) {
	c.Helper()
	parent := objectidentity.NewBuilder(identifier.ForDialect("ydb")).TableParts("app", "kept")
	c.Assert(must.Must(objects.All()), qt.DeepEquals, must.Must(sourceObjects.ForParent(parent).All()))
	c.Assert(coverage.Representation(), qt.Equals, sourceCoverage.Representation())
	c.Assert(coverage.KindRecords(), qt.DeepEquals, sourceCoverage.KindRecords())
	c.Assert(coverage.SubjectRecords(), qt.DeepEquals, sourceCoverage.ForParent(parent).SubjectRecords())
	c.Assert(sourceObjects.Len(), qt.Equals, 2)
	c.Assert(sourceCoverage.SubjectRecords(), qt.HasLen, 3)
}
