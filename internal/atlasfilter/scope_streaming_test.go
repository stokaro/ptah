package atlasfilter_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/ydb/ydbstreaming"
	"ptah.run/dialect/ydb/ydbworkload"
	"ptah.run/internal/atlasfilter"
)

func TestScope_StreamingQueriesAndTheirPools(t *testing.T) {
	for _, test := range []struct {
		name     string
		scope    atlasfilter.Scope
		selected string
		pools    []string
	}{
		{name: "include explicit pool", scope: atlasfilter.Scope{Include: []string{"copy[type=streaming_query]"}}, selected: "copy", pools: []string{"batch"}},
		{name: "include default pool", scope: atlasfilter.Scope{Include: []string{"other[type=streaming_query]"}}, selected: "other", pools: []string{"default"}},
		{name: "exclude query", scope: atlasfilter.Scope{Exclude: []string{"copy[type=streaming_query]"}}, selected: "other", pools: []string{"batch", "default", "idle"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			declared := &schemamodel.Database{FeatureObjects: must.Must(schemaext.NewObjects(
				ydbstreaming.DesiredObject("", "copy", "Copy", ydbstreaming.Spec{Text: "SELECT 1;", ResourcePool: "batch"}, true),
				ydbstreaming.DesiredObject("", "other", "Other", ydbstreaming.Spec{Text: "SELECT 2;"}, false),
				ydbworkload.DesiredPoolObject("batch", "", ydbworkload.PoolSpec{}),
				ydbworkload.DesiredPoolObject("default", "", ydbworkload.PoolSpec{}),
				ydbworkload.DesiredPoolObject("idle", "", ydbworkload.PoolSpec{}),
			))}
			held := &catalog.Database{FeatureObjects: must.Must(schemaext.NewObjects(
				ydbstreaming.ObservedObject("", "copy", ydbstreaming.Spec{Text: "SELECT 1;", ResourcePool: "batch"}),
				ydbstreaming.ObservedObject("", "other", ydbstreaming.Spec{Text: "SELECT 2;"}),
				ydbworkload.ObservedPoolObject("batch", ydbworkload.PoolSpec{}),
				ydbworkload.ObservedPoolObject("default", ydbworkload.PoolSpec{}),
				ydbworkload.ObservedPoolObject("idle", ydbworkload.PoolSpec{}),
			))}
			generated, _, err := atlasfilter.ScopeGeneratedSelectionReport(declared, test.scope)
			c.Assert(err, qt.IsNil)
			live, err := atlasfilter.ScopeDatabase(held, test.scope)
			c.Assert(err, qt.IsNil)
			desired, found, err := declared.FeatureObjects.Get(ydbstreaming.Ref("", test.selected))
			c.Assert(err, qt.IsNil)
			c.Assert(found, qt.IsTrue)
			observed, found, err := held.FeatureObjects.Get(ydbstreaming.Ref("", test.selected))
			c.Assert(err, qt.IsNil)
			c.Assert(found, qt.IsTrue)
			c.Assert(must.Must(generated.FeatureObjects.Select(func(ref objectidentity.ID) bool { return ref.Kind == objectidentity.Kind(ydbstreaming.Kind) }).All()), qt.DeepEquals, []schemaext.Object{desired})
			c.Assert(must.Must(live.FeatureObjects.Select(func(ref objectidentity.ID) bool { return ref.Kind == objectidentity.Kind(ydbstreaming.Kind) }).All()), qt.DeepEquals, []schemaext.Object{observed})
			c.Assert(streamingPoolNames(generated.FeatureObjects, live.FeatureObjects), qt.DeepEquals, [2][]string{test.pools, test.pools})
			c.Assert(declared.FeatureObjects.Len(), qt.Equals, 5)
			c.Assert(held.FeatureObjects.Len(), qt.Equals, 5)
		})
	}
}

func streamingPoolNames(desired, observed schemaext.Objects) [2][]string {
	var names [2][]string
	for i, objects := range []schemaext.Objects{desired, observed} {
		pools := objects.Select(func(ref objectidentity.ID) bool { return ref.Kind == objectidentity.Kind(ydbworkload.PoolKind) })
		for _, ref := range pools.Refs() {
			names[i] = append(names[i], ref.Name.Source)
		}
	}
	return names
}

func TestScopeStreamingLimitsFollowObjectSelection(t *testing.T) {
	for _, test := range []struct {
		name  string
		scope atlasfilter.Scope
	}{
		{name: "include", scope: atlasfilter.Scope{Include: []string{"app.*[type=streaming_query]"}}},
		{name: "exclude", scope: atlasfilter.Scope{Exclude: []string{"other.*[type=streaming_query]"}}},
		{name: "schema", scope: atlasfilter.Scope{Schemas: []string{"app"}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			desired := streamingScopeCoverage(c, schemaext.Desired)
			observed := streamingScopeCoverage(c, schemaext.Observed)
			generated, err := atlasfilter.ScopeGenerated(&schemamodel.Database{FeatureCoverage: desired}, test.scope)
			c.Assert(err, qt.IsNil)
			live, err := atlasfilter.ScopeDatabase(&catalog.Database{FeatureCoverage: observed}, test.scope)
			c.Assert(err, qt.IsNil)
			c.Assert(generated.FeatureCoverage.SubjectRecords(), qt.DeepEquals, desired.SubjectRecords()[:1])
			c.Assert(live.FeatureCoverage.SubjectRecords(), qt.DeepEquals, observed.SubjectRecords()[:1])
			c.Assert(generated.FeatureCoverage.KindRecords(), qt.DeepEquals, desired.KindRecords())
			c.Assert(live.FeatureCoverage.KindRecords(), qt.DeepEquals, observed.KindRecords())
			c.Assert(desired.SubjectRecords(), qt.HasLen, 2)
			c.Assert(observed.SubjectRecords(), qt.HasLen, 2)
		})
	}
}

func streamingScopeCoverage(c *qt.C, representation schemaext.Representation) schemaext.Coverage {
	c.Helper()
	return must.Must(ydbstreaming.Coverage(representation, schemaext.Knowledge{State: schemaext.Complete}, []schemaext.SubjectCoverage{
		{Kind: ydbstreaming.Kind, Subject: ydbstreaming.Ref("app", "limited"), Knowledge: schemaext.Knowledge{State: schemaext.Unrepresentable, Reason: "permission denied"}},
		{Kind: ydbstreaming.Kind, Subject: ydbstreaming.Ref("other", "limited"), Knowledge: schemaext.Knowledge{State: schemaext.Uninspected, Reason: "not inspected"}},
	}))
}
