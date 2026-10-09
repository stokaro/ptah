package atlasfilter_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/ydb/ydbworkload"
	"ptah.run/internal/atlasfilter"
)

// Include retains a captured destination pool. Exclusion remains authoritative,
// and schema selection cannot reinterpret a dot in a database-wide name.
func TestScopeWorkloadObjects(t *testing.T) {
	for _, test := range []struct {
		name  string
		scope atlasfilter.Scope
		refs  []objectidentity.ID
	}{
		{name: "classifier retains its pool", scope: atlasfilter.Scope{Include: []string{"etl_users[type=resource_pool_classifier]"}}, refs: []objectidentity.ID{ydbworkload.PoolRef("batch.jobs"), ydbworkload.ClassifierRef("etl_users")}},
		{name: "pool does not pull in classifiers", scope: atlasfilter.Scope{Include: []string{"batch.jobs[type=resource_pool]"}}, refs: []objectidentity.ID{ydbworkload.PoolRef("batch.jobs")}},
		{name: "explicit pool exclusion", scope: atlasfilter.Scope{Include: []string{"etl_users[type=resource_pool_classifier]"}, Exclude: []string{"batch.jobs[type=resource_pool]"}}, refs: []objectidentity.ID{ydbworkload.ClassifierRef("etl_users")}},
		{name: "classifier exclusion", scope: atlasfilter.Scope{Exclude: []string{"*[type=resource_pool_classifier]"}}, refs: []objectidentity.ID{ydbworkload.PoolRef("batch.jobs"), ydbworkload.PoolRef("reports")}},
		{name: "database scope survives schema restriction", scope: atlasfilter.Scope{Schemas: []string{"unrelated"}, Include: []string{"etl_users[type=resource_pool_classifier]"}}, refs: []objectidentity.ID{ydbworkload.PoolRef("batch.jobs"), ydbworkload.ClassifierRef("etl_users")}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			declared := &schemamodel.Database{FeatureObjects: must.Must(schemaext.NewObjects(
				ydbworkload.DesiredPoolObject("batch.jobs", "Pool", ydbworkload.PoolSpec{ConcurrentQueryLimit: new(int32(0))}),
				ydbworkload.DesiredPoolObject("reports", "Reports", ydbworkload.PoolSpec{}),
				ydbworkload.DesiredClassifierObject("etl_users", "ETL", ydbworkload.ClassifierSpec{ResourcePool: "batch.jobs", Rank: 10}),
				ydbworkload.DesiredClassifierObject("analysts", "Analysts", ydbworkload.ClassifierSpec{ResourcePool: "reports", Rank: 20}),
			))}
			held := &catalog.Database{FeatureObjects: must.Must(schemaext.NewObjects(
				ydbworkload.ObservedPoolObject("batch.jobs", ydbworkload.PoolSpec{ConcurrentQueryLimit: new(int32(0))}),
				ydbworkload.ObservedPoolObject("reports", ydbworkload.PoolSpec{}),
				ydbworkload.ObservedClassifierObject("etl_users", ydbworkload.ClassifierSpec{ResourcePool: "batch.jobs", Rank: 10}),
				ydbworkload.ObservedClassifierObject("analysts", ydbworkload.ClassifierSpec{ResourcePool: "reports", Rank: 20}),
			))}
			generated, err := atlasfilter.ScopeGenerated(declared, test.scope)
			c.Assert(err, qt.IsNil)
			live, err := atlasfilter.ScopeDatabase(held, test.scope)
			c.Assert(err, qt.IsNil)
			c.Assert(generated.FeatureObjects.Refs(), qt.DeepEquals, test.refs)
			c.Assert(live.FeatureObjects.Refs(), qt.DeepEquals, test.refs)
			for _, ref := range test.refs {
				wantDesired, _, err := declared.FeatureObjects.Get(ref)
				c.Assert(err, qt.IsNil)
				gotDesired, _, err := generated.FeatureObjects.Get(ref)
				c.Assert(err, qt.IsNil)
				c.Assert(gotDesired, qt.DeepEquals, wantDesired)
				wantObserved, _, err := held.FeatureObjects.Get(ref)
				c.Assert(err, qt.IsNil)
				gotObserved, _, err := live.FeatureObjects.Get(ref)
				c.Assert(err, qt.IsNil)
				c.Assert(gotObserved, qt.DeepEquals, wantObserved)
			}
			c.Assert(declared.FeatureObjects.Len(), qt.Equals, 4)
			c.Assert(held.FeatureObjects.Len(), qt.Equals, 4)
		})
	}
}

func TestScopeWorkloadLimitsFollowExactObjectNames(t *testing.T) {
	for _, kind := range []schemaext.Kind{ydbworkload.PoolKind, ydbworkload.ClassifierKind} {
		for _, scope := range []atlasfilter.Scope{
			{Include: []string{"batch.jobs"}},
			{Exclude: []string{"other"}},
		} {
			c := qt.New(t)
			identity := map[schemaext.Kind]func(string) objectidentity.ID{ydbworkload.PoolKind: ydbworkload.PoolRef, ydbworkload.ClassifierKind: ydbworkload.ClassifierRef}[kind]
			subjects := []schemaext.SubjectCoverage{
				{Kind: kind, Subject: identity("batch.jobs"), Knowledge: schemaext.Knowledge{State: schemaext.Unrepresentable, Reason: "unsupported setting"}},
				{Kind: kind, Subject: identity("other"), Knowledge: schemaext.Knowledge{State: schemaext.Uninspected, Reason: "not inspected"}},
			}
			desired := must.Must(ydbworkload.Coverage(kind, schemaext.Desired, schemaext.Knowledge{State: schemaext.Uninspected, Reason: "partial source"}, subjects))
			observed := must.Must(ydbworkload.Coverage(kind, schemaext.Observed, schemaext.Knowledge{State: schemaext.Uninspected, Reason: "partial read"}, subjects))
			generated, err := atlasfilter.ScopeGenerated(&schemamodel.Database{FeatureCoverage: desired}, scope)
			c.Assert(err, qt.IsNil)
			live, err := atlasfilter.ScopeDatabase(&catalog.Database{FeatureCoverage: observed}, scope)
			c.Assert(err, qt.IsNil)
			c.Assert(generated.FeatureCoverage.SubjectRecords(), qt.DeepEquals, subjects[:1])
			c.Assert(live.FeatureCoverage.SubjectRecords(), qt.DeepEquals, subjects[:1])
			c.Assert(generated.FeatureCoverage.KindRecords(), qt.DeepEquals, desired.KindRecords())
			c.Assert(live.FeatureCoverage.KindRecords(), qt.DeepEquals, observed.KindRecords())
		}
	}
}
