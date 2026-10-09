package schemadiff_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/capability"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/ydb/ydbdiff"
	"ptah.run/dialect/ydb/ydbworkload"
	"ptah.run/engine/builtin"
	"ptah.run/migration/planner"
	"ptah.run/migration/schemadiff"
)

func workloadCoverage(representation schemaext.Representation, knowledge schemaext.Knowledge) schemaext.Coverage {
	pools := must.Must(ydbworkload.Coverage(ydbworkload.PoolKind, representation, knowledge, nil))
	classifiers := must.Must(ydbworkload.Coverage(ydbworkload.ClassifierKind, representation, knowledge, nil))
	return must.Must(pools.Combine(classifiers))
}

// Application tables do not acquire ownership of database-wide pools. Neither
// an observed default pool nor unavailable enumeration authorizes workload DDL.
func TestCompareTablesPreservesUnmanagedWorkloadState(t *testing.T) {
	for _, test := range []struct {
		name    string
		objects []schemaext.Object
	}{
		{name: "unknown namespace"},
		{name: "observed default", objects: []schemaext.Object{ydbworkload.ObservedPoolObject("default", ydbworkload.PoolSpec{})}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			runtime := must.Must(builtin.New())
			info := catalog.ServerInfo{Dialect: "ydb", Capabilities: capability.YDB262().With(capability.ResourcePools, false)}
			desired := &schemamodel.Database{
				Tables:          []schemamodel.Table{{Name: "jobs", StructName: "Job"}},
				Fields:          []schemamodel.Field{{Name: "id", Type: "BIGINT", Primary: true, StructName: "Job"}},
				FeatureCoverage: workloadCoverage(schemaext.Desired, schemaext.Knowledge{State: schemaext.Complete}),
			}
			schemamodel.Finalize(desired)
			current := &catalog.Database{
				FeatureObjects:  must.Must(schemaext.NewObjects(test.objects...)),
				FeatureCoverage: workloadCoverage(schemaext.Observed, schemaext.Knowledge{State: schemaext.Uninspected, Reason: "workload DDL is disabled"}),
			}
			diff, err := schemadiff.CompareWithDatabaseInfo(t.Context(), desired, current, info, nil, runtime)
			c.Assert(err, qt.IsNil)
			c.Assert(diff.FeatureChanges, qt.HasLen, 0)
			statements, err := planner.GenerateSchemaDiffSQLStatementsWithOptions(t.Context(), runtime, diff, "ydb", planner.Options{Capabilities: info.Capabilities})
			c.Assert(err, qt.IsNil)
			c.Assert(statements, qt.HasLen, 1)
			c.Assert(statements[0], qt.Contains, "CREATE TABLE `jobs`")
			c.Assert(desired.FeatureObjects.Len(), qt.Equals, 0)
		})
	}
}

// Database-wide objects omitted by an application remain in the effective
// desired snapshot. Changed objects retain both operands for planning and rollback.
func TestCompare_ResourcePools(t *testing.T) {
	before := ydbworkload.PoolSpec{ConcurrentQueryLimit: new(int32(10))}
	after := ydbworkload.PoolSpec{ConcurrentQueryLimit: new(int32(20))}
	everyone := ydbworkload.ClassifierSpec{ResourcePool: "batch", Rank: 1000}
	redirected := ydbworkload.ClassifierSpec{ResourcePool: "default", Rank: 5}
	for _, test := range []struct {
		name             string
		desired, current []schemaext.Object
		changes          []schemaext.ChangeRecord
	}{
		{
			name:    "the same pool and classifier",
			desired: []schemaext.Object{ydbworkload.DesiredPoolObject("batch", "", before), ydbworkload.DesiredClassifierObject("all", "", everyone)},
			current: []schemaext.Object{ydbworkload.ObservedPoolObject("batch", before), ydbworkload.ObservedClassifierObject("all", everyone)},
		},
		{
			name:    "objects only the declaration has",
			desired: []schemaext.Object{ydbworkload.DesiredPoolObject("batch", "", before), ydbworkload.DesiredClassifierObject("all", "", everyone)},
			changes: []schemaext.ChangeRecord{
				{Subject: ydbworkload.PoolRef("batch"), Value: &ydbdiff.ResourcePool{After: &ydbworkload.DesiredPool{Spec: before}}},
				{Subject: ydbworkload.ClassifierRef("all"), Value: &ydbdiff.ResourcePoolClassifier{After: &ydbworkload.DesiredClassifier{Spec: everyone}}},
			},
		},
		{
			name:    "current-only objects are preserved",
			current: []schemaext.Object{ydbworkload.ObservedPoolObject("batch", before), ydbworkload.ObservedPoolObject("default", ydbworkload.PoolSpec{}), ydbworkload.ObservedClassifierObject("all", everyone)},
		},
		{
			name:    "changed limit and routing",
			desired: []schemaext.Object{ydbworkload.DesiredPoolObject("batch", "", after), ydbworkload.DesiredClassifierObject("all", "", redirected)},
			current: []schemaext.Object{ydbworkload.ObservedPoolObject("batch", before), ydbworkload.ObservedClassifierObject("all", everyone)},
			changes: []schemaext.ChangeRecord{
				{Subject: ydbworkload.PoolRef("batch"), Value: &ydbdiff.ResourcePool{Before: &ydbworkload.ObservedPool{Spec: before}, After: &ydbworkload.DesiredPool{Spec: after}}},
				{Subject: ydbworkload.ClassifierRef("all"), Value: &ydbdiff.ResourcePoolClassifier{Before: &ydbworkload.ObservedClassifier{Spec: everyone}, After: &ydbworkload.DesiredClassifier{Spec: redirected}}},
			},
		},
		{
			name:    "default settings change in place",
			desired: []schemaext.Object{ydbworkload.DesiredPoolObject("default", "", ydbworkload.PoolSpec{ResourceWeight: new(30.0)})},
			current: []schemaext.Object{ydbworkload.ObservedPoolObject("default", ydbworkload.PoolSpec{})},
			changes: []schemaext.ChangeRecord{{Subject: ydbworkload.PoolRef("default"), Value: &ydbdiff.ResourcePool{Before: &ydbworkload.ObservedPool{}, After: &ydbworkload.DesiredPool{Spec: ydbworkload.PoolSpec{ResourceWeight: new(30.0)}}}}},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			desired := &schemamodel.Database{FeatureObjects: must.Must(schemaext.NewObjects(test.desired...)), FeatureCoverage: workloadCoverage(schemaext.Desired, schemaext.Knowledge{State: schemaext.Complete})}
			current := &catalog.Database{FeatureObjects: must.Must(schemaext.NewObjects(test.current...)), FeatureCoverage: workloadCoverage(schemaext.Observed, schemaext.Knowledge{State: schemaext.Complete})}
			captured := desired.FeatureObjects
			diff, err := schemadiff.CompareWithDatabaseInfo(t.Context(), desired, current,
				catalog.ServerInfo{Dialect: "ydb", Capabilities: capability.YDB262().With(capability.ResourcePools, true)}, nil, must.Must(builtin.New()))
			c.Assert(err, qt.IsNil)
			c.Assert(diff.FeatureChanges, qt.DeepEquals, test.changes)
			c.Assert(diff.HasChanges(), qt.Equals, len(test.changes) > 0)
			c.Assert(desired.FeatureObjects, qt.DeepEquals, captured)
		})
	}
}

// Unknown or incomplete namespace inspection cannot establish absence. The
// comparison refuses and reports each declared subject lacking evidence.
func TestCompare_ResourcePools_Coverage(t *testing.T) {
	for _, test := range []struct {
		name     string
		coverage schemaext.Coverage
	}{
		{name: "missing enrollment"},
		{name: "outside realm", coverage: workloadCoverage(schemaext.Observed, schemaext.Knowledge{State: schemaext.Uninspected, Reason: "outside dev realm"})},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			desired := &schemamodel.Database{FeatureCoverage: workloadCoverage(schemaext.Desired, schemaext.Knowledge{State: schemaext.Complete}), FeatureObjects: must.Must(schemaext.NewObjects(
				ydbworkload.DesiredPoolObject("batch", "", ydbworkload.PoolSpec{}),
				ydbworkload.DesiredClassifierObject("all", "", ydbworkload.ClassifierSpec{ResourcePool: "batch", Rank: 1}),
			))}
			current := &catalog.Database{FeatureCoverage: test.coverage}
			refused, err := schemadiff.CompareWithDatabaseInfo(t.Context(), desired, current,
				catalog.ServerInfo{Dialect: "ydb", Capabilities: capability.YDB262().With(capability.ResourcePools, true)}, nil, must.Must(builtin.New()))
			c.Assert(err, qt.ErrorIs, schemadiff.ErrIncompleteComparison)
			c.Assert(refused, qt.IsNil)
			var incomplete *schemadiff.IncompleteComparisonError
			c.Assert(err, qt.ErrorAs, &incomplete)
			c.Assert(incomplete.Diagnostics.Common, qt.HasLen, 0)
			var subjects []objectidentity.ID
			for _, diagnostic := range incomplete.Diagnostics.Features {
				subjects = append(subjects, diagnostic.Subject)
			}
			c.Assert(subjects, qt.DeepEquals, []objectidentity.ID{ydbworkload.PoolRef("batch"), ydbworkload.ClassifierRef("all")})
		})
	}
}
