package generator_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/platform/capability"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/ydb/ydbdiff"
	"ptah.run/dialect/ydb/ydbworkload"
	"ptah.run/engine/builtin"
	"ptah.run/migration/generator"
	"ptah.run/migration/schemadiff"
)

func workloadStateCoverage(representation schemaext.Representation) schemaext.Coverage {
	knowledge := schemaext.Knowledge{State: schemaext.Complete}
	pools := must.Must(ydbworkload.Coverage(ydbworkload.PoolKind, representation, knowledge, nil))
	classifiers := must.Must(ydbworkload.Coverage(ydbworkload.ClassifierKind, representation, knowledge, nil))
	return must.Must(pools.Combine(classifiers))
}

// The public generator must select owner reversal, preserve nil versus zero
// settings, release occupied ranks in both directions, and report workload
// recovery limits. Independent models prove the comparison snapshots survive.
func TestResourcePoolsBidirectionalPlanUsesOwnedChanges(t *testing.T) {
	poolBefore := ydbworkload.PoolSpec{ConcurrentQueryLimit: new(int32(5)), QueueSize: new(int32(9))}
	poolAfter := ydbworkload.PoolSpec{ConcurrentQueryLimit: new(int32(0))}
	alpha := ydbworkload.ClassifierSpec{ResourcePool: "batch", Rank: 10}
	beta := ydbworkload.ClassifierSpec{ResourcePool: "batch", Rank: 20}
	for _, test := range []struct {
		name             string
		current, desired []schemaext.Object
		forward, reverse string
		changes          int
	}{
		{
			name:    "creation rollback drops classifier before pool",
			desired: []schemaext.Object{ydbworkload.DesiredPoolObject("batch", "", poolBefore), ydbworkload.DesiredClassifierObject("alpha", "", alpha)},
			forward: "CREATE RESOURCE POOL `batch` WITH (CONCURRENT_QUERY_LIMIT = 5, QUEUE_SIZE = 9);\n" +
				"CREATE RESOURCE POOL CLASSIFIER `alpha` WITH (RESOURCE_POOL = 'batch', RANK = 10);\n",
			reverse: "DROP RESOURCE POOL CLASSIFIER `alpha`;\nDROP RESOURCE POOL `batch`;\n", changes: 2,
		},
		{
			name:    "limit reset and rank swap restore captured settings",
			current: []schemaext.Object{ydbworkload.ObservedPoolObject("batch", poolBefore), ydbworkload.ObservedClassifierObject("alpha", alpha), ydbworkload.ObservedClassifierObject("beta", beta)},
			desired: []schemaext.Object{ydbworkload.DesiredPoolObject("batch", "", poolAfter), ydbworkload.DesiredClassifierObject("alpha", "", beta), ydbworkload.DesiredClassifierObject("beta", "", alpha)},
			forward: "DROP RESOURCE POOL CLASSIFIER `alpha`;\nDROP RESOURCE POOL CLASSIFIER `beta`;\n" +
				"ALTER RESOURCE POOL `batch` SET (CONCURRENT_QUERY_LIMIT = 0), RESET (QUEUE_SIZE);\n" +
				"CREATE RESOURCE POOL CLASSIFIER `alpha` WITH (RESOURCE_POOL = 'batch', RANK = 20);\n" +
				"CREATE RESOURCE POOL CLASSIFIER `beta` WITH (RESOURCE_POOL = 'batch', RANK = 10);\n",
			reverse: "DROP RESOURCE POOL CLASSIFIER `alpha`;\nDROP RESOURCE POOL CLASSIFIER `beta`;\n" +
				"ALTER RESOURCE POOL `batch` SET (CONCURRENT_QUERY_LIMIT = 5, QUEUE_SIZE = 9);\n" +
				"CREATE RESOURCE POOL CLASSIFIER `alpha` WITH (RESOURCE_POOL = 'batch', RANK = 10);\n" +
				"CREATE RESOURCE POOL CLASSIFIER `beta` WITH (RESOURCE_POOL = 'batch', RANK = 20);\n", changes: 3,
		},
		{
			name:    "default settings restore an explicit zero",
			current: []schemaext.Object{ydbworkload.ObservedPoolObject("default", ydbworkload.PoolSpec{ResourceWeight: new(0.0)})},
			desired: []schemaext.Object{ydbworkload.DesiredPoolObject("default", "", ydbworkload.PoolSpec{})},
			forward: "ALTER RESOURCE POOL `default` RESET (RESOURCE_WEIGHT);\n",
			reverse: "ALTER RESOURCE POOL `default` SET (RESOURCE_WEIGHT = 0);\n", changes: 1,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			runtime := must.Must(builtin.New())
			caps := capability.YDB262().With(capability.ResourcePools, true)
			current := &catalog.Database{FeatureObjects: must.Must(schemaext.NewObjects(test.current...)), FeatureCoverage: workloadStateCoverage(schemaext.Observed)}
			desired := &schemamodel.Database{FeatureObjects: must.Must(schemaext.NewObjects(test.desired...)), FeatureCoverage: workloadStateCoverage(schemaext.Desired)}
			diff, err := schemadiff.CompareWithDatabaseInfo(t.Context(), desired, current, catalog.ServerInfo{Dialect: "ydb", Capabilities: caps}, nil, runtime)
			c.Assert(err, qt.IsNil)
			captured, err := runtime.Codecs().SnapshotChanges(t.Context(), diff.FeatureChanges)
			c.Assert(err, qt.IsNil)
			plan, err := generator.PlanBidirectionalSchemaDiff(t.Context(), generator.BidirectionalSchemaPlanOptions{
				Runtime: runtime, Diff: diff, DesiredSchema: desired, CurrentSchema: current, Dialect: "ydb", Capabilities: caps,
			})
			c.Assert(err, qt.IsNil)
			forward, err := builtin.RenderSQLWithCapabilities("ydb", caps, plan.Forward.Nodes...)
			c.Assert(err, qt.IsNil)
			reverse, err := builtin.RenderSQLWithCapabilities("ydb", caps, plan.Reverse.Nodes...)
			c.Assert(err, qt.IsNil)
			c.Assert(forward, qt.Equals, test.forward)
			c.Assert(reverse, qt.Contains, test.reverse)
			c.Assert(reverse, qt.Contains, "cannot undo queries run, queued, or rejected")
			c.Assert(plan.Reverse.Recovery, qt.HasLen, test.changes)
			c.Assert(diff.FeatureChanges, qt.DeepEquals, captured)
			// Editing a reverse operand cannot rewrite the accepted forward snapshot.
			restored := plan.Reverse.Diff.FeatureChanges[0].Value.(*ydbdiff.ResourcePool)
			restored.Before.Spec.ResourceWeight = new(99.0)
			c.Assert(diff.FeatureChanges, qt.DeepEquals, captured)
		})
	}
}
