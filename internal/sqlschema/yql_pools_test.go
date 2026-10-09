package sqlschema_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform/capability"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbcoordination"
	"ptah.run/dialect/ydb/ydbworkload"
	"ptah.run/engine/builtin"
	"ptah.run/internal/sqlschema"
)

func TestReadYQLResourcePools(t *testing.T) {
	c := qt.New(t)
	database, _, err := sqlschema.Read([]byte("CREATE RESOURCE POOL batch WITH (CONCURRENT_QUERY_LIMIT = 4, QUEUE_SIZE = 8, QUERY_MEMORY_LIMIT_PERCENT_PER_NODE = '12.5'); CREATE RESOURCE POOL idle WITH (CONCURRENT_QUERY_LIMIT = '-1'); CREATE RESOURCE POOL CLASSIFIER to_batch WITH (RESOURCE_POOL = 'batch', RANK = 20, MEMBER_NAME = 'worker');"), "ydb")
	c.Assert(err, qt.IsNil)
	objects, err := database.FeatureObjects.All()
	c.Assert(err, qt.IsNil)
	c.Assert(objects, qt.ContentEquals, []schemaext.Object{
		ydbworkload.DesiredPoolObject("batch", "", ydbworkload.PoolSpec{ConcurrentQueryLimit: new(int32(4)), QueueSize: new(int32(8)), QueryMemoryLimitPercentPerNode: new(12.5)}),
		ydbworkload.DesiredPoolObject("idle", "", ydbworkload.PoolSpec{}),
		ydbworkload.DesiredClassifierObject("to_batch", "", ydbworkload.ClassifierSpec{ResourcePool: "batch", Rank: 20, MemberName: "worker"}),
	})
}

func TestReadYQLCoordinationNode(t *testing.T) {
	c := qt.New(t)
	database, _, err := sqlschema.Read([]byte("CREATE COORDINATION NODE `app/locks.v1` WITH (self_check_period = Interval('PT0.5S'), attach_consistency_mode = 'strict');"), "ydb")
	c.Assert(err, qt.IsNil)
	objects, err := database.FeatureObjects.All()
	c.Assert(err, qt.IsNil)
	c.Assert(objects, qt.DeepEquals, []schemaext.Object{ydbcoordination.DesiredObject("app", "locks.v1", "", ydbcoordination.Spec{SelfCheckPeriodMillis: 500, AttachConsistencyMode: "strict"})})
}

func TestReadYQLResourceRefusals(t *testing.T) {
	for _, text := range []string{
		"CREATE COORDINATION NODE n WITH (unknown = 1);",
		"CREATE COORDINATION NODE n WITH (self_check_period = Interval('PT0.0001S'));",
		"CREATE COORDINATION NODE n WITH (read_consistency_mode = 'unknown');",
		"CREATE COORDINATION NODE `/local/n`;",
		"CREATE RESOURCE POOL p;",
		"ALTER RESOURCE POOL p SET (CONCURRENT_QUERY_LIMIT = 4);",
		"ALTER RESOURCE POOL default RESET (RESOURCE_WEIGHT);",
		"CREATE RESOURCE POOL p WITH (unknown = '-1');",
		"CREATE RESOURCE POOL p WITH (name = 'other');",
		"CREATE RESOURCE POOL p WITH (CONCURRENT_QUERY_LIMIT = -1);",
		"CREATE RESOURCE POOL p WITH (QUEUE_SIZE = 8);",
		"CREATE RESOURCE POOL p WITH (QUERY_MEMORY_LIMIT_PERCENT_PER_NODE = 101);",
		"CREATE RESOURCE POOL CLASSIFIER c WITH (RESOURCE_POOL = 'p');",
		"CREATE RESOURCE POOL CLASSIFIER c WITH (RANK = 20);",
		"CREATE RESOURCE POOL CLASSIFIER c WITH (RESOURCE_POOL = 'p', RANK = 20, unknown = 'x');",
	} {
		t.Run(text, func(t *testing.T) {
			c := qt.New(t)
			database, statements, err := sqlschema.Read([]byte(text), "ydb")
			c.Assert(err, qt.ErrorMatches, "YQL schema at position .*")
			c.Assert(statements, qt.IsNil)
			c.Assert(database.FeatureObjects.Len(), qt.Equals, 0)
		})
	}
}

func TestReadYQLResourcePoolsRoundTrip(t *testing.T) {
	c := qt.New(t)
	source := "ALTER RESOURCE POOL default SET (RESOURCE_WEIGHT = 30); CREATE RESOURCE POOL batch WITH (CONCURRENT_QUERY_LIMIT = 4, QUEUE_SIZE = 8); CREATE RESOURCE POOL idle WITH (CONCURRENT_QUERY_LIMIT = '-1'); CREATE RESOURCE POOL CLASSIFIER worker WITH (RESOURCE_POOL = 'batch', RANK = 20, MEMBER_NAME = 'worker');"
	database, _, err := sqlschema.Read([]byte(source), "ydb")
	c.Assert(err, qt.IsNil)
	want, err := database.FeatureObjects.All()
	c.Assert(err, qt.IsNil)
	for _, caps := range []capability.Capabilities{capability.YDB251(), capability.YDB262()} {
		statements, renderErr := builtin.GetOrderedCreateStatementsWithCapabilities(&database, "ydb", caps.With(capability.ResourcePools, true))
		c.Assert(renderErr, qt.IsNil)
		again, _, readErr := sqlschema.Read([]byte(strings.Join(statements, "\n")), "ydb")
		c.Assert(readErr, qt.IsNil)
		c.Assert(again.FeatureObjects.Len(), qt.Equals, 4)
		objects, err := again.FeatureObjects.All()
		c.Assert(err, qt.IsNil)
		c.Assert(objects, qt.DeepEquals, want)
	}
}
