package builtin_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/platform/capability"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbworkload"
	"ptah.run/engine/builtin"
	"ptah.run/internal/convert/goschematodb"
	"ptah.run/internal/sqlschema"
	"ptah.run/migration/planner"
	"ptah.run/migration/schemadiff"
)

func TestWorkloadFeatureSchemaUsesSelectedOwnerOnEveryEntryPoint(t *testing.T) {
	for _, test := range []struct {
		name   string
		object schemaext.Object
		sql    string
	}{
		{name: "pool", object: ydbworkload.DesiredPoolObject("batch.jobs", "", ydbworkload.PoolSpec{ConcurrentQueryLimit: new(int32(0))}), sql: "CREATE RESOURCE POOL `batch.jobs` WITH (CONCURRENT_QUERY_LIMIT = 0);"},
		{name: "classifier", object: ydbworkload.DesiredClassifierObject("route.jobs", "", ydbworkload.ClassifierSpec{ResourcePool: "default", Rank: 0}), sql: "CREATE RESOURCE POOL CLASSIFIER `route.jobs` WITH (RESOURCE_POOL = 'default', RANK = 0);"},
		{name: "default settings", object: ydbworkload.DesiredPoolObject("default", "", ydbworkload.PoolSpec{ResourceWeight: new(0.0)}), sql: "ALTER RESOURCE POOL `default` SET (RESOURCE_WEIGHT = 0);"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			database := coordinationFeatureSchema(c, "unused")
			database.FeatureObjects = must.Must(schemaext.NewObjects(test.object))
			assertOwnedSchemaEntryPoints(c, database, capability.YDB262().With(capability.ResourcePools, true), test.sql)
		})
	}
}

func TestWorkloadSourceComparisonAndRankSwapReachRegisteredPlanner(t *testing.T) {
	c := qt.New(t)
	runtime := must.Must(builtin.New())
	caps := capability.YDB262().With(capability.ResourcePools, true)
	before := "CREATE RESOURCE POOL batch WITH (CONCURRENT_QUERY_LIMIT = 5); " +
		"CREATE RESOURCE POOL CLASSIFIER alpha WITH (RESOURCE_POOL = 'batch', RANK = 10); " +
		"CREATE RESOURCE POOL CLASSIFIER beta WITH (RESOURCE_POOL = 'batch', RANK = 20);"
	database, _, err := sqlschema.Read([]byte(before), "ydb")
	c.Assert(err, qt.IsNil)
	current, err := goschematodb.ToDBSchema(t.Context(), &database, "ydb", runtime)
	c.Assert(err, qt.IsNil)
	info := catalog.ServerInfo{Dialect: "ydb", Capabilities: caps}
	unchanged, err := schemadiff.CompareWithDatabaseInfo(t.Context(), &database, current, info, nil, runtime)
	c.Assert(err, qt.IsNil)
	c.Assert(unchanged.HasChanges(), qt.IsFalse)
	after := strings.ReplaceAll(before, "RANK = 10", "RANK = 30")
	after = strings.ReplaceAll(after, "RANK = 20", "RANK = 10")
	after = strings.ReplaceAll(after, "RANK = 30", "RANK = 20")
	desired, _, err := sqlschema.Read([]byte(after), "ydb")
	c.Assert(err, qt.IsNil)
	diff, err := schemadiff.CompareWithDatabaseInfo(t.Context(), &desired, current, info, nil, runtime)
	c.Assert(err, qt.IsNil)
	c.Assert(diff.FeatureChanges, qt.HasLen, 2)
	statements, err := planner.GenerateSchemaDiffSQLStatementsWithOptions(t.Context(), runtime, diff, "ydb", planner.Options{Capabilities: caps})
	c.Assert(err, qt.IsNil)
	c.Assert(statements, qt.DeepEquals, []string{
		"DROP RESOURCE POOL CLASSIFIER `alpha`",
		"DROP RESOURCE POOL CLASSIFIER `beta`",
		"CREATE RESOURCE POOL CLASSIFIER `alpha` WITH (RESOURCE_POOL = 'batch', RANK = 20)",
		"CREATE RESOURCE POOL CLASSIFIER `beta` WITH (RESOURCE_POOL = 'batch', RANK = 10)",
	})
	unchanged, err = schemadiff.CompareWithDatabaseInfo(t.Context(), &database, current, info, nil, runtime)
	c.Assert(err, qt.IsNil)
	c.Assert(unchanged.HasChanges(), qt.IsFalse)
}
