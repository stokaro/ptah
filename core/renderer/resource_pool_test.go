package renderer_test

import (
	"regexp"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/core/renderer"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/ydbpool"
)

// poolNodes are the six resource pool and classifier statements, as nodes,
// with the subject the renderer's central check names each by.
var poolNodes = []struct {
	node    ast.Node
	subject string
}{
	{node: ast.NewCreateResourcePool("batch", ast.ResourcePoolSpec{}), subject: "resource pool batch"},
	{node: ast.NewAlterResourcePool("batch", ast.ResourcePoolSpec{}, ast.ResourcePoolSpec{}),
		subject: "ALTER RESOURCE POOL batch"},
	{node: ast.NewDropResourcePool("batch"), subject: "DROP RESOURCE POOL batch"},
	{node: ast.NewCreateResourcePoolClassifier("c", ast.ResourcePoolClassifierSpec{ResourcePool: "batch"}),
		subject: "resource pool classifier c"},
	{node: ast.NewAlterResourcePoolClassifier("c", ast.ResourcePoolClassifierSpec{ResourcePool: "batch"},
		ast.ResourcePoolClassifierSpec{ResourcePool: "default"}), subject: "ALTER RESOURCE POOL CLASSIFIER c"},
	{node: ast.NewDropResourcePoolClassifier("c"), subject: "DROP RESOURCE POOL CLASSIFIER c"},
}

// poolSchema declares a pool, the pool default's settings and a classifier.
func poolSchema() *schemamodel.Database {
	return &schemamodel.Database{
		ResourcePools: []schemamodel.ResourcePool{
			{Name: "batch", Spec: ast.ResourcePoolSpec{ConcurrentQueryLimit: new(int32(10)), QueueSize: new(int32(5))}},
			{Name: "default", Spec: ast.ResourcePoolSpec{ResourceWeight: new(30.0)}},
		},
		ResourcePoolClassifiers: []schemamodel.ResourcePoolClassifier{{
			Name: "etl_users", Spec: ast.ResourcePoolClassifierSpec{ResourcePool: "batch", MemberName: "etl", Rank: 10},
		}},
	}
}

// TestRender_ResourcePool_HappyPath writes a declared pool, the pool default
// and a classifier on a YDB cluster whose EnableResourcePools flag is on. The
// pool default exists on every such cluster, so its declaration is a change of
// its settings rather than a creation.
func TestRender_ResourcePool_HappyPath(t *testing.T) {
	c := qt.New(t)

	statements, err := renderer.GetOrderedCreateStatementsWithCapabilities(poolSchema(), platform.YDB,
		capability.YDB262().With(capability.ResourcePools, true))

	c.Assert(err, qt.IsNil)
	c.Assert(statements, qt.DeepEquals, []string{
		"CREATE RESOURCE POOL `batch` WITH (CONCURRENT_QUERY_LIMIT = 10, QUEUE_SIZE = 5);\n",
		"ALTER RESOURCE POOL `default` SET (RESOURCE_WEIGHT = 30);\n",
		"CREATE RESOURCE POOL CLASSIFIER `etl_users` WITH (RESOURCE_POOL = 'batch', RANK = 10, MEMBER_NAME = 'etl');\n",
	})
}

// TestRender_ResourcePool_FailurePath refuses a pool on every target without
// resource_pools, through the whole-schema render and each node alike: built as
// nothing, the declaration would report a pool applied that the target does
// not have.
func TestRender_ResourcePool_FailurePath(t *testing.T) {
	tests := []struct {
		dialect string
		caps    capability.Capabilities
	}{
		{dialect: platform.Postgres, caps: capability.Postgres18()},
		{dialect: platform.MySQL, caps: capability.MySQL84()},
		{dialect: platform.MariaDB, caps: capability.MariaDB1011()},
		{dialect: platform.SQLite, caps: capability.SQLite3()},
		{dialect: platform.ClickHouse, caps: capability.ClickHouse24()},
		{dialect: platform.SQLServer, caps: capability.SQLServer2022()},
		{dialect: platform.Oracle, caps: capability.Oracle23()},
	}
	for _, test := range tests {
		t.Run(test.dialect, func(t *testing.T) {
			c := qt.New(t)

			statements, err := renderer.GetOrderedCreateStatementsWithCapabilities(poolSchema(), test.dialect, test.caps)
			c.Assert(err, qt.ErrorMatches,
				`resource pool "batch", which requires target capability resource_pools, unavailable on this \w+ target`)
			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(statements, qt.IsNil)

			for _, node := range poolNodes {
				sql, err := renderer.RenderSQLWithCapabilities(test.dialect, test.caps, node.node)
				c.Assert(err, qt.ErrorMatches,
					node.subject+`, which requires target capability resource_pools, unavailable on this \w+ target`)
				c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
				c.Assert(sql, qt.Equals, "")
			}
		})
	}
}

// Every YDB preset says resource_pools is false, because EnableResourcePools
// is off by default on every line, and the refusal says how a cluster turns
// it on.
func TestRender_ResourcePool_YDBWithoutTheFlag(t *testing.T) {
	for _, test := range []struct {
		name   string
		preset func() capability.Capabilities
	}{{"26.2", capability.YDB262}, {"25.1", capability.YDB251}} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			statements, err := renderer.GetOrderedCreateStatementsWithCapabilities(poolSchema(), platform.YDB,
				test.preset())
			c.Assert(err, qt.ErrorMatches, `resource pool "batch", which requires target capability resource_pools, `+
				`unavailable on this ydb target; `+regexp.QuoteMeta(ydbpool.FlagHint))
			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(statements, qt.IsNil)
		})
	}
}

// A declaration YDB would take and run differently from what it says is
// refused before any statement, on a cluster that holds the key.
func TestRender_ResourcePool_RefusesWhatYDBKeepsDifferently(t *testing.T) {
	tests := []struct {
		name    string
		schema  *schemamodel.Database
		wantErr string
	}{
		{
			name: "two classifiers on one rank",
			schema: &schemamodel.Database{ResourcePoolClassifiers: []schemamodel.ResourcePoolClassifier{
				{Name: "a", Spec: ast.ResourcePoolClassifierSpec{ResourcePool: "default", Rank: 1}},
				{Name: "b", Spec: ast.ResourcePoolClassifierSpec{ResourcePool: "default", Rank: 1}},
			}},
			wantErr: `resource pool classifier "b": its rank 1 is the rank of classifier "a", and YDB keeps one ` +
				`classifier per rank`,
		},
		{
			name: "a classifier naming a pool nobody declared",
			schema: &schemamodel.Database{ResourcePoolClassifiers: []schemamodel.ResourcePoolClassifier{
				{Name: "a", Spec: ast.ResourcePoolClassifierSpec{ResourcePool: "batch", Rank: 1}},
			}},
			wantErr: `resource pool classifier "a": it names resource pool "batch", which is not declared; .*`,
		},
		{
			name: "a pool built by hand with a queue and nothing to wait for",
			schema: &schemamodel.Database{ResourcePools: []schemamodel.ResourcePool{
				{Name: "batch", Spec: ast.ResourcePoolSpec{QueueSize: new(int32(3))}},
			}},
			wantErr: `resource pool "batch": a queue needs concurrent_query_limit or database_load_cpu_threshold .*`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			statements, err := renderer.GetOrderedCreateStatementsWithCapabilities(test.schema, platform.YDB,
				capability.YDB262().With(capability.ResourcePools, true))
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(statements, qt.IsNil)
		})
	}
}

// The YDB renderer writes each node, and refuses a drop of the pool default,
// which YDB takes and then runs no query of the database.
func TestRender_ResourcePoolNodes_YDB(t *testing.T) {
	caps := capability.YDB262().With(capability.ResourcePools, true)
	tests := []struct {
		name string
		node ast.Node
		want string
	}{
		{name: "alter", node: ast.NewAlterResourcePool("batch",
			ast.ResourcePoolSpec{ConcurrentQueryLimit: new(int32(20))},
			ast.ResourcePoolSpec{ConcurrentQueryLimit: new(int32(10)), QueueSize: new(int32(5))}),
			want: "ALTER RESOURCE POOL `batch` SET (CONCURRENT_QUERY_LIMIT = 20), RESET (QUEUE_SIZE);\n"},
		{name: "an alter that changes nothing writes nothing", node: ast.NewAlterResourcePool("batch",
			ast.ResourcePoolSpec{}, ast.ResourcePoolSpec{}), want: ""},
		{name: "drop", node: ast.NewDropResourcePool("batch"), want: "DROP RESOURCE POOL `batch`;\n"},
		{name: "alter a classifier", node: ast.NewAlterResourcePoolClassifier("c",
			ast.ResourcePoolClassifierSpec{ResourcePool: "batch", Rank: 2},
			ast.ResourcePoolClassifierSpec{ResourcePool: "batch", MemberName: "etl", Rank: 1}),
			want: "ALTER RESOURCE POOL CLASSIFIER `c` SET (RESOURCE_POOL = 'batch', RANK = 2), RESET (MEMBER_NAME);\n"},
		{name: "drop a classifier", node: ast.NewDropResourcePoolClassifier("c"),
			want: "DROP RESOURCE POOL CLASSIFIER `c`;\n"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			sql, err := renderer.RenderSQLWithCapabilities(platform.YDB, caps, test.node)
			c.Assert(err, qt.IsNil)
			c.Assert(sql, qt.Equals, test.want)
		})
	}
}

func TestRender_ResourcePoolNodes_YDB_FailurePath(t *testing.T) {
	c := qt.New(t)

	sql, err := renderer.RenderSQLWithCapabilities(platform.YDB,
		capability.YDB262().With(capability.ResourcePools, true), ast.NewDropResourcePool("default"))

	c.Assert(err, qt.ErrorMatches, "DROP RESOURCE POOL default: it is the pool YDB runs every query in that no "+
		"classifier sends elsewhere, and after it is dropped every query of the database fails with `Resource pool "+
		"default not found`")
	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(sql, qt.Equals, "")
}

// A caller that claims the key for a target whose renderer writes no pool
// passes the central check, and the renderer refuses the node itself, naming
// itself.
func TestRender_ResourcePool_RenderersWithoutPoolsRefuse(t *testing.T) {
	tests := []struct {
		dialect string
		caps    capability.Capabilities
	}{
		{dialect: platform.Postgres, caps: capability.Postgres18()},
		{dialect: platform.MySQL, caps: capability.MySQL84()},
		{dialect: platform.MariaDB, caps: capability.MariaDB1011()},
		{dialect: platform.SQLite, caps: capability.SQLite3()},
		{dialect: platform.ClickHouse, caps: capability.ClickHouse24()},
		{dialect: platform.SQLServer, caps: capability.SQLServer2022()},
		{dialect: platform.Oracle, caps: capability.Oracle23()},
	}
	for _, test := range tests {
		t.Run(test.dialect, func(t *testing.T) {
			c := qt.New(t)
			for _, node := range poolNodes {
				sql, err := renderer.RenderSQLWithCapabilities(test.dialect,
					test.caps.With(capability.ResourcePools, true), node.node)
				c.Assert(err, qt.ErrorMatches, node.subject+`: the \w+ renderer writes no resource pool; a resource `+
					`pool needs target capability resource_pools, which only YDB has`)
				c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
				c.Assert(sql, qt.Equals, "")
			}
		})
	}
}
