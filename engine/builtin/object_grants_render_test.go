package builtin_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/platform/capability"
	"ptah.run/core/renderer"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/ydb/ydbcoordination"
	"ptah.run/dialect/ydb/ydbstreaming"
	"ptah.run/dialect/ydb/ydbtopic"
	"ptah.run/dialect/ydb/ydbworkload"
	"ptah.run/engine/builtin"
)

// TestRenderSchema_ObjectGrantsFollowTheirObject renders a declaration that
// grants on the scheme objects it declares: each GRANT follows the statement
// that creates its object, a coordination node's included, which its owner
// does not place ahead of the common statements.
func TestRenderSchema_ObjectGrantsFollowTheirObject(t *testing.T) {
	c := qt.New(t)
	database := &schemamodel.Database{
		FeatureObjects: must.Must(schemaext.NewObjects(
			ydbcoordination.DesiredObject("app", "locks", "", ydbcoordination.Spec{}),
			ydbtopic.DesiredObject("app", "events", "", ydbtopic.Spec{}),
		)),
		Roles: []schemamodel.Role{{Name: "reader", Login: true, Inherit: true}},
		Grants: []schemamodel.Grant{
			{Role: "reader", Privileges: []string{"ydb.generic.use"}, OnObject: "app.locks", ObjectKind: "COORDINATION NODE"},
			{Role: "reader", Privileges: []string{"SELECT ROW"}, OnObject: "app.events", ObjectKind: "TOPIC"},
		},
	}

	rendered, err := must.Must(builtin.New()).RenderSchema(c.Context(),
		renderer.SchemaRequest{Target: "ydb", Capabilities: capability.YDB262(), Schema: database})

	c.Assert(err, qt.IsNil)
	c.Assert(rendered.Statements, qt.DeepEquals, []string{
		"CREATE TOPIC `app/events`;\n",
		"CREATE USER `reader`;\n",
		"CREATE COORDINATION NODE `app/locks`;\n",
		"GRANT 'ydb.generic.use' ON `app/locks` TO `reader`;\n",
		"GRANT 'ydb.granular.select_row' ON `app/events` TO `reader`;\n",
	})
}

// TestRenderSchema_GrantsOnPoolsAndStreamingQueries renders a declaration
// that grants on a resource pool and a streaming query it declares: the
// pool's GRANT names the path YDB gives a pool, and each GRANT follows the
// statement that creates its object, the streaming query's included, which its
// owner places after every other statement.
func TestRenderSchema_GrantsOnPoolsAndStreamingQueries(t *testing.T) {
	c := qt.New(t)
	database := &schemamodel.Database{
		FeatureObjects: must.Must(schemaext.NewObjects(
			ydbworkload.DesiredPoolObject("batch", "", ydbworkload.PoolSpec{ConcurrentQueryLimit: new(int32(5))}),
			ydbstreaming.DesiredObject("app", "copy", "", ydbstreaming.Spec{Text: "INSERT INTO dst SELECT * FROM src;", Run: new(false)}, false),
		)),
		Roles: []schemamodel.Role{{Name: "reader", Login: true, Inherit: true}},
		Grants: []schemamodel.Grant{
			{Role: "reader", Privileges: []string{"SELECT ROW"}, OnObject: "batch", ObjectKind: "RESOURCE POOL"},
			{Role: "reader", Privileges: []string{"ydb.generic.manage"}, OnObject: "app.copy", ObjectKind: "STREAMING QUERY"},
		},
	}
	caps := capability.YDB262().With(capability.ResourcePools, true).With(capability.StreamingQueries, true)

	rendered, err := must.Must(builtin.New()).RenderSchema(c.Context(),
		renderer.SchemaRequest{Target: "ydb", Capabilities: caps, Schema: database})

	c.Assert(err, qt.IsNil)
	c.Assert(rendered.Statements, qt.DeepEquals, []string{
		"CREATE USER `reader`;\n",
		"CREATE RESOURCE POOL `batch` WITH (CONCURRENT_QUERY_LIMIT = 5);\n",
		"CREATE STREAMING QUERY `app/copy` WITH (RUN = FALSE, RESOURCE_POOL = `default`) AS DO BEGIN\n" +
			"INSERT INTO dst SELECT * FROM src;\nEND DO;\n",
		"GRANT 'ydb.granular.select_row' ON `.metadata/workload_manager/pools/batch` TO `reader`;\n",
		"GRANT 'ydb.generic.manage' ON `app/copy` TO `reader`;\n",
	})
}
