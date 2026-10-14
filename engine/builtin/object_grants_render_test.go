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
	"ptah.run/dialect/ydb/ydbtopic"
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
